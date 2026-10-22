"""Pi Coding Agent runtime.

设计取向：
- **轮询同步**（AgentRuntime 默认）：会话快照直接读 JSONL，不维护事件订阅；
- **单写者**：同一会话的 prompt/steer/abort 都走同一个 `pi --mode rpc` 子进程；
- **只读会话目录**：列表/快照不修改任何文件；
- 进程按会话懒启动，Host 停止时统一回收。
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC
from pathlib import Path
from typing import Any

from connector.runtime_protocol import (
    CAPABILITY_CATALOG_MODEL,
    CAPABILITY_RUNTIME_ATTACHMENT,
    CAPABILITY_SESSION_INTERACTION_APPROVAL,
    CAPABILITY_SESSION_INTERRUPT,
    CAPABILITY_SESSION_SEND_MESSAGE,
    CAPABILITY_SESSION_STEER,
    AgentRuntime,
    InputRequestForm,
    InputRequestOption,
    InputRequestQuestion,
    PreparedSessionTimelineSync,
    RuntimeCapability,
    RuntimeCapabilitySet,
    RuntimeConfig,
    RuntimeIdentity,
    RuntimeInvalidRequestError,
    RuntimeModelCatalog,
    RuntimeOperationResult,
    RuntimePermissionCatalog,
    RuntimeStatus,
    RuntimeTimelineItem,
    RuntimeTimelineSnapshot,
    RuntimeUnsupportedError,
    SessionMeta,
    SessionNotice,
    SessionSourceState,
    SessionState,
    timeline_content_hash,
)
from connector.runtime_protocol.attachments import attachment_target
from connector.runtimes.pi.catalog import build_models, read_model_config
from connector.runtimes.pi.discovery import resolve_executable
from connector.runtimes.pi.rpc import PiRpcError, PiRpcProcess, build_env
from connector.runtimes.pi.sessions import (
    PiSessionFile,
    agent_dir,
    build_timeline,
    create_session_file,
    platform_session_id,
    read_entries,
    read_summary,
    scan_sessions,
    session_roots,
)

logger = logging.getLogger(__name__)

DIALOG_METHODS = frozenset({"select", "confirm", "input", "editor"})
# ask 插件写进 `ui.input` placeholder 的结构化标记前缀（见 my-pi-plugins
# packages/pi-ask-user-question/src/aa-marker.ts）。pi 的 RPC 模式会丢弃 ui.input
# 的第三个参数，多选只能靠这行 JSON 才能还原成真勾选框。
AA_MARKER_PREFIX = "#aa-ask:"
CATALOG_TTL_SECONDS = 60.0
INDEX_REFRESH_SECONDS = 3.0
MAX_SNAPSHOT_ITEMS = 5_000
# 流式推送节流：太快会把 websocket 刷爆，太慢手机上看着卡。
STREAM_PUSH_MIN_INTERVAL_SECONDS = 0.12
# pi 把用户消息写进 JSONL 是异步的：agent_start 那一刻文件可能还是空的。
# 发送后盯着文件指纹，一有写入就立刻推一次时间线（否则自己的消息要等下一轮
# 推送才上台，期间只有乐观气泡，AI 的流式条目会跑到它上面）。
USER_WRITE_PUBLISH_TIMEOUT_SECONDS = 3.0
USER_WRITE_PUBLISH_POLL_SECONDS = 0.1
# 单个流式气泡最多推这么多字，剩下的等回合结束的完整快照。
STREAM_MAX_CHARS = 100_000
# 图片直接塞进 pi 的 RPC（base64），太大就落盘让 pi 自己读。
MAX_INLINE_IMAGE_BYTES = 8 * 1024 * 1024
MAX_INLINE_IMAGES = 8
ATTACHMENT_ONLY_PROMPT = "（我发来了附件，请查看）" 


def _file_marker(path: Path) -> str | None:
    """会话文件的内容指纹：mtime+size 变了就认为需要重新同步时间线。"""
    try:
        stat = path.stat()
    except OSError:
        return None
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _serialize_tool_input(value: Any, limit: int = 4_000) -> str | None:
    if value is None:
        return None
    try:
        text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except (TypeError, ValueError):
        text = str(value)
    return text[:limit]


def _sync_metadata(path: Path, synced: dict[str, str]) -> dict[str, Any]:
    marker = _file_marker(path)
    previous = synced.get(str(path.resolve()))
    requires = marker is not None and marker != previous
    return {
        "sync": {
            "marker": marker,
            "requires_timeline_sync": requires,
            "changed": requires,
            "previous_marker": previous,
        }
    }


@dataclass(slots=True)
class ClientMessage:
    """一条来自客户端的消息：clientMessageId + 文本 + 附件元信息。"""

    client_message_id: str
    text: str
    attachments: tuple[dict[str, Any], ...] = ()


@dataclass(slots=True)
class LiveStream:
    """正在流式输出的 assistant 消息（对应 `pi --mode rpc` 的 message_update）。"""

    item_id: str
    text: str = ""
    thinking: str = ""
    tools: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    last_push_at: float = 0.0


@dataclass(slots=True)
class PendingInteraction:
    notice: SessionNotice
    method: str
    # 多选（ask 插件把选项藏在 placeholder 的 AA 标记里）：回答要换算成 “1,3”
    multiple: bool = False
    # 回退上一题时该回传的哨兵值（插件按 `↩` 前缀识别）
    back_value: str | None = None


class PiRuntime(AgentRuntime):
    def __init__(
        self,
        *,
        config: RuntimeConfig,
        host: Any,
        runtime_version: str | None = None,
    ) -> None:
        self.config = config
        self.host = host
        self.values: dict[str, Any] = dict(config.values)
        self.runtime_version = runtime_version or "unknown"
        self._identity = RuntimeIdentity(
            runtime="pi",
            runtime_version=self.runtime_version,
            display_name="Pi Coding Agent",
            runtime_id=config.runtime_id,
        )
        self._index: dict[str, PiSessionFile] = {}
        self._processes: dict[str, PiRpcProcess] = {}
        self._active: dict[str, RuntimeStatus] = {}
        self._selections: dict[str, dict[str, str | None]] = {}
        self._notices: dict[str, dict[str, PendingInteraction]] = {}
        # platform id（sess_pi_...） -> 服务端会话 id（sess_xxx），出站 notice 用后者
        self._server_session_ids: dict[str, str] = {}
        self._snapshot_cache: dict[str, tuple[int, int, RuntimeTimelineSnapshot]] = {}
        # 已发布到服务端的时间线指纹；list_sessions 拿它对比文件当前指纹，
        # 决定是否需要重新同步（polling 模式靠这个上报增量）。
        self._synced: dict[str, str] = {}
        self._index_refreshed_at = 0.0
        # 流式输出：session -> 当前正在生成的气泡
        self._streams: dict[str, LiveStream] = {}
        self._stream_seq = 0
        self._order_seq: dict[str, int] = {}
        # 自动重试中的提示（pi 会自己重试，但手机得看到发生了什么）
        self._retry_notices: dict[str, str] = {}
        # 前端发消息时带的 clientMessageId：写进时间线 item.source，客户端才能
        # 把“本地乐观气泡”换成服务端条目，而不是再追加一条（否则同一条消息显示两遍）。
        self._client_messages: dict[str, list[ClientMessage]] = {}
        self._catalog: tuple[float, tuple[int, int] | None, RuntimeModelCatalog] | None = None
        # 时间戳起点：进程重启后 revision 仍单调递增，避免服务端判成冲突。
        self._catalog_revision = int(time.time() * 1000)
        self._catalog_key: tuple[tuple[str, ...], ...] | None = None
        self._closed = False

    @property
    def identity(self) -> RuntimeIdentity:
        return self._identity

    async def get_config(self) -> RuntimeConfig:
        return self.config

    def _canonical_session_id(self, summary: PiSessionFile) -> str:
        """把调用方给的 session id 归一成本 runtime 内部的 id。

        服务端会用自己那一套 session id 调 RPC（例如 sess_AAuQaz7Pp6sjhg），
        而事件/轮询用的是 platform_session_id（sess_pi_...）。两套 id 指向同一个
        JSONL，如果各存各的，_active/_selections/_client_messages 就会互相看不见——
        表现为回合结束通知丢了、clientMessageId 对不上导致消息重复。
        """
        return platform_session_id(self.host.connector_id, summary)

    def _published_session_id(self, canonical: str) -> str:
        """出站 payload 该用的 session id。

        服务端给 pi 会话自己有 id（sess_xxx，由 session.create 生成），而扫描/事件侧
        用的是 platform_session_id（sess_pi_...）。时间线、状态能被服务端按 external
        session 重新绑定，但 notice.upsert 不会——它直接按 sessionId 查会话行，查不到
        就当不存在**静默丢弃**。所以 notice 必须用服务端认识的那个 id。
        """
        return self._server_session_ids.get(canonical, canonical)

    def _notice_for_publish(self, notice: SessionNotice, canonical: str) -> SessionNotice:
        published = self._published_session_id(canonical)
        if notice.session_id == published:
            return notice
        blocking = notice.blocking
        if isinstance(blocking, Mapping):
            blocking = {**blocking, "targetId": published}
        return replace(notice, session_id=published, blocking=blocking)

    def _agent_dir(self) -> Path:
        return agent_dir(self.values)

    def _pi_executable(self) -> str:
        # Windows 上裸 "pi" 会让 CreateProcess 找不到 npm 的 pi.cmd（WinError 2），
        # 所以这里和 discovery 用同一套解析：piPath → PATH 上的真实路径 → "pi"。
        return resolve_executable(self.values) or "pi"

    def _extensions_enabled(self) -> bool:
        return bool(self.values.get("extensions", True))

    def _request_timeout(self) -> float:
        value = self.values.get("requestTimeoutMs")
        return float(value) / 1000.0 if isinstance(value, int) and value > 0 else 60.0

    def _startup_timeout(self) -> float:
        value = self.values.get("startupTimeoutMs")
        return float(value) / 1000.0 if isinstance(value, int) and value > 0 else 30.0

    async def start(self) -> None:
        # 启动时先取一次目录并上报，服务端会话快照才有模型可选。
        try:
            await self.list_model_catalog()
        except Exception:  # 目录失败不能阻止 runtime 启动
            logger.warning("pi runtime start: model catalog unavailable", exc_info=True)

    async def stop(self) -> None:
        self._closed = True
        processes = list(self._processes.values())
        self._processes.clear()
        for process in processes:
            await process.close()

    # ---- discovery / listing ----

    def _refresh_index(self) -> None:
        connector_id = self.host.connector_id
        self._index = {
            platform_session_id(connector_id, summary): summary
            for summary in scan_sessions(self.values, connector_id)
        }
        self._index_refreshed_at = time.monotonic()

    async def list_sessions(
        self,
        limit: int = 100,
        cursor: str | None = None,
        force: bool = False,
    ) -> tuple[SessionMeta, ...]:
        if (
            force
            or not self._index
            or time.monotonic() - self._index_refreshed_at > INDEX_REFRESH_SECONDS
        ):
            # 桌面端新建的会话不会经过 connector，定期重扫目录才能发现它们。
            self._refresh_index()
        sessions = sorted(
            self._index.values(),
            key=lambda summary: summary.updated_at,
            reverse=True,
        )
        rows: list[SessionMeta] = []
        for summary in sessions[: max(1, limit)]:
            external = str(summary.path.resolve())
            rows.append(
                SessionMeta(
                    session_id=platform_session_id(self.host.connector_id, summary),
                    external_session_id=external,
                    runtime="pi",
                    title=summary.title,
                    cwd=summary.cwd,
                    ordering_time=summary.updated_at.astimezone(UTC).isoformat(),
                    source_state=SessionSourceState(availability="available"),
                    metadata={
                        "nativeSessionId": summary.native_id,
                        "name": summary.name,
                        "sessionFormatVersion": 3,
                        **_sync_metadata(summary.path, self._synced),
                    },
                    runtime_id=self.config.runtime_id,
                )
            )
        return tuple(rows)

    async def list_complete_session_inventory(
        self,
        page_size: int = 100,
        force: bool = False,
    ) -> tuple[SessionMeta, ...]:
        return await self.list_sessions(limit=10_000, force=force)

    def supports_complete_session_inventory(self) -> bool:
        return True

    async def get_session_state(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> SessionState | None:
        summary = self._resolve(session_id, external_session_id)
        canonical = self._canonical_session_id(summary)
        status = self._active.get(canonical, "idle")
        if status == "running":
            status = await self._reconcile_running_status(canonical, summary)
        selections = self._selections.get(canonical)
        if not selections:
            selections = self._selections_from_session(canonical, summary)
        return SessionState(
            session_id=session_id,
            external_session_id=str(summary.path.resolve()),
            runtime="pi",
            status=status,
            selections=selections,
            runtime_id=self.config.runtime_id,
        )

    def _selections_from_session(
        self,
        session_id: str,
        summary: PiSessionFile,
    ) -> dict[str, str]:
        """从 JSONL 的 model_change / thinking_level_change 恢复当前选择。

        connector 重启后内存里没有选择，不恢复的话手机上的模型选择器会显示成
        默认模型（第一条），看起来像“切了模型没生效”。
        """
        provider: str | None = None
        model_id: str | None = None
        level: str | None = None
        try:
            entries = read_entries(summary.path)
        except OSError:
            entries = []
        for entry in reversed(entries):
            kind = entry.get("type")
            if kind == "model_change" and provider is None:
                value_provider = entry.get("provider")
                value_model = entry.get("modelId")
                if (
                    isinstance(value_provider, str)
                    and value_provider
                    and isinstance(value_model, str)
                    and value_model
                ):
                    provider, model_id = value_provider, value_model
            elif kind == "thinking_level_change" and level is None:
                value_level = entry.get("thinkingLevel")
                if isinstance(value_level, str) and value_level:
                    level = value_level
            if provider is not None and level is not None:
                break
        if provider is None or model_id is None:
            return {}
        selection = f"model:{provider}/{model_id}"
        if level:
            selection = f"{selection}#{level}"
        selections = {"model": selection}
        self._selections[session_id] = selections
        return selections

    async def _reconcile_running_status(
        self,
        session_id: str,
        summary: PiSessionFile,
    ) -> RuntimeStatus:
        """内存里的 running 可能因为丢了 agent_settled 永远卡住。

        对存活过的 RPC 进程问一句 get_state：不在流式输出就是真的闲了。
        """
        key = str(summary.path.resolve())
        process = self._processes.get(key)
        if process is None or not process.running:
            self._active.pop(session_id, None)
            self._notices.pop(session_id, None)
            return "idle"
        try:
            data = await process.request("get_state", timeout=min(5.0, self._request_timeout()))
        except PiRpcError as exc:
            logger.info("pi get_state failed while reconciling session=%s error=%s", session_id, exc)
            return "running"
        if data.get("isStreaming") is True:
            return "running"
        logger.info("pi session reconciled to idle (agent_settled was missed) session=%s", session_id)
        self._active.pop(session_id, None)
        self._notices.pop(session_id, None)
        return "idle"

    async def get_session_snapshot(
        self,
        session_id: str,
        external_session_id: str | None = None,
        limit: int | None = None,
    ) -> RuntimeTimelineSnapshot:
        summary = self._resolve(session_id, external_session_id)
        requested_id = session_id
        session_id = self._canonical_session_id(summary)
        path = summary.path.resolve()
        try:
            stat = path.stat()
        except OSError as exc:
            raise RuntimeInvalidRequestError(f"会话文件不可读：{exc}") from exc
        cached = self._snapshot_cache.get(str(path))
        if cached is not None and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
            snapshot = cached[2]
        else:
            entries = read_entries(path)
            items = build_timeline(session_id, str(path), entries)
            snapshot = RuntimeTimelineSnapshot(
                session_id=session_id,
                external_session_id=str(path),
                runtime="pi",
                items=tuple(items[-MAX_SNAPSHOT_ITEMS:]),
                complete=True,
                metadata={"sessionFormatVersion": 3},
                runtime_id=self.config.runtime_id,
            )
            self._snapshot_cache[str(path)] = (stat.st_mtime_ns, stat.st_size, snapshot)
        snapshot = self._bind_client_message_ids(session_id, snapshot)
        snapshot = self._with_live_stream(session_id, snapshot)
        if requested_id and requested_id != session_id:
            snapshot = replace(snapshot, session_id=requested_id)
        if limit is not None and 0 < limit < len(snapshot.items):
            return RuntimeTimelineSnapshot(
                session_id=snapshot.session_id,
                external_session_id=snapshot.external_session_id,
                runtime=snapshot.runtime,
                items=snapshot.items[-limit:],
                complete=False,
                metadata=snapshot.metadata,
                runtime_id=self.config.runtime_id,
            )
        return snapshot

    async def prepare_session_timeline_sync(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> PreparedSessionTimelineSync:
        """polling 同步入口：先读快照，发布成功后用 commit 记下指纹。"""
        summary = self._resolve(session_id, external_session_id)
        key = str(summary.path.resolve())
        marker = _file_marker(summary.path)
        snapshot = await self.get_session_snapshot(session_id, str(summary.path.resolve()))

        async def commit() -> None:
            if marker is not None:
                self._synced[key] = marker

        return PreparedSessionTimelineSync(snapshot=snapshot, commit=commit)

    async def resynchronize(
        self,
        session_id: str | None = None,
        external_session_id: str | None = None,
    ) -> None:
        if session_id is None and external_session_id is None:
            self._snapshot_cache.clear()
            self._synced.clear()
            return
        summary = self._resolve(session_id or "", external_session_id)
        self._snapshot_cache.pop(str(summary.path.resolve()), None)
        self._synced.pop(str(summary.path.resolve()), None)

    # ---- capabilities / catalogs ----

    def _capability(self, capability_id: str, session_id: str | None = None) -> RuntimeCapability:
        return RuntimeCapability(
            capability_id=capability_id,
            scope="session" if session_id else "runtime",
            runtime="pi",
            session_id=session_id,
            connector_id=self.host.connector_id,
            runtime_id=self.config.runtime_id,
        )

    async def get_runtime_capabilities(self) -> RuntimeCapabilitySet:
        return RuntimeCapabilitySet(
            runtime="pi",
            revision=1,
            capabilities=(
                self._capability(CAPABILITY_CATALOG_MODEL),
                self._capability(CAPABILITY_SESSION_SEND_MESSAGE),
                self._capability(CAPABILITY_SESSION_STEER),
                self._capability(CAPABILITY_SESSION_INTERRUPT),
                self._capability(CAPABILITY_SESSION_INTERACTION_APPROVAL),
                self._capability(CAPABILITY_RUNTIME_ATTACHMENT),
            ),
            connector_id=self.host.connector_id,
            runtime_id=self.config.runtime_id,
        )

    async def get_session_capabilities(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> RuntimeCapabilitySet:
        return RuntimeCapabilitySet(
            runtime="pi",
            revision=1,
            capabilities=(
                self._capability(CAPABILITY_CATALOG_MODEL, session_id),
                self._capability(CAPABILITY_SESSION_SEND_MESSAGE, session_id),
                self._capability(CAPABILITY_SESSION_STEER, session_id),
                self._capability(CAPABILITY_SESSION_INTERRUPT, session_id),
                self._capability(CAPABILITY_SESSION_INTERACTION_APPROVAL, session_id),
                self._capability(CAPABILITY_RUNTIME_ATTACHMENT, session_id),
            ),
            session_id=session_id,
            connector_id=self.host.connector_id,
            runtime_id=self.config.runtime_id,
        )

    async def list_permission_catalog(
        self,
        query: str | None = None,
        limit: int = 100,
    ) -> RuntimePermissionCatalog:
        return RuntimePermissionCatalog(runtime="pi", revision=1, permissions=())

    async def list_model_catalog(
        self,
        query: str | None = None,
        limit: int = 100,
    ) -> RuntimeModelCatalog:
        now = asyncio.get_running_loop().time()
        signature = self._models_signature()
        if (
            self._catalog is not None
            and now - self._catalog[0] < CATALOG_TTL_SECONDS
            and self._catalog[1] == signature
        ):
            catalog = self._catalog[2]
        else:
            catalog = await self._load_model_catalog()
        catalog = await self._publish_catalog(catalog)
        self._catalog = (now, signature, catalog)
        models = catalog.models
        if query:
            needle = query.casefold()
            models = tuple(
                item
                for item in models
                if needle in item.id.casefold() or needle in item.title.casefold()
            )
        if limit > 0:
            models = models[:limit]
        return RuntimeModelCatalog(
            runtime="pi",
            revision=catalog.revision,
            models=models,
            runtime_id=self.config.runtime_id,
        )

    def _models_signature(self) -> tuple[int, int] | None:
        """models.json 变了就立刻失效缓存，启停供应商不用等 TTL。"""

        try:
            stat = (self._agent_dir() / "models.json").stat()
        except OSError:
            return None
        return (stat.st_mtime_ns, stat.st_size)

    async def _load_model_catalog(self) -> RuntimeModelCatalog:
        process = PiRpcProcess(
            command=self._pi_executable(),
            args=self._ephemeral_args(),
            cwd=self._agent_dir(),
            env=build_env(self._agent_dir()),
            on_event=lambda _record: None,
        )
        try:
            await process.start(timeout=self._startup_timeout())
            data = await process.request("get_available_models", timeout=self._request_timeout())
        except PiRpcError as exc:
            logger.warning("pi model catalog unavailable: %s", exc)
            return RuntimeModelCatalog(runtime="pi", revision=1, models=())
        finally:
            await process.close()
        raw_models = data.get("models")
        models = build_models(
            raw_models if isinstance(raw_models, list) else [],
            read_model_config(self._agent_dir()),
        )
        return RuntimeModelCatalog(
            runtime="pi",
            runtime_id=self.config.runtime_id,
            revision=max(1, self._catalog_revision),
            models=models,
        )

    async def _publish_catalog(self, catalog: RuntimeModelCatalog) -> RuntimeModelCatalog:
        """内容变化时上报 `runtime.catalog.updated`，否则会话快照里没有模型。"""

        key: tuple[tuple[str, ...], ...] = tuple(
            (model.id, model.title, model.description or "", model.selection_id or "")
            for model in catalog.models
        )
        if key == self._catalog_key:
            return replace(catalog, revision=max(1, self._catalog_revision))
        self._catalog_revision += 1
        published = replace(catalog, revision=self._catalog_revision)
        try:
            await self.host.model_catalog_update(published)
        except Exception:  # 上报失败下次再试
            logger.warning("failed to publish pi model catalog", exc_info=True)
            return published
        self._catalog_key = key
        return published

    # ---- turns ----

    async def create_and_start_session(
        self,
        session_id: str,
        content: str,
        title: str | None = None,
        cwd: str | None = None,
        selections: Mapping[str, str | None] | None = None,
        attachments: tuple[Any, ...] = (),
        client_message_id: str | None = None,
        *,
        runtime_options: Mapping[str, Any] | None = None,
    ) -> RuntimeOperationResult:
        if runtime_options:
            raise RuntimeUnsupportedError("runtimeOptions")
        configured = self.values.get("cwd")
        target_cwd = (
            cwd
            or (configured if isinstance(configured, str) and configured.strip() else None)
            or str(Path.home())
        )
        if not Path(target_cwd).expanduser().is_dir():
            # Pi 拒绝在不存在的工作目录里打开会话；退回用户主目录而不是报错。
            target_cwd = str(Path.home())
        summary = create_session_file(self.values, target_cwd, title=title)
        self._index[session_id] = summary
        try:
            result = await self.start_turn(
                session_id=session_id,
                external_session_id=str(summary.path.resolve()),
                content=content,
                selections=selections,
                attachments=attachments,
                client_message_id=client_message_id,
            )
        except Exception:
            self._index.pop(session_id, None)
            raise
        return RuntimeOperationResult(
            ok=result.ok,
            code=result.code,
            message=result.message,
            result={"sessionId": session_id, **result.result},
        )

    async def start_turn(
        self,
        session_id: str,
        external_session_id: str | None,
        content: str,
        selections: Mapping[str, str | None] | None = None,
        attachments: tuple[Any, ...] = (),
        client_message_id: str | None = None,
        cwd: str | None = None,
    ) -> RuntimeOperationResult:
        summary = self._resolve(session_id, external_session_id)
        session_id = self._canonical_session_id(summary)
        if selections:
            self._selections[session_id] = dict(selections)
        process = await self._ensure_process(summary)
        await self._apply_selections(session_id, process)
        prompt_text, images, attachment_meta = await self._prepare_attachments(
            session_id,
            content,
            attachments,
        )
        # 必须在 prompt 之前绑定：文件一写入就可能被轮询同步投影，
        # 那条先发出去的条目要是缺 clientMessageId，服务端按 contentHash 判定
        # 未变化，之后就再也补不上了（客户端于是多出一条乐观气泡）。
        self._remember_client_message(session_id, client_message_id, content, attachment_meta)
        # 发送前的文件指纹：pi 一落盘就补推时间线，让这条消息马上有平台条目。
        marker_before = _file_marker(summary.path)
        running = self._active.get(session_id) in {"running", "waiting_approval", "stopping"}
        extra: dict[str, Any] = {"images": images} if images else {}
        try:
            if running:
                await process.send("steer", message=prompt_text, **extra)
                disposition = "queued"
            else:
                data = await process.request(
                    "prompt",
                    timeout=self._request_timeout(),
                    message=prompt_text,
                    **extra,
                )
                disposition = data.get("disposition") if isinstance(data.get("disposition"), str) else "started"
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
        logger.info(
            "pi turn dispatched session=%s disposition=%s client_message_id=%s",
            session_id,
            disposition,
            client_message_id,
        )
        if disposition in {"started", "queued"}:
            self._activate(session_id, "running", summary)
            self._spawn(
                self._publish_timeline_on_first_write(session_id, summary, marker_before)
            )
        return RuntimeOperationResult(
            ok=True,
            result={
                "disposition": disposition,
                "externalSessionId": str(summary.path.resolve()),
                "clientMessageId": client_message_id,
            },
        )

    async def steer_turn(
        self,
        session_id: str,
        external_session_id: str | None,
        content: str,
        attachments: tuple[Any, ...] = (),
        client_message_id: str | None = None,
    ) -> RuntimeOperationResult:
        summary = self._resolve(session_id, external_session_id)
        session_id = self._canonical_session_id(summary)
        process = self._processes.get(str(summary.path.resolve()))
        if process is None or not process.running:
            raise RuntimeInvalidRequestError("会话当前没有运行中的 Pi 进程")
        prompt_text, images, attachment_meta = await self._prepare_attachments(
            session_id,
            content,
            attachments,
        )
        self._remember_client_message(session_id, client_message_id, content, attachment_meta)
        try:
            await process.send(
                "steer",
                message=prompt_text,
                **({"images": images} if images else {}),
            )
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
        self._activate(session_id, "running", summary)
        return RuntimeOperationResult(ok=True, result={"clientMessageId": client_message_id})

    def _remember_client_message(
        self,
        session_id: str,
        client_message_id: str | None,
        content: str,
        attachments: tuple[dict[str, Any], ...] = (),
    ) -> None:
        if not client_message_id:
            return
        bindings = self._client_messages.setdefault(session_id, [])
        bindings.append(ClientMessage(client_message_id, content, attachments))
        del bindings[:-20]

    async def _prepare_attachments(
        self,
        session_id: str,
        content: str,
        attachments: tuple[Any, ...],
    ) -> tuple[str, list[dict[str, Any]], tuple[dict[str, Any], ...]]:
        """把 AA 的附件变成 pi 能用的形态。

        - 图片：base64 塞进 rpc 的 `images`（pi 会存进会话 JSONL，模型直接看得到）
        - 其他文件：落到 connector 数据目录，再把路径写进提示词，让 pi 自己 read
        """
        if not attachments:
            return content, [], ()
        images: list[dict[str, Any]] = []
        notes: list[str] = []
        meta: list[dict[str, Any]] = []
        for attachment in attachments:
            downloaded = await self.host.attachment_download(session_id, attachment.file_id)
            data = downloaded.content
            media_type = (
                (downloaded.media_type or attachment.media_type or "application/octet-stream")
                .split(";", 1)[0]
                .strip()
                .lower()
            )
            name = attachment.name or downloaded.name or attachment.file_id
            if (
                media_type.startswith("image/")
                and len(data) <= MAX_INLINE_IMAGE_BYTES
                and len(images) < MAX_INLINE_IMAGES
            ):
                images.append(
                    {
                        "type": "image",
                        "data": base64.b64encode(data).decode("ascii"),
                        "mimeType": media_type,
                    }
                )
            else:
                target = attachment_target(session_id, attachment.file_id, name)
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                target.write_bytes(data)
                notes.append(f"- {name} ({media_type}): {target}")
            meta.append(
                {
                    "fileId": attachment.file_id,
                    "name": name,
                    "mediaType": media_type,
                    "size": len(data),
                }
            )
        text = content.strip() or ATTACHMENT_ONLY_PROMPT
        if notes:
            listing = "\n".join(notes)
            text = f"{text}\n\n以下附件已保存到本机，需要时用 read 等工具打开：\n{listing}"
        logger.info(
            "pi attachments prepared session=%s images=%s files=%s",
            session_id,
            len(images),
            len(notes),
        )
        return text, images, tuple(meta)

    def _bind_client_message_ids(
        self,
        session_id: str,
        snapshot: RuntimeTimelineSnapshot,
    ) -> RuntimeTimelineSnapshot:
        bindings = self._client_messages.get(session_id)
        if not bindings:
            return snapshot
        used: set[str] = set()
        items: list[Any] = []
        changed = False
        for item in snapshot.items:
            if (
                item.type == "message"
                and item.role == "user"
                and "clientMessageId" not in item.source
            ):
                text = (item.content or {}).get("text")
                for binding in bindings:
                    if binding.client_message_id in used or text != binding.text:
                        continue
                    used.add(binding.client_message_id)
                    content = dict(item.content or {})
                    if binding.attachments:
                        content["attachments"] = [dict(a) for a in binding.attachments]
                    # 服务端只按 contentHash 判断条目是否变化（见 upstream
                    # core/timeline.py:timeline_item_state_is_unchanged），
                    # 只改 source 的话更新会被丢弃。
                    item = replace(
                        item,
                        content=content,
                        source={
                            **item.source,
                            "clientMessageId": binding.client_message_id,
                        },
                        content_hash=f"{item.content_hash}:cmid:{binding.client_message_id}",
                    )
                    changed = True
                    break
            items.append(item)
        if not changed:
            return snapshot
        return replace(snapshot, items=tuple(items))

    async def interrupt_session(
        self,
        session_id: str,
        reason: str | None = None,
    ) -> RuntimeOperationResult:
        summary = self._resolve(session_id, None)
        session_id = self._canonical_session_id(summary)
        process = self._processes.get(str(summary.path.resolve()))
        if process is None or not process.running:
            return RuntimeOperationResult(ok=True, result={"interrupted": False})
        try:
            await process.send("abort")
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
        self._active[session_id] = "stopping"
        self._report_state(session_id, summary, "stopping")
        return RuntimeOperationResult(ok=True, result={"interrupted": True, "reason": reason})

    async def update_session_selections(
        self,
        session_id: str,
        external_session_id: str | None,
        selections: Mapping[str, str | None],
    ) -> RuntimeOperationResult:
        summary = self._resolve(session_id, external_session_id)
        logger.info(
            "pi selections update session=%s requested=%s selections=%s",
            session_id,
            external_session_id,
            dict(selections),
        )
        session_id = self._canonical_session_id(summary)
        self._selections[session_id] = dict(selections)
        process = self._processes.get(str(summary.path.resolve()))
        if process is not None and process.running:
            await self._apply_selections(session_id, process)
        self._report_state(session_id, summary, self._active.get(session_id, "idle"))
        return RuntimeOperationResult(ok=True, result={"selections": dict(selections)})

    # ---- interactions ----

    async def get_session_notices(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> tuple[SessionNotice, ...]:
        summary = self._resolve(session_id, external_session_id) if external_session_id else None
        if summary is None:
            summary = self._index.get(session_id)
        canonical = self._canonical_session_id(summary) if summary is not None else session_id
        pending = self._notices.get(canonical, {})
        return tuple(
            self._notice_for_publish(item.notice, canonical) for item in pending.values()
        )

    async def respond_interaction(
        self,
        session_id: str,
        notice_id: str,
        action_id: str,
        input_data: Mapping[str, Any] | None = None,
    ) -> RuntimeOperationResult:
        summary = self._resolve(session_id, None)
        session_id = self._canonical_session_id(summary)
        pending = self._notices.get(session_id, {})
        interaction = pending.pop(notice_id, None)
        if interaction is None:
            raise RuntimeInvalidRequestError("交互请求不存在或已处理")
        process = self._processes.get(str(summary.path.resolve()))
        if process is None or not process.running:
            raise RuntimeInvalidRequestError("Pi 进程已退出，无法回答交互请求")
        payload: dict[str, Any] = {"id": notice_id}
        if interaction.method == "confirm":
            confirmed = action_id in {"confirm", "approve", "yes"}
            if isinstance(input_data, Mapping) and isinstance(input_data.get("confirmed"), bool):
                confirmed = bool(input_data["confirmed"])
            payload["confirmed"] = confirmed
        elif action_id == "back" and interaction.back_value:
            # 插件按 `↩` 前缀识别回退，原样把哨兵串送回去。
            payload["value"] = interaction.back_value
        else:
            value = _answer_value(input_data, multiple=interaction.multiple)
            if value is None:
                raise RuntimeInvalidRequestError("回答缺少选项或文本")
            payload["value"] = value
        try:
            await process.send("extension_ui_response", **payload)
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
        # 立刻把这条交互收掉，否则客户端要等下一次 notices 轮询才消失。
        # SessionNotice 没有 revision 字段（payload 里服务端默认 1），两端客户端都
        # 是 `revision >= 已存`，同 revision 的状态变更会被接受。
        closed = replace(
            self._notice_for_publish(interaction.notice, session_id),
            status="resolved",
            blocking=None,
        )
        self._spawn(self.host.notice_upsert(closed))
        self._activate(session_id, "running", summary)
        return RuntimeOperationResult(ok=True)

    # ---- internals ----

    def _resolve(self, session_id: str, external_session_id: str | None) -> PiSessionFile:
        if external_session_id:
            path = Path(external_session_id)
            if path.suffix != ".jsonl":
                raise RuntimeInvalidRequestError("external session id 必须是 .jsonl 路径")
            resolved = path.resolve()
            if not any(_is_under(resolved, root.resolve()) for root in session_roots(self.values)):
                raise RuntimeInvalidRequestError("会话文件不在 Pi 会话目录内")
            summary = read_summary(resolved)
            if summary is None:
                raise RuntimeInvalidRequestError("会话文件不可读或格式不正确")
            self._index[session_id] = summary
            canonical = platform_session_id(self.host.connector_id, summary)
            if session_id != canonical:
                # 服务端用自己那套 id 调过来，记下来：出站 notice 要用它，
                # 否则会被服务端当成不存在的会话丢掉。
                self._server_session_ids[canonical] = session_id
            return summary
        summary = self._index.get(session_id)
        if summary is None:
            # 服务端 id（sess_xxx）的别名不是目录扫描出来的，_refresh_index 重建
            # _index 时会把它冲掉。session.interrupt 这类调用只带 sessionId、
            # 不带 externalSessionId，只能靠持久的别名表把平台 id 找回来
            # （否则就是「未知的 Pi 会话：sess_xxx」，界面上点了停止没反应）。
            summary = self._summary_for_server_session_id(session_id)
            if summary is not None:
                self._index[session_id] = summary
                return summary
            self._refresh_index()
            summary = self._index.get(session_id) or self._summary_for_server_session_id(
                session_id
            )
            if summary is not None:
                self._index[session_id] = summary
        if summary is None:
            raise RuntimeInvalidRequestError(f"未知的 Pi 会话：{session_id}")
        return summary

    def _summary_for_server_session_id(self, session_id: str) -> PiSessionFile | None:
        """按别名表把服务端会话 id 还原成平台 id 对应的会话。"""
        for canonical, server_id in self._server_session_ids.items():
            if server_id == session_id:
                return self._index.get(canonical)
        return None

    def _session_args(self, summary: PiSessionFile) -> list[str]:
        args = ["--mode", "rpc", "--session", str(summary.path.resolve())]
        if not self._extensions_enabled():
            args.append("--no-extensions")
        return args

    def _ephemeral_args(self) -> list[str]:
        args = ["--mode", "rpc", "--no-session"]
        if not self._extensions_enabled():
            args.append("--no-extensions")
        return args

    async def _ensure_process(self, summary: PiSessionFile) -> PiRpcProcess:
        key = str(summary.path.resolve())
        process = self._processes.get(key)
        if process is not None and process.running:
            return process
        cwd = Path(summary.cwd)
        if not cwd.is_dir():
            cwd = self._agent_dir()
        process = PiRpcProcess(
            command=self._pi_executable(),
            args=self._session_args(summary),
            cwd=cwd,
            env=build_env(self._agent_dir()),
            on_event=lambda record: self._handle_event(summary, record),
            on_exit=lambda: self._processes.pop(key, None),
        )
        try:
            await process.start(timeout=self._startup_timeout())
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(f"无法启动 Pi：{exc}") from exc
        self._processes[key] = process
        return process

    async def _apply_selections(self, session_id: str, process: PiRpcProcess) -> None:
        selection = self._selections.get(session_id, {}).get("model")
        if not isinstance(selection, str) or not selection:
            return
        raw = selection.removeprefix("model:")
        level: str | None = None
        if "#" in raw:
            raw, level = raw.split("#", 1)
        if "/" in raw:
            provider, model_id = raw.split("/", 1)
            logger.info(
                "pi apply selection session=%s provider=%s model=%s level=%s",
                session_id,
                provider,
                model_id,
                level,
            )
            try:
                await process.request(
                    "set_model",
                    timeout=self._request_timeout(),
                    provider=provider,
                    modelId=model_id,
                )
            except PiRpcError as exc:
                logger.warning("pi set_model failed: %s", exc)
        if level:
            try:
                await process.request(
                    "set_thinking_level",
                    timeout=self._request_timeout(),
                    level=level,
                )
            except PiRpcError as exc:
                logger.warning("pi set_thinking_level failed: %s", exc)

    # ---- 流式输出 ----

    def _handle_message_event(
        self,
        session_id: str,
        summary: PiSessionFile,
        record: Mapping[str, Any],
    ) -> None:
        """把 pi 的 message_* 事件投影成会不断增长的文本 / 思考 / 工具卡片。"""
        event_type = record.get("type")
        message = record.get("message")
        role = message.get("role") if isinstance(message, Mapping) else None
        if event_type == "message_start":
            if role != "assistant":
                return
            self._stream_seq += 1
            self._streams[session_id] = LiveStream(item_id=f"pi:live:{self._stream_seq:x}")
            return
        stream = self._streams.get(session_id)
        if stream is None:
            return
        if event_type == "message_end":
            if role == "assistant":
                # 最终内容由回合结束时的完整快照接管，这里只丢弃临时气泡。
                self._streams.pop(session_id, None)
            return
        payload = record.get("assistantMessageEvent")
        if not isinstance(payload, Mapping):
            return
        kind = payload.get("type")
        if kind == "error":
            failed = payload.get("error")
            message_text = failed.get("errorMessage") if isinstance(failed, Mapping) else None
            stream.error = (
                message_text.strip()
                if isinstance(message_text, str) and message_text.strip()
                else "模型调用失败"
            )
            self._push_live(session_id, summary, stream, force=True)
            self._streams.pop(session_id, None)
            return
        if kind == "toolcall_end":
            # pi 的 toolcall 事件不带 partial，只有 toolcall_end 里有完整 toolCall。
            tool_call = payload.get("toolCall")
            if isinstance(tool_call, Mapping):
                tool_id = tool_call.get("id")
                title = tool_call.get("name")
                entries = stream.tools
                if not any(
                    entry.get("id") == tool_id for entry in entries
                ) if isinstance(tool_id, str) else True:
                    entries.append(
                        {
                            "id": tool_id if isinstance(tool_id, str) else None,
                            "title": title if isinstance(title, str) else None,
                            "input": tool_call.get("arguments"),
                        }
                    )
        partial = payload.get("partial")
        if isinstance(partial, Mapping):
            self._apply_partial(stream, partial)
        else:
            # 兜底：老版本 rpc 可能只带 delta
            delta = payload.get("delta")
            if isinstance(delta, str) and delta:
                if kind == "text_delta":
                    stream.text = (stream.text + delta)[:STREAM_MAX_CHARS]
                elif kind == "thinking_delta":
                    stream.thinking = (stream.thinking + delta)[:STREAM_MAX_CHARS]
        if not isinstance(kind, str):
            return
        if kind not in {"text_delta", "thinking_delta", "done"} and not kind.startswith("toolcall"):
            return
        # 工具回合/消息结束这些「形状变化」必须立刻推，别被节流吞掉。
        self._push_live(
            session_id,
            summary,
            stream,
            force=kind == "done" or kind.startswith("toolcall"),
        )

    @staticmethod
    def _apply_partial(stream: LiveStream, partial: Mapping[str, Any]) -> None:
        """`partial` 里是这条消息到目前为止的完整内容，照抄比累加 delta 稳。"""
        texts: list[str] = []
        thinking: list[str] = []
        tools: list[dict[str, Any]] = []
        content = partial.get("content")
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, Mapping):
                    continue
                block_type = block.get("type")
                if block_type == "text" and isinstance(block.get("text"), str):
                    texts.append(block["text"])
                elif block_type == "thinking" and isinstance(block.get("thinking"), str):
                    thinking.append(block["thinking"])
                elif block_type == "toolCall":
                    tools.append(
                        {
                            "id": block.get("id")
                            if isinstance(block.get("id"), str)
                            else None,
                            "title": block.get("name")
                            if isinstance(block.get("name"), str)
                            else None,
                            "input": block.get("arguments"),
                        }
                    )
        stream.text = "".join(texts)[:STREAM_MAX_CHARS]
        stream.thinking = "".join(thinking)[:STREAM_MAX_CHARS]
        if tools:
            # partial 里有 toolCall 块时以它为准（会去重，和 toolcall_end 收集的合并）
            known = {entry.get("id") for entry in stream.tools if entry.get("id")}
            for tool in tools:
                if tool.get("id") and tool["id"] in known:
                    continue
                stream.tools.append(tool)

    def _push_live(
        self,
        session_id: str,
        summary: PiSessionFile,
        stream: LiveStream,
        *,
        force: bool = False,
    ) -> None:
        now = time.monotonic()
        if not force and now - stream.last_push_at < STREAM_PUSH_MIN_INTERVAL_SECONDS:
            return
        stream.last_push_at = now
        path = str(summary.path.resolve())
        for item in self._live_items(session_id, path, stream):
            self._spawn(self.host.timeline_item_upsert(item))

    def _live_items(
        self,
        session_id: str,
        path: str,
        stream: LiveStream,
        base_order: int | None = None,
    ) -> list[RuntimeTimelineItem]:
        items: list[RuntimeTimelineItem] = []
        order = base_order

        def next_order() -> int:
            nonlocal order
            if order is None:
                return self._next_order_seq(session_id)
            order += 1
            return order

        if stream.thinking.strip():
            items.append(
                self._live_item(
                    session_id,
                    path,
                    f"{stream.item_id}:thinking",
                    item_type="system",
                    role="assistant",
                    content={
                        "kind": "reasoning",
                        "text": stream.thinking.strip(),
                    },
                    event="thinking_delta",
                    order_seq=next_order(),
                )
            )
        if stream.text.strip():
            items.append(
                self._live_item(
                    session_id,
                    path,
                    stream.item_id,
                    item_type="message",
                    role="assistant",
                    content={"kind": "text", "text": stream.text, "format": "text"},
                    event="text_delta",
                    order_seq=next_order(),
                )
            )
        for index, tool in enumerate(stream.tools):
            content: dict[str, Any] = {"kind": "tool_call"}
            if tool.get("title"):
                content["title"] = tool.get("title")
            serialized = _serialize_tool_input(tool.get("input"))
            if serialized:
                content["input"] = serialized
            items.append(
                self._live_item(
                    session_id,
                    path,
                    f"{stream.item_id}:tool:{index}",
                    item_type="tool",
                    role="assistant",
                    content=content,
                    event="toolcall_end",
                    order_seq=next_order(),
                )
            )
        if stream.error:
            items.append(
                self._live_item(
                    session_id,
                    path,
                    f"{stream.item_id}:error",
                    item_type="system",
                    role="assistant",
                    content={"kind": "error", "text": stream.error},
                    event="error",
                    status="failed",
                    order_seq=next_order(),
                )
            )
        return items

    def _retry_item(
        self,
        session_id: str,
        path: str,
        order_seq: int,
    ) -> RuntimeTimelineItem | None:
        notice = self._retry_notices.get(session_id)
        if not notice:
            return None
        return self._live_item(
            session_id,
            path,
            f"pi:live:retry:{session_id[-8:]}",
            item_type="system",
            role="assistant",
            content={"kind": "error", "text": notice},
            event="auto_retry",
            order_seq=order_seq,
        )

    def _live_item(
        self,
        session_id: str,
        path: str,
        item_id: str,
        *,
        item_type: str,
        role: str,
        content: dict[str, Any],
        event: str,
        status: str = "running",
        order_seq: int,
    ) -> RuntimeTimelineItem:
        return RuntimeTimelineItem(
            id=item_id,
            session_id=session_id,
            type=item_type,
            status=status,
            role=role,
            content=content,
            source={
                "runtime": "pi",
                "sessionId": path,
                "itemId": item_id,
                "itemType": item_type,
                "event": event,
                "runtimeId": self.config.runtime_id,
            },
            order_seq=order_seq,
            content_hash=timeline_content_hash(
                item_type=item_type,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                role=role,  # type: ignore[arg-type]
                content=content,
            ),
        )

    def _next_order_seq(self, session_id: str) -> int:
        value = self._order_seq.get(session_id, 0) + 1
        self._order_seq[session_id] = value
        return value

    def _with_live_stream(
        self,
        session_id: str,
        snapshot: RuntimeTimelineSnapshot,
    ) -> RuntimeTimelineSnapshot:
        stream = self._streams.get(session_id)
        retry_notice = self._retry_notices.get(session_id)
        if stream is None and not retry_notice:
            return snapshot
        live: list[RuntimeTimelineItem] = []
        max_order = max((item.order_seq for item in snapshot.items), default=0)
        if stream is not None:
            items = self._live_items(
                session_id,
                snapshot.external_session_id or "",
                stream,
                base_order=max_order,
            )
            live.extend(items)
            max_order = max((item.order_seq for item in items), default=max_order)
        if retry_notice:
            item = self._retry_item(
                session_id,
                snapshot.external_session_id or "",
                max_order + 1,
            )
            if item is not None:
                live.append(item)
        if not live:
            return snapshot
        return replace(snapshot, items=(*snapshot.items, *live))

    async def _publish_timeline_on_first_write(
        self,
        session_id: str,
        summary: PiSessionFile,
        seen_marker: str | None,
        *,
        timeout: float = USER_WRITE_PUBLISH_TIMEOUT_SECONDS,
        interval: float = USER_WRITE_PUBLISH_POLL_SECONDS,
    ) -> None:
        """等 pi 把这条用户消息写进会话文件，一落盘立刻推时间线。

        pi 走 RPC 时用户消息是异步写盘的，`agent_start` 到达那一刻 JSONL 可能还
        是空的（实测 items=0），于是这条消息本身要等下一次推送（多半是回合结束）
        才有平台条目。这期间客户端只剩乐观气泡挂在末尾，而 AI 的流式条目已经先
        到，看起来就像「回复跑到了我发的消息上面」，刷新后才恢复。
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(interval)
            marker = _file_marker(summary.path)
            if marker is not None and marker != seen_marker:
                await self._publish_timeline(session_id, summary)
                return

    async def _publish_timeline(self, session_id: str, summary: PiSessionFile) -> None:
        """RPC 回合事件到达时立刻推一次时间线，并发完就记下指纹避免重复推。"""
        key = str(summary.path.resolve())
        marker = _file_marker(summary.path)
        snapshot = await self.get_session_snapshot(session_id, key)
        await self.host.timeline_sync(
            session_id=session_id,
            runtime="pi",
            external_session_id=key,
            items=snapshot.items,
            complete=snapshot.complete,
            metadata=snapshot.metadata,
        )
        if snapshot.items:
            self._order_seq[session_id] = max(
                (item.order_seq for item in snapshot.items),
                default=self._order_seq.get(session_id, 0),
            )
        logger.info(
            "pi timeline published session=%s items=%s user=%s",
            session_id,
            len(snapshot.items),
            [
                (item.id, item.source.get("clientMessageId"))
                for item in snapshot.items
                if item.type == "message" and item.role == "user"
            ],
        )
        if marker is not None:
            self._synced[key] = marker

    def _handle_event(self, summary: PiSessionFile, record: dict[str, Any]) -> None:
        event_type = record.get("type")
        session_id = platform_session_id(self.host.connector_id, summary)
        if event_type in {"message_start", "message_update", "message_end"}:
            self._handle_message_event(session_id, summary, record)
            return
        if event_type == "auto_retry_start":
            attempt = record.get("attempt")
            max_attempts = record.get("maxAttempts")
            reason = record.get("errorMessage")
            detail = reason.strip() if isinstance(reason, str) and reason.strip() else "模型调用失败"
            self._retry_notices[session_id] = (
                f"{detail}（自动重试 {attempt}/{max_attempts}）"
                if attempt and max_attempts
                else detail
            )
            path = str(summary.path.resolve())
            item = self._retry_item(
                session_id,
                path,
                self._next_order_seq(session_id),
            )
            if item is not None:
                self._spawn(self.host.timeline_item_upsert(item))
            return
        if event_type == "auto_retry_end":
            if self._retry_notices.pop(session_id, None) is not None:
                logger.info("pi auto retry finished session=%s success=%s", session_id, record.get("success"))
                self._spawn(self._publish_timeline(session_id, summary))
            return
        if event_type == "agent_start":
            logger.info("pi event agent_start session=%s", session_id)
            self._activate(session_id, "running", summary)
            self._spawn(self._publish_timeline(session_id, summary))
            return
        if event_type == "agent_settled":
            logger.info("pi event agent_settled session=%s aborted=%s", session_id, record.get("aborted") is True)
            self._active.pop(session_id, None)
            self._notices.pop(session_id, None)
            self._report_state(session_id, summary, "idle")
            # 手机端靠这条推送拿到这轮回复，不等 30s 的轮询。
            self._spawn(self._publish_timeline(session_id, summary))
            outcome = "aborted" if record.get("aborted") is True else "completed"
            self._spawn(
                self.host.session_turn_ended(
                    session_id,
                    "pi",
                    external_session_id=str(summary.path.resolve()),
                    outcome=outcome,
                )
            )
            return
        if event_type == "thinking_level_changed":
            level = record.get("level")
            if isinstance(level, str) and level:
                current = dict(self._selections.get(session_id, {}))
                # 只更新显示层；模型选择保持原样，避免把 selectionId 拆坏。
                current.setdefault("thinking", level)
                self._selections[session_id] = current
            return
        if event_type == "session_info_changed":
            name = record.get("name")
            if isinstance(name, str) and name.strip():
                self._spawn(
                    self.host.session_meta_upsert(
                        session_id,
                        "pi",
                        external_session_id=str(summary.path.resolve()),
                        title=name.strip(),
                        cwd=summary.cwd,
                    )
                )
            return
        if event_type == "extension_ui_request":
            self._handle_extension_request(session_id, summary, record)
            return

    def _handle_extension_request(
        self,
        session_id: str,
        summary: PiSessionFile,
        record: dict[str, Any],
    ) -> None:
        method = record.get("method")
        request_id = record.get("id")
        if not isinstance(request_id, str):
            return
        if method not in DIALOG_METHODS:
            if method == "notify":
                logger.info("pi notify: %s", record.get("message"))
            return
        title = record.get("title") if isinstance(record.get("title"), str) else "Pi 请求确认"
        message = record.get("message") if isinstance(record.get("message"), str) else None
        placeholder = record.get("placeholder") if isinstance(record.get("placeholder"), str) else None
        _, marker = _split_aa_marker(placeholder)
        notice = build_notice(
            session_id=session_id,
            external_session_id=str(summary.path.resolve()),
            request_id=request_id,
            method=str(method),
            title=title,
            message=message,
            options=record.get("options"),
            placeholder=placeholder,
            timeout=record.get("timeout"),
            runtime_id=self.config.runtime_id,
        )
        self._notices.setdefault(session_id, {})[request_id] = PendingInteraction(
            notice=notice,
            method=str(method),
            multiple=marker is not None and marker.get("kind") == "multiSelect",
            back_value=_marker_back_value(marker),
        )
        # 和 claude/codex 一样即时上报：只靠周期性 session sync 上报的话，手机端要等
        # 一个同步周期（~30s）才能看到弹窗，而且 sync 只在会话文件有变化时才推 notices。
        published = self._notice_for_publish(notice, session_id)
        logger.info(
            "pi interaction notice published session_id=%s notice_id=%s method=%s title=%s",
            published.session_id,
            published.notice_id,
            method,
            published.title,
        )
        self._spawn(self.host.notice_upsert(published))
        self._activate(session_id, "waiting_approval", summary)

    def _activate(self, session_id: str, status: RuntimeStatus, summary: PiSessionFile) -> None:
        self._active[session_id] = status
        self._report_state(session_id, summary, status)

    def _report_state(self, session_id: str, summary: PiSessionFile, status: RuntimeStatus) -> None:
        self._spawn(
            self.host.session_state_update(
                session_id,
                "pi",
                status=status,
                selections=self._selections.get(session_id, {}),
                external_session_id=str(summary.path.resolve()),
            )
        )

    def _spawn(self, coroutine: Any) -> None:
        async def runner() -> None:
            try:
                await coroutine
            except Exception:
                logger.exception("pi runtime host update failed")

        try:
            asyncio.get_running_loop().create_task(runner())
        except RuntimeError:
            coroutine.close()


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _answer_value(input_data: Mapping[str, Any] | None, *, multiple: bool = False) -> str | None:
    if not isinstance(input_data, Mapping):
        return None
    direct = input_data.get("value")
    if isinstance(direct, str) and direct:
        return direct
    answers = input_data.get("answers")
    if not isinstance(answers, Mapping):
        return None
    first = answers.get("value")
    if not isinstance(first, Mapping):
        return None
    custom = first.get("customText")
    if isinstance(custom, str) and custom.strip():
        # 插件把非编号输入当成自定义答案，所以自定义文本优先。
        return custom.strip()
    option_ids = first.get("optionIds")
    if isinstance(option_ids, list):
        ids = [item for item in option_ids if isinstance(item, str) and item]
        if ids:
            # 多选：插件的 parseMultiSelection 只认 1-based 序号串（"1,3"）。
            return ",".join(ids) if multiple else ids[0]
    return None


def build_notice(
    *,
    session_id: str,
    external_session_id: str,
    request_id: str,
    method: str,
    title: str,
    message: str | None,
    options: Any,
    placeholder: str | None = None,
    timeout: Any,
    runtime_id: str | None,
) -> SessionNotice:
    # 协议里 NoticeBlocking 只有 scope/targetId（服务端 NoticeBlocking 两个字段都必填），
    # 前端也靠 scope=="session" and targetId==sessionId 才把 notice 当成阻塞交互渲染。
    # 这里曾经写成 {"reason": "extension_ui", "method": ...}，导致被服务端校验拒绝、
    # 前端永远看不到 confirm/select 弹窗。method 已经放在 source.method 里。
    blocking: dict[str, Any] = {"scope": "session", "targetId": session_id}
    if isinstance(timeout, int) and timeout > 0:
        blocking["timeoutMs"] = timeout
    if method == "confirm":
        return SessionNotice(
            notice_id=request_id,
            session_id=session_id,
            runtime="pi",
            type="interaction",
            title=title,
            message=message,
            severity="warning",
            status="open",
            interaction_type="confirmation",
            blocking=blocking,
            response_required=True,
            actions=(
                {"actionId": "confirm", "label": "确认", "style": "primary"},
                {"actionId": "cancel", "label": "取消", "style": "secondary"},
            ),
            source={"externalSessionId": external_session_id, "method": method},
            runtime_id=runtime_id,
        )
    labels = [option for option in options if isinstance(option, str)] if isinstance(options, list) else []
    placeholder_text, marker = _split_aa_marker(placeholder)
    actions: tuple[Mapping[str, Any], ...]
    if method == "select":
        question = InputRequestQuestion(
            question_id="value",
            prompt=message or title,
            options=tuple(InputRequestOption(option_id=label, label=label) for label in labels),
            header=title,
            multiple=False,
            allow_custom=False,
        )
        actions = (InputRequestForm(questions=(question,)).action(label="提交"),)
    elif marker is not None:
        # ask 插件把多选的结构化信息藏在 placeholder 的标记行里（pi 的 RPC 模式
        # 会丢弃 ui.input 的第三个参数，只能走这条道），解出来就是真勾选框。
        question = _marker_question(marker, title)
        form = InputRequestForm(questions=(question,))
        back_value = _marker_back_value(marker)
        actions = (form.action(label="提交"),)
        if back_value is not None:
            actions = (
                *actions,
                {"actionId": "back", "label": "上一题", "style": "secondary"},
            )
    else:
        question = InputRequestQuestion(
            question_id="value",
            prompt=title,
            options=(),
            header=None,
            multiple=False,
            allow_custom=True,
        )
        actions = (InputRequestForm(questions=(question,)).action(label="提交"),)
        # 纯文本输入（非标记）：把去掉标记的 placeholder 当提示显示，
        # 否则宿主只知道标题，看不到「怎么填」。
        if placeholder_text:
            message = placeholder_text
    return SessionNotice(
        notice_id=request_id,
        session_id=session_id,
        runtime="pi",
        type="interaction",
        title=title,
        message=message if marker is None else None,
        severity="info",
        status="open",
        interaction_type="input_request",
        blocking=blocking,
        response_required=True,
        actions=actions,
        source={"externalSessionId": external_session_id, "method": method},
        runtime_id=runtime_id,
    )


def _split_aa_marker(text: str | None) -> tuple[str | None, Mapping[str, Any] | None]:
    """从 placeholder 里摘下 ask 插件的 AA 标记行（它是单行 JSON，永远在末尾）。"""
    if not text or AA_MARKER_PREFIX not in text:
        return text, None
    lines = text.splitlines()
    for index in range(len(lines) - 1, -1, -1):
        line = lines[index]
        if not line.startswith(AA_MARKER_PREFIX):
            continue
        try:
            payload = json.loads(line[len(AA_MARKER_PREFIX) :])
        except ValueError:
            return text, None
        if not isinstance(payload, Mapping):
            return text, None
        del lines[index]
        return "\n".join(lines).strip() or None, payload
    return text, None


def _marker_back_value(marker: Mapping[str, Any] | None) -> str | None:
    """只有标记显式声明可回退时才认（第一题没有上一题）。"""
    if marker is None or marker.get("allowBack") is not True:
        return None
    value = marker.get("backValue")
    return value if isinstance(value, str) and value.strip() else None


def _marker_question(marker: Mapping[str, Any], title: str) -> InputRequestQuestion:
    """按 AA 标记构造多选题。选项 id 用 1-based 序号，插件正是按序号解析多选。"""
    options: list[InputRequestOption] = []
    raw_options = marker.get("options")
    if isinstance(raw_options, list):
        for raw in raw_options:
            if not isinstance(raw, Mapping):
                continue
            label = raw.get("label")
            if not isinstance(label, str) or not label.strip():
                continue
            description = raw.get("description")
            options.append(
                InputRequestOption(
                    option_id=str(len(options) + 1),
                    label=label.strip(),
                    description=(
                        description.strip()
                        if isinstance(description, str) and description.strip()
                        else None
                    ),
                )
            )
    prompt = marker.get("prompt")
    header = marker.get("header")
    return InputRequestQuestion(
        question_id="value",
        prompt=prompt.strip() if isinstance(prompt, str) and prompt.strip() else title,
        options=tuple(options),
        header=header.strip() if isinstance(header, str) and header.strip() else None,
        multiple=marker.get("kind") == "multiSelect",
        allow_custom=bool(marker.get("allowCustom", True)),
    )
