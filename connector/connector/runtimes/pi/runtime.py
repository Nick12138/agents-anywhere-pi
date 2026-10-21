"""Pi Coding Agent runtime.

设计取向：
- **轮询同步**（AgentRuntime 默认）：会话快照直接读 JSONL，不维护事件订阅；
- **单写者**：同一会话的 prompt/steer/abort 都走同一个 `pi --mode rpc` 子进程；
- **只读会话目录**：列表/快照不修改任何文件；
- 进程按会话懒启动，Host 停止时统一回收。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC
from pathlib import Path
from typing import Any

from connector.runtime_protocol import (
    CAPABILITY_CATALOG_MODEL,
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
CATALOG_TTL_SECONDS = 60.0
INDEX_REFRESH_SECONDS = 3.0
MAX_SNAPSHOT_ITEMS = 5_000
# 流式推送节流：太快会把 websocket 刷爆，太慢手机上看着卡。
STREAM_PUSH_MIN_INTERVAL_SECONDS = 0.12
# 单个流式气泡最多推这么多字，剩下的等回合结束的完整快照。
STREAM_MAX_CHARS = 100_000


def _file_marker(path: Path) -> str | None:
    """会话文件的内容指纹：mtime+size 变了就认为需要重新同步时间线。"""
    try:
        stat = path.stat()
    except OSError:
        return None
    return f"{stat.st_mtime_ns}:{stat.st_size}"


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
class LiveStream:
    """正在流式输出的 assistant 消息（对应 `pi --mode rpc` 的 message_update）。"""

    item_id: str
    text: str = ""
    last_push_at: float = 0.0
    last_pushed_text: str = ""


@dataclass(slots=True)
class PendingInteraction:
    notice: SessionNotice
    method: str


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
        self._snapshot_cache: dict[str, tuple[int, int, RuntimeTimelineSnapshot]] = {}
        # 已发布到服务端的时间线指纹；list_sessions 拿它对比文件当前指纹，
        # 决定是否需要重新同步（polling 模式靠这个上报增量）。
        self._synced: dict[str, str] = {}
        self._index_refreshed_at = 0.0
        # 流式输出：session -> 当前正在生成的气泡
        self._streams: dict[str, LiveStream] = {}
        self._stream_seq = 0
        self._order_seq: dict[str, int] = {}
        # 前端发消息时带的 clientMessageId：写进时间线 item.source，客户端才能
        # 把“本地乐观气泡”换成服务端条目，而不是再追加一条（否则同一条消息显示两遍）。
        self._client_messages: dict[str, list[tuple[str, str]]] = {}
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
        if attachments:
            raise RuntimeUnsupportedError("pi runtime does not accept attachments yet")
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
        if attachments:
            raise RuntimeUnsupportedError("pi runtime does not accept attachments yet")
        summary = self._resolve(session_id, external_session_id)
        session_id = self._canonical_session_id(summary)
        if selections:
            self._selections[session_id] = dict(selections)
        process = await self._ensure_process(summary)
        await self._apply_selections(session_id, process)
        # 必须在 prompt 之前绑定：文件一写入就可能被轮询同步投影，
        # 那条先发出去的条目要是缺 clientMessageId，服务端按 contentHash 判定
        # 未变化，之后就再也补不上了（客户端于是多出一条乐观气泡）。
        self._remember_client_message(session_id, client_message_id, content)
        running = self._active.get(session_id) in {"running", "waiting_approval", "stopping"}
        try:
            if running:
                await process.send("steer", message=content)
                disposition = "queued"
            else:
                data = await process.request(
                    "prompt",
                    timeout=self._request_timeout(),
                    message=content,
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
        if attachments:
            raise RuntimeUnsupportedError("pi runtime does not accept attachments yet")
        summary = self._resolve(session_id, external_session_id)
        session_id = self._canonical_session_id(summary)
        process = self._processes.get(str(summary.path.resolve()))
        if process is None or not process.running:
            raise RuntimeInvalidRequestError("会话当前没有运行中的 Pi 进程")
        self._remember_client_message(session_id, client_message_id, content)
        try:
            await process.send("steer", message=content)
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
        self._activate(session_id, "running", summary)
        return RuntimeOperationResult(ok=True, result={"clientMessageId": client_message_id})

    def _remember_client_message(
        self,
        session_id: str,
        client_message_id: str | None,
        content: str,
    ) -> None:
        if not client_message_id:
            return
        bindings = self._client_messages.setdefault(session_id, [])
        bindings.append((client_message_id, content))
        del bindings[:-20]

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
                for client_message_id, sent_text in bindings:
                    if client_message_id in used or text != sent_text:
                        continue
                    used.add(client_message_id)
                    # 服务端只按 contentHash 判断条目是否变化（见 upstream
                    # core/timeline.py:timeline_item_state_is_unchanged），
                    # 只改 source 的话更新会被丢弃。
                    item = replace(
                        item,
                        source={**item.source, "clientMessageId": client_message_id},
                        content_hash=f"{item.content_hash}:cmid:{client_message_id}",
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
        return tuple(item.notice for item in pending.values())

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
        else:
            value = _answer_value(input_data)
            if value is None:
                raise RuntimeInvalidRequestError("回答缺少选项或文本")
            payload["value"] = value
        try:
            await process.send("extension_ui_response", **payload)
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
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
            return summary
        summary = self._index.get(session_id)
        if summary is None:
            self._refresh_index()
            summary = self._index.get(session_id)
        if summary is None:
            raise RuntimeInvalidRequestError(f"未知的 Pi 会话：{session_id}")
        return summary

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
        """把 pi 的 message_* 事件投影成一张会不断增长的 assistant 气泡。"""
        message = record.get("message")
        role = message.get("role") if isinstance(message, Mapping) else None
        event_type = record.get("type")
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
        if not isinstance(payload, Mapping) or payload.get("type") != "text_delta":
            return
        delta = payload.get("delta")
        if not isinstance(delta, str) or not delta:
            return
        if len(stream.text) >= STREAM_MAX_CHARS:
            return
        stream.text += delta
        now = time.monotonic()
        if now - stream.last_push_at < STREAM_PUSH_MIN_INTERVAL_SECONDS:
            return
        stream.last_push_at = now
        stream.last_pushed_text = stream.text
        path = str(summary.path.resolve())
        self._spawn(self.host.timeline_item_upsert(self._live_item(session_id, path, stream)))

    def _live_item(
        self,
        session_id: str,
        path: str,
        stream: LiveStream,
        order_seq: int | None = None,
    ) -> RuntimeTimelineItem:
        content: dict[str, Any] = {"kind": "text", "text": stream.text, "format": "text"}
        return RuntimeTimelineItem(
            id=stream.item_id,
            session_id=session_id,
            type="message",
            status="running",
            role="assistant",
            content=content,
            source={
                "runtime": "pi",
                "sessionId": path,
                "itemId": stream.item_id,
                "itemType": "message",
                "event": "message_update",
                "runtimeId": self.config.runtime_id,
            },
            order_seq=order_seq if order_seq is not None else self._next_order_seq(session_id),
            content_hash=timeline_content_hash(
                item_type="message",
                status="running",
                role="assistant",
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
        if stream is None or not stream.text:
            return snapshot
        max_order = max((item.order_seq for item in snapshot.items), default=0)
        live = self._live_item(
            session_id,
            snapshot.external_session_id or "",
            stream,
            order_seq=max_order + 1,
        )
        return replace(snapshot, items=(*snapshot.items, live))

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
        notice = build_notice(
            session_id=session_id,
            external_session_id=str(summary.path.resolve()),
            request_id=request_id,
            method=str(method),
            title=title,
            message=message,
            options=record.get("options"),
            timeout=record.get("timeout"),
            runtime_id=self.config.runtime_id,
        )
        self._notices.setdefault(session_id, {})[request_id] = PendingInteraction(
            notice=notice, method=str(method)
        )
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


def _answer_value(input_data: Mapping[str, Any] | None) -> str | None:
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
        return custom
    option_ids = first.get("optionIds")
    if isinstance(option_ids, list) and option_ids and isinstance(option_ids[0], str):
        return option_ids[0]
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
    timeout: Any,
    runtime_id: str | None,
) -> SessionNotice:
    blocking: dict[str, Any] = {"reason": "extension_ui", "method": method}
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
    if method == "select":
        question = InputRequestQuestion(
            question_id="value",
            prompt=message or title,
            options=tuple(InputRequestOption(option_id=label, label=label) for label in labels),
            header=title,
            multiple=False,
            allow_custom=False,
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
    form = InputRequestForm(questions=(question,))
    return SessionNotice(
        notice_id=request_id,
        session_id=session_id,
        runtime="pi",
        type="interaction",
        title=title,
        message=message,
        severity="info",
        status="open",
        interaction_type="input_request",
        blocking=blocking,
        response_required=True,
        actions=(form.action(label="提交"),),
        source={"externalSessionId": external_session_id, "method": method},
        runtime_id=runtime_id,
    )
