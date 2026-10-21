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
    RuntimeCapability,
    RuntimeCapabilitySet,
    RuntimeConfig,
    RuntimeIdentity,
    RuntimeInvalidRequestError,
    RuntimeModelCatalog,
    RuntimeOperationResult,
    RuntimePermissionCatalog,
    RuntimeStatus,
    RuntimeTimelineSnapshot,
    RuntimeUnsupportedError,
    SessionMeta,
    SessionNotice,
    SessionSourceState,
    SessionState,
)
from connector.runtimes.pi.catalog import build_models, read_model_config
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
MAX_SNAPSHOT_ITEMS = 5_000


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

    def _agent_dir(self) -> Path:
        return agent_dir(self.values)

    def _pi_executable(self) -> str:
        configured = self.values.get("piPath")
        return configured if isinstance(configured, str) and configured else "pi"

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

    async def list_sessions(
        self,
        limit: int = 100,
        cursor: str | None = None,
        force: bool = False,
    ) -> tuple[SessionMeta, ...]:
        if force or not self._index:
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
        status = self._active.get(session_id, "idle")
        return SessionState(
            session_id=session_id,
            external_session_id=str(summary.path.resolve()),
            runtime="pi",
            status=status,
            selections=self._selections.get(session_id, {}),
            runtime_id=self.config.runtime_id,
        )

    async def get_session_snapshot(
        self,
        session_id: str,
        external_session_id: str | None = None,
        limit: int | None = None,
    ) -> RuntimeTimelineSnapshot:
        summary = self._resolve(session_id, external_session_id)
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

    async def resynchronize(
        self,
        session_id: str | None = None,
        external_session_id: str | None = None,
    ) -> None:
        if session_id is None and external_session_id is None:
            self._snapshot_cache.clear()
            return
        summary = self._resolve(session_id or "", external_session_id)
        self._snapshot_cache.pop(str(summary.path.resolve()), None)

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
        if selections:
            self._selections[session_id] = dict(selections)
        process = await self._ensure_process(summary)
        await self._apply_selections(session_id, process)
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
        process = self._processes.get(str(summary.path.resolve()))
        if process is None or not process.running:
            raise RuntimeInvalidRequestError("会话当前没有运行中的 Pi 进程")
        try:
            await process.send("steer", message=content)
        except PiRpcError as exc:
            raise RuntimeInvalidRequestError(str(exc)) from exc
        self._activate(session_id, "running", summary)
        return RuntimeOperationResult(ok=True, result={"clientMessageId": client_message_id})

    async def interrupt_session(
        self,
        session_id: str,
        reason: str | None = None,
    ) -> RuntimeOperationResult:
        summary = self._resolve(session_id, None)
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
        pending = self._notices.get(session_id, {})
        return tuple(item.notice for item in pending.values())

    async def respond_interaction(
        self,
        session_id: str,
        notice_id: str,
        action_id: str,
        input_data: Mapping[str, Any] | None = None,
    ) -> RuntimeOperationResult:
        pending = self._notices.get(session_id, {})
        interaction = pending.pop(notice_id, None)
        if interaction is None:
            raise RuntimeInvalidRequestError("交互请求不存在或已处理")
        summary = self._resolve(session_id, None)
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

    def _handle_event(self, summary: PiSessionFile, record: dict[str, Any]) -> None:
        event_type = record.get("type")
        session_id = platform_session_id(self.host.connector_id, summary)
        if event_type == "agent_start":
            self._activate(session_id, "running", summary)
            return
        if event_type == "agent_settled":
            self._active.pop(session_id, None)
            self._notices.pop(session_id, None)
            self._report_state(session_id, summary, "idle")
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
