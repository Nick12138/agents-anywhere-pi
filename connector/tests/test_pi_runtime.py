from __future__ import annotations

import asyncio
import json
from pathlib import Path

from connector.runtime_protocol import RuntimeModelCatalog, RuntimeModelItem
from connector.runtimes.pi import sessions
from connector.runtimes.pi.provider import PiProvider


def write_session(root: Path, name: str, entries: list[dict]) -> Path:
    workspace = root / "--tmp-project--"
    workspace.mkdir(parents=True, exist_ok=True)
    path = workspace / f"2026-01-01T00-00-00-000Z_{name}.jsonl"
    path.write_text("\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n", encoding="utf-8")
    return path


def session_entries() -> list[dict]:
    return [
        {"type": "session", "version": 3, "id": "sess-1", "timestamp": "2026-01-01T00:00:00.000Z", "cwd": "/tmp/project"},
        {
            "type": "message",
            "id": "u1",
            "parentId": None,
            "timestamp": "2026-01-01T00:00:01.000Z",
            "message": {"role": "user", "content": "帮我看看这个 bug", "timestamp": 1},
        },
        {
            "type": "message",
            "id": "a1",
            "parentId": "u1",
            "timestamp": "2026-01-01T00:00:02.000Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "先读文件"},
                    {"type": "text", "text": "我先看看代码。"},
                    {"type": "toolCall", "id": "call_1", "name": "read", "arguments": {"path": "a.py"}},
                ],
                "stopReason": "toolUse",
                "timestamp": 2,
            },
        },
        {
            "type": "message",
            "id": "r1",
            "parentId": "a1",
            "timestamp": "2026-01-01T00:00:03.000Z",
            "message": {
                "role": "toolResult",
                "toolCallId": "call_1",
                "toolName": "read",
                "content": [{"type": "text", "text": "print(1)"}],
                "isError": False,
                "timestamp": 3,
            },
        },
        {
            "type": "session_info",
            "id": "n1",
            "parentId": "r1",
            "timestamp": "2026-01-01T00:00:04.000Z",
            "name": "修 bug",
        },
    ]


def test_scan_sessions_reads_header_and_name(tmp_path: Path) -> None:
    path = write_session(tmp_path, "one", session_entries())
    found = sessions.scan_sessions({"sessionDir": str(tmp_path)}, "conn_test")
    assert len(found) == 1
    summary = found[0]
    assert summary.path == path
    assert summary.cwd == "/tmp/project"
    assert summary.name == "修 bug"
    assert summary.title == "修 bug"


def test_build_timeline_projects_message_reasoning_and_tool(tmp_path: Path) -> None:
    path = write_session(tmp_path, "one", session_entries())
    entries = sessions.read_entries(path)
    items = sessions.build_timeline("sess_platform", str(path), entries)
    types = [(item.type, item.role, item.status) for item in items]
    assert ("message", "user", "done") in types
    assert ("system", "assistant", "done") in types
    assert any(item.type == "tool" and item.content.get("kind") == "tool_result" for item in items)
    tool = next(item for item in items if item.type == "tool")
    assert tool.content.get("output") == "print(1)"
    assert tool.content.get("title") == "read"
    assert [item.order_seq for item in items] == list(range(len(items)))


def test_current_branch_ignores_abandoned_sibling(tmp_path: Path) -> None:
    entries = session_entries()
    entries.insert(
        3,
        {
            "type": "message",
            "id": "old",
            "parentId": "u1",
            "timestamp": "2026-01-01T00:00:01.500Z",
            "message": {"role": "assistant", "content": [{"type": "text", "text": "旧分支"}], "stopReason": "stop"},
        },
    )
    path = write_session(tmp_path, "branch", entries)
    items = sessions.build_timeline("sess_platform", str(path), sessions.read_entries(path))
    texts = [item.content.get("text") for item in items if item.type == "message"]
    assert "旧分支" not in texts
    assert "我先看看代码。" in texts


class FakeHost:
    connector_id = "conn_test"

    def __init__(self) -> None:
        self.catalogs = []
        self.upserts = []
        self.timelines = []
        self.attachments = {}
        self.notices = []

    async def timeline_item_upsert(self, item) -> None:
        self.upserts.append(item)

    async def notice_upsert(self, notice) -> None:
        self.notices.append(notice)

    async def timeline_sync(self, **kwargs) -> None:
        self.timelines.append(kwargs)

    async def attachment_download(self, session_id, file_id):
        return self.attachments[file_id]

    async def model_catalog_update(self, catalog) -> None:
        self.catalogs.append(catalog)

    async def session_state_update(self, *args, **kwargs) -> None:
        return None

    async def session_turn_ended(self, *args, **kwargs) -> None:
        return None

    async def session_meta_upsert(self, *args, **kwargs) -> None:
        return None


def test_provider_config_and_snapshot(tmp_path: Path) -> None:
    write_session(tmp_path, "one", session_entries())

    async def run() -> None:
        provider = PiProvider()
        config = await provider.validate_config({"agentDir": str(tmp_path), "sessionDir": str(tmp_path)})
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10)
        assert len(listed) == 1
        state = await runtime.get_session_state(listed[0].session_id, listed[0].external_session_id)
        assert state is not None and state.status == "idle"
        snapshot = await runtime.get_session_snapshot(listed[0].session_id, listed[0].external_session_id)
        assert snapshot.complete is True
        assert len(snapshot.items) == 4
        await runtime.stop()

    asyncio.run(run())


def test_model_catalog_is_published_with_runtime_id(tmp_path: Path) -> None:
    async def run() -> None:
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        host = FakeHost()
        runtime = await provider.create_runtime(config, host)
        models = (
            RuntimeModelItem(
                id="2/gpt-6-luna",
                title="NingYi / gpt-6-luna",
                selection_id="model:2/gpt-6-luna",
                description="2 · gpt-6-luna",
                reasoning_items=(),
                metadata={"provider": "2"},
            ),
        )

        async def load() -> RuntimeModelCatalog:
            return RuntimeModelCatalog(
                runtime="pi",
                runtime_id=config.runtime_id,
                revision=1,
                models=models,
            )

        runtime._load_model_catalog = load  # type: ignore[method-assign]
        catalog = await runtime.list_model_catalog(limit=10)
        assert catalog.revision == host.catalogs[-1].revision
        assert catalog.revision > 1
        assert host.catalogs[-1].runtime_id == config.runtime_id
        assert [item.id for item in host.catalogs[-1].models] == ["2/gpt-6-luna"]
        # 内容没变不重复上报
        await runtime.list_model_catalog(limit=10)
        assert len(host.catalogs) == 1
        await runtime.stop()

    asyncio.run(run())


def test_create_session_file_matches_pi_layout(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    summary = sessions.create_session_file(
        {"sessionDir": str(tmp_path)},
        str(project),
        title="新会话",
        native_id="sess-new",
    )
    assert summary.cwd == str(project.resolve())
    assert summary.path.parent.name == sessions.encode_session_dir(str(project))
    assert summary.path.name.endswith("_sess-new.jsonl")
    assert summary.name == "新会话"
    header = json.loads(summary.path.read_text(encoding="utf-8").splitlines()[0])
    assert header == {
        "type": "session",
        "version": 3,
        "id": "sess-new",
        "timestamp": header["timestamp"],
        "cwd": str(project.resolve()),
    }


def test_create_and_start_session_creates_pi_session(tmp_path: Path) -> None:
    class FakeProcess:
        async def request(self, method: str, timeout: float | None = None, **kwargs):
            if method == "prompt":
                return {"disposition": "started"}
            return {}

        async def send(self, method: str, **kwargs) -> None:
            return None

    async def run() -> None:
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())

        async def ensure_process(summary) -> FakeProcess:
            return FakeProcess()

        async def apply_selections(session_id: str, process: FakeProcess) -> None:
            return None

        runtime._ensure_process = ensure_process  # type: ignore[method-assign]
        runtime._apply_selections = apply_selections  # type: ignore[method-assign]
        project = tmp_path / "project"
        project.mkdir()
        result = await runtime.create_and_start_session(
            session_id="sess-platform",
            content="你好",
            title="来自 AA",
            cwd=str(project),
        )
        external = result.result["externalSessionId"]
        assert result.ok is True
        assert result.result["sessionId"] == "sess-platform"
        assert Path(external).exists()
        assert sessions.read_summary(Path(external)).name == "来自 AA"
        await runtime.stop()

    asyncio.run(run())


def test_model_catalog_uses_provider_names_and_filters() -> None:
    from connector.runtimes.pi.catalog import ModelConfig, build_models

    config = ModelConfig(
        provider_names={"2": "NingYi Luna", "14": "移动云"},
        enabled_providers=frozenset({"2"}),
        provider_models={"2": ("gpt-6-luna",)},
    )
    models = build_models(
        [
            {"id": "gpt-6-luna", "name": "gpt-6-luna", "provider": "2", "thinkingLevelMap": {"off": None, "high": "high"}},
            {"id": "gpt-6-sol", "name": "gpt-6-sol", "provider": "2"},
            {"id": "glm-5.3", "name": "glm-5.3", "provider": "14", "reasoning": True},
            {"id": "no-name", "provider": "99"},
        ],
        config,
    )
    assert [item.title for item in models] == ["NingYi Luna / gpt-6-luna"]
    luna = models[0]
    assert luna.description == "2 · gpt-6-luna"
    assert luna.metadata["providerName"] == "NingYi Luna"
    assert [item.title for item in luna.reasoning_items] == ["high"]


def test_model_config_reads_piabyss_enablement(tmp_path: Path) -> None:
    from connector.runtimes.pi.catalog import read_model_config

    (tmp_path / "models.json").write_text(
        json.dumps(
            {
                "providers": {
                    "2": {"name": "NingYi Luna", "models": [{"id": "gpt-6-luna"}]},
                    "14": {"name": "移动云", "models": [{"id": "glm-5.3"}]},
                },
                "piabyssEnabledProviders": ["14"],
                "piabyssProviderModels": {"14": ["glm-5.3"]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    config = read_model_config(tmp_path)
    assert config.provider_names == {"2": "NingYi Luna", "14": "移动云"}
    assert config.enabled_providers == frozenset({"14"})
    assert config.provider_models == {"14": ("glm-5.3",)}

    (tmp_path / "models.json").write_text('{"providers": {}}', encoding="utf-8")
    no_filter = read_model_config(tmp_path)
    assert no_filter.enabled_providers is None
    assert no_filter.provider_models == {}


def test_pi_executable_resolves_from_path(monkeypatch, tmp_path: Path) -> None:
    """Windows 上裸 `pi` 会让 CreateProcess 找不到 npm 的 pi.cmd，必须解析出真实路径。"""
    from connector.runtimes.pi import discovery

    async def run() -> None:
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())

        monkeypatch.setattr(discovery.shutil, "which", lambda _name: None)
        assert runtime._pi_executable() == "pi"

        fake = tmp_path / "pi.cmd"
        fake.write_text("", encoding="utf-8")
        monkeypatch.setattr(discovery.shutil, "which", lambda _name: str(fake))
        assert runtime._pi_executable() == str(fake)

        await runtime.stop()

    asyncio.run(run())


def test_timeline_sync_marker_tracks_file_changes(tmp_path: Path) -> None:
    """polling 同步靠 sync 元数据判断增量；不设置就永远不会重新拉时间线。"""

    async def run() -> None:
        path = write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())

        listed = await runtime.list_sessions(limit=10, force=True)
        assert len(listed) == 1
        sync = listed[0].metadata["sync"]
        assert sync["requires_timeline_sync"] is True
        assert sync["changed"] is True

        prepared = await runtime.prepare_session_timeline_sync(
            listed[0].session_id, listed[0].external_session_id
        )
        assert prepared.snapshot is not None and prepared.commit is not None
        await prepared.commit()

        listed = await runtime.list_sessions(limit=10, force=True)
        assert listed[0].metadata["sync"]["requires_timeline_sync"] is False

        with path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "type": "message",
                        "id": "u2",
                        "parentId": "a1",
                        "timestamp": "2026-01-01T00:00:05.000Z",
                        "message": {"role": "user", "content": "再改一版", "timestamp": 5},
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

        listed = await runtime.list_sessions(limit=10, force=True)
        assert listed[0].metadata["sync"]["requires_timeline_sync"] is True

        # resynchronize 会丢掉指纹，强制整会话重推
        await runtime.prepare_session_timeline_sync(
            listed[0].session_id, listed[0].external_session_id
        )
        await runtime.resynchronize(listed[0].session_id, listed[0].external_session_id)
        listed = await runtime.list_sessions(limit=10, force=True)
        assert listed[0].metadata["sync"]["requires_timeline_sync"] is True

        await runtime.stop()

    asyncio.run(run())


def test_session_state_reconciles_stale_running(tmp_path: Path) -> None:
    """丢了 agent_settled 时不能永远显示「正在工作」。"""

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id
        key = str(Path(ext).resolve())

        # 没有 RPC 进程存活 → 直接当成 idle
        runtime._active[sid] = "running"
        state = await runtime.get_session_state(sid, ext)
        assert state is not None and state.status == "idle"
        assert sid not in runtime._active

        class StreamingProcess:
            running = True

            async def request(self, command, timeout=None, **fields):
                assert command == "get_state"
                return {"isStreaming": True}

        class IdleProcess(StreamingProcess):
            async def request(self, command, timeout=None, **fields):
                return {"isStreaming": False}

        runtime._active[sid] = "running"
        runtime._processes[key] = StreamingProcess()  # type: ignore[assignment]
        state = await runtime.get_session_state(sid, ext)
        assert state is not None and state.status == "running"

        runtime._processes[key] = IdleProcess()  # type: ignore[assignment]
        state = await runtime.get_session_state(sid, ext)
        assert state is not None and state.status == "idle"
        assert sid not in runtime._active

        runtime._processes.pop(key, None)
        await runtime.stop()

    asyncio.run(run())


def test_user_message_carries_client_message_id(tmp_path: Path) -> None:
    """前端靠 source.clientMessageId 把乐观气泡换成服务端条目，否则一条消息显示两遍。"""

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id

        before = await runtime.get_session_snapshot(sid, ext)
        user_items = [item for item in before.items if item.role == "user"]
        assert user_items and "clientMessageId" not in user_items[0].source

        runtime._remember_client_message(sid, "cmid-1", "帮我看看这个 bug")
        after = await runtime.get_session_snapshot(sid, ext)
        user_items = [item for item in after.items if item.role == "user"]
        assert user_items[0].source["clientMessageId"] == "cmid-1"
        # 只绑一次，重复读不会串到下一条
        assert [item.source.get("clientMessageId") for item in after.items if item.role == "user"] == [
            "cmid-1"
        ]

        # 后来又多了一条客户端消息也不会串号：还是只绑第一条用户消息
        runtime._remember_client_message(sid, "cmid-2", "另一条消息")
        again = await runtime.get_session_snapshot(sid, ext)
        user_items = [item for item in again.items if item.role == "user"]
        assert [item.source["clientMessageId"] for item in user_items] == ["cmid-1"]

        await runtime.stop()

    asyncio.run(run())


def test_client_message_bound_before_prompt_and_hash_bumped(tmp_path: Path) -> None:
    """绑定要早于 prompt（否则先发出去的条目缺 cmid），且 contentHash 要变。"""

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id

        seen: list[list[object]] = []

        class FakeProcess:
            running = True

            async def request(self, command, timeout=None, **fields):
                if command == "prompt":
                    seen.append(list(runtime._client_messages.get(sid, [])))
                return {"disposition": "started"}

            async def send(self, command, **fields):
                return None

        async def fake_ensure(_summary):
            return FakeProcess()

        runtime._ensure_process = fake_ensure  # type: ignore[method-assign]
        await runtime.start_turn(sid, ext, "帮我看看这个 bug", {}, (), "cmid-order")
        assert len(seen) == 1 and len(seen[0]) == 1
        binding = seen[0][0]
        assert binding.client_message_id == "cmid-order"
        assert binding.text == "帮我看看这个 bug" 

        snapshot = await runtime.get_session_snapshot(sid, ext)
        user = next(item for item in snapshot.items if item.role == "user")
        assert user.source["clientMessageId"] == "cmid-order"
        assert user.content_hash.endswith(":cmid:cmid-order")

        await runtime.stop()

    asyncio.run(run())


def test_turn_with_server_side_session_id_binds_client_message(tmp_path: Path) -> None:
    """服务端会用自己那套 session id 调 RPC，内部状态必须归一到 platform id。"""

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10, force=True)
        canonical, ext = listed[0].session_id, listed[0].external_session_id
        foreign = "sess_AAuQaz7Pp6sjhg"

        class FakeProcess:
            running = True

            async def request(self, command, timeout=None, **fields):
                if command == "get_state":
                    return {"isStreaming": True}
                return {"disposition": "started"}

            async def send(self, command, **fields):
                return None

            async def close(self, *, grace: float = 0.0):
                pass

        async def fake_ensure(summary):
            process = FakeProcess()
            runtime._processes[str(summary.path.resolve())] = process  # type: ignore[assignment]
            return process

        runtime._ensure_process = fake_ensure  # type: ignore[method-assign]
        await runtime.start_turn(foreign, ext, "帮我看看这个 bug", {}, (), "cmid-foreign")

        # 内部状态统一挂在 platform id 下，事件路径才看得见
        assert canonical in runtime._client_messages
        assert canonical in runtime._active
        assert foreign not in runtime._client_messages

        state = await runtime.get_session_state(foreign, ext)
        assert state is not None
        # 对外仍回调用方的 id
        assert state.session_id == foreign
        assert state.status == "running"

        snapshot = await runtime.get_session_snapshot(foreign, ext)
        user = next(item for item in snapshot.items if item.role == "user")
        assert user.source["clientMessageId"] == "cmid-foreign"

        await runtime.stop()

    asyncio.run(run())


def test_streaming_pushes_growing_assistant_item(tmp_path: Path) -> None:
    """流式输出：message_update 的 text_delta 要变成一张不断变长的气泡。"""

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        host = FakeHost()
        runtime = await provider.create_runtime(config, host)
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id
        summary = runtime._resolve(sid, ext)

        runtime._handle_event(summary, {"type": "message_start", "message": {"role": "assistant"}})
        for delta in ("你", "好", "呀"):
            runtime._handle_event(
                summary,
                {
                    "type": "message_update",
                    "assistantMessageEvent": {"type": "text_delta", "delta": delta},
                },
            )
        await asyncio.sleep(0.05)

        assert host.upserts, "应该至少推一次流式气泡"
        live = host.upserts[0]
        assert live.status == "running" and live.role == "assistant"
        assert live.content["text"].startswith("你")

        snapshot = await runtime.get_session_snapshot(sid, ext)
        running = [item for item in snapshot.items if item.status == "running"]
        assert running and running[0].id == live.id

        runtime._handle_event(summary, {"type": "message_end", "message": {"role": "assistant"}})
        snapshot = await runtime.get_session_snapshot(sid, ext)
        assert not [item for item in snapshot.items if item.status == "running"]

        await runtime.stop()

    asyncio.run(run())


def test_session_state_recovers_selection_from_jsonl(tmp_path: Path) -> None:
    """重启后从 JSONL 恢复当前模型，别让手机显示成默认模型。"""

    async def run() -> None:
        base = session_entries()
        entries = [
            base[0],
            {
                "type": "model_change",
                "id": "m1",
                "parentId": None,
                "timestamp": "2026-01-01T00:00:00.000Z",
                "provider": "13",
                "modelId": "deepseek-v4.1-flash",
            },
            {
                "type": "thinking_level_change",
                "id": "t1",
                "parentId": "m1",
                "timestamp": "2026-01-01T00:00:01.000Z",
                "thinkingLevel": "high",
            },
            *base[1:],
            {
                "type": "model_change",
                "id": "m2",
                "parentId": "a1",
                "timestamp": "2026-01-01T00:00:09.000Z",
                "provider": "17",
                "modelId": "GLM-5.3-Flash",
            },
        ]
        write_session(tmp_path, "one", entries)
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10, force=True)
        state = await runtime.get_session_state(listed[0].session_id, listed[0].external_session_id)
        assert state is not None
        assert state.selections["model"] == "model:17/GLM-5.3-Flash#high"
        await runtime.stop()

    asyncio.run(run())


def test_attachments_go_to_pi_images_and_files(tmp_path: Path, monkeypatch) -> None:
    """图片塞进 rpc 的 images；其他文件落盘并把路径写进提示词。"""

    from connector.runtime_protocol import RuntimeAttachment, RuntimeAttachmentContent

    monkeypatch.setenv("AGENT_CONNECTOR_ATTACHMENTS_ROOT", str(tmp_path / "attachments"))
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        host = FakeHost()
        host.attachments = {
            "file_img": RuntimeAttachmentContent(
                file_id="file_img", name="shot.png", media_type="image/png", content=png
            ),
            "file_doc": RuntimeAttachmentContent(
                file_id="file_doc", name="notes.md", media_type="text/markdown", content="# 标题".encode()
            ),
        }
        runtime = await provider.create_runtime(config, host)
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id

        calls: list[dict] = []

        class FakeProcess:
            running = True

            async def request(self, command, timeout=None, **fields):
                if command == "prompt":
                    calls.append(fields)
                    return {"disposition": "started"}
                return {}

            async def send(self, command, **fields):
                calls.append(fields)

            async def close(self, *, grace: float = 0.0):
                pass

        async def fake_ensure(_summary):
            return FakeProcess()

        runtime._ensure_process = fake_ensure  # type: ignore[method-assign]
        attachments = (
            RuntimeAttachment(file_id="file_img", name="shot.png", media_type="image/png", size=len(png)),
            RuntimeAttachment(file_id="file_doc", name="notes.md", media_type="text/markdown", size=4),
        )
        await runtime.start_turn(sid, ext, "帮我看看这个 bug", {}, attachments, "cmid-files")

        prompt = calls[0]
        assert prompt["message"].startswith("帮我看看这个 bug")
        assert "notes.md" in prompt["message"] and "shot.png" not in prompt["message"]
        assert len(prompt["images"]) == 1
        assert prompt["images"][0]["mimeType"] == "image/png"
        assert prompt["images"][0]["type"] == "image"
        assert prompt["images"][0]["data"]

        snapshot = await runtime.get_session_snapshot(sid, ext)
        user = next(item for item in snapshot.items if item.role == "user")
        names = [a["name"] for a in user.content["attachments"]]
        assert names == ["shot.png", "notes.md"]
        assert user.source["clientMessageId"] == "cmid-files"

        await runtime.stop()

    asyncio.run(run())


def _partial(*blocks: dict) -> dict:
    return {"partial": {"role": "assistant", "content": list(blocks), "stopReason": "stop"}}


def test_streaming_covers_thinking_tools_and_errors(tmp_path: Path) -> None:
    """思考、工具调用要边跑边出卡片；失败回合要有可见报错。"""

    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        host = FakeHost()
        runtime = await provider.create_runtime(config, host)
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id
        summary = runtime._resolve(sid, ext)

        runtime._handle_event(summary, {"type": "message_start", "message": {"role": "assistant"}})
        runtime._handle_event(
            summary,
            {
                "type": "message_update",
                "assistantMessageEvent": {
                    "type": "thinking_delta",
                    **_partial({"type": "thinking", "thinking": "先看看代码"}),
                },
            },
        )
        runtime._handle_event(
            summary,
            {
                "type": "message_update",
                "assistantMessageEvent": {
                    "type": "toolcall_end",
                    **_partial(
                        {"type": "thinking", "thinking": "先看看代码"},
                        {"type": "text", "text": "我来读文件"},
                        {"type": "toolCall", "id": "call_1", "name": "read", "arguments": {"path": "a.py"}},
                    ),
                },
            },
        )
        await asyncio.sleep(0.05)

        kinds = {(item.type, item.content.get("kind")) for item in host.upserts}
        assert ("system", "reasoning") in kinds, kinds
        assert ("message", "text") in kinds, kinds
        assert ("tool", "tool_call") in kinds, kinds

        tool_item = next(item for item in host.upserts if item.type == "tool")
        assert tool_item.content["title"] == "read"
        assert "a.py" in tool_item.content["input"]

        # 快照里也应带上这些临时卡片
        snapshot = await runtime.get_session_snapshot(sid, ext)
        live_ids = {item.id for item in host.upserts}
        assert live_ids <= {item.id for item in snapshot.items}

        # 失败事件 → 立刻推一条 error 气泡
        runtime._handle_event(summary, {"type": "message_start", "message": {"role": "assistant"}})
        runtime._handle_event(
            summary,
            {
                "type": "message_update",
                "assistantMessageEvent": {
                    "type": "error",
                    "reason": "error",
                    "error": {"role": "assistant", "content": [], "errorMessage": "401 invalid api key"},
                },
            },
        )
        await asyncio.sleep(0.05)
        errors = [item for item in host.upserts if item.content.get("kind") == "error"]
        assert errors and "401" in errors[0].content["text"]

        await runtime.stop()

    asyncio.run(run())


def test_auto_retry_notice_visible(tmp_path: Path) -> None:
    async def run() -> None:
        write_session(tmp_path, "one", session_entries())
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        host = FakeHost()
        runtime = await provider.create_runtime(config, host)
        listed = await runtime.list_sessions(limit=10, force=True)
        sid, ext = listed[0].session_id, listed[0].external_session_id
        summary = runtime._resolve(sid, ext)

        runtime._handle_event(
            summary,
            {
                "type": "auto_retry_start",
                "attempt": 2,
                "maxAttempts": 5,
                "delayMs": 800,
                "errorMessage": "503 service unavailable",
            },
        )
        await asyncio.sleep(0.05)
        notices = [item for item in host.upserts if item.content.get("kind") == "error"]
        assert notices and "503" in notices[0].content["text"] and "2/5" in notices[0].content["text"]

        snapshot = await runtime.get_session_snapshot(sid, ext)
        assert any(item.content.get("kind") == "error" for item in snapshot.items)

        runtime._handle_event(summary, {"type": "auto_retry_end", "success": True, "attempt": 2})
        await asyncio.sleep(0.05)
        assert sid not in runtime._retry_notices
        await runtime.stop()

    asyncio.run(run())


def test_failed_assistant_message_projects_error_item(tmp_path: Path) -> None:
    """JSONL 里 stopReason=error + 空正文时，不能什么都不显示。"""

    async def run() -> None:
        base = session_entries()
        entries = [
            *base,
            {
                "type": "message",
                "id": "err1",
                "parentId": "a1",
                "timestamp": "2026-01-01T00:00:20.000Z",
                "message": {
                    "role": "assistant",
                    "content": [],
                    "stopReason": "error",
                    "errorMessage": "Provider returned 429 Too Many Requests",
                },
            },
        ]
        write_session(tmp_path, "one", entries)
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        runtime = await provider.create_runtime(config, FakeHost())
        listed = await runtime.list_sessions(limit=10, force=True)
        snapshot = await runtime.get_session_snapshot(
            listed[0].session_id, listed[0].external_session_id
        )
        errors = [item for item in snapshot.items if item.content.get("kind") == "error"]
        assert errors and "429" in errors[0].content["text"]
        assert errors[0].status == "failed"
        await runtime.stop()

    asyncio.run(run())


def test_extension_ui_notice_blocking_matches_protocol() -> None:
    """blocking 必须是 {scope: session, targetId: <sessionId>}。

    服务端 NoticeBlocking 的 scope/targetId 都是必填字段，前端也只把
    scope == "session" 且 targetId == sessionId 的 notice 当阻塞交互渲染。
    早先写成 {"reason": "extension_ui", "method": ...} 会直接被服务端校验拒绝，
    用户在 Web/客户端里永远看不到 confirm/select 弹窗。
    """
    from connector.runtimes.pi.runtime import build_notice

    confirm = build_notice(
        session_id="sess_1",
        external_session_id="/tmp/pi/s1.jsonl",
        request_id="req_1",
        method="confirm",
        title="要执行吗",
        message=None,
        options=None,
        timeout=None,
        runtime_id="rt-1",
    )
    assert confirm.blocking == {"scope": "session", "targetId": "sess_1"}
    assert confirm.source["method"] == "confirm"

    select = build_notice(
        session_id="sess_1",
        external_session_id="/tmp/pi/s1.jsonl",
        request_id="req_2",
        method="select",
        title="选一个",
        message="选哪个？",
        options=["a", "b"],
        timeout=3000,
        runtime_id="rt-1",
    )
    assert select.blocking == {"scope": "session", "targetId": "sess_1", "timeoutMs": 3000}
    assert select.source["method"] == "select"


def test_extension_ui_request_pushed_immediately(tmp_path: Path) -> None:
    """pi 的交互 notice 必须当场推给服务端，不能只等 30s 一次的 session sync。

    claude / codex 都是收到交互就 notice_upsert；pi 之前只把 notice 存本地，
    手机端要等一个同步周期，且 sync 仅在会话文件变化时才带 notice。
    """
    write_session(tmp_path, "one", session_entries())

    async def run() -> None:
        provider = PiProvider()
        config = await provider.validate_config(
            {"agentDir": str(tmp_path), "sessionDir": str(tmp_path)}
        )
        host = FakeHost()
        runtime = await provider.create_runtime(config, host)
        listed = await runtime.list_sessions(limit=10)
        session_id = listed[0].session_id
        summary = runtime._index[session_id]
        runtime._handle_extension_request(
            session_id,
            summary,
            {
                "type": "extension_ui_request",
                "id": "req_1",
                "method": "select",
                "title": "[测试] 随便选一个",
                "options": ["清晨", "深夜", "Type something."],
            },
        )
        await asyncio.sleep(0)
        assert [notice.notice_id for notice in host.notices] == ["req_1"]
        notice = host.notices[0]
        assert notice.type == "interaction"
        assert notice.blocking == {"scope": "session", "targetId": session_id}
        assert await runtime.get_session_notices(session_id) == (notice,)
        await runtime.stop()

    asyncio.run(run())
