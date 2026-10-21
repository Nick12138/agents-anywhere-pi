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

        seen: list[list[tuple[str, str]]] = []

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
        assert seen == [[("cmid-order", "帮我看看这个 bug")]]

        snapshot = await runtime.get_session_snapshot(sid, ext)
        user = next(item for item in snapshot.items if item.role == "user")
        assert user.source["clientMessageId"] == "cmid-order"
        assert user.content_hash.endswith(":cmid:cmid-order")

        await runtime.stop()

    asyncio.run(run())
