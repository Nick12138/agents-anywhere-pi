"""Pi Coding Agent 会话文件的读取与时间线投影。

AA 的 pi runtime 不走 bridge：会话就是磁盘上的 JSONL，直接读比在 Pi 进程里再
造一套投影简单得多。这里只做两件事：

- 列表：扫描 `<agentDir>/sessions/*/*.jsonl`，拿 header、名字、首条用户消息；
- 投影：按当前分支把会话条目转成 AA 的 timeline item。

时间线只使用协议里已有的类型（message / system.reasoning / tool），不发明内容形状。
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from connector.runtime_protocol import (
    MessageTimelineItem,
    ReasoningSystemContent,
    RuntimeTimelineItem,
    SystemTimelineItem,
    TextMessageContent,
    TimelineSource,
    ToolCallContent,
    ToolResultContent,
    ToolTimelineItem,
)
from connector.runtimes.session_identity import stable_runtime_session_id

DEFAULT_AGENT_DIR = Path.home() / ".pi" / "agent"
MAX_LISTING_BYTES = 256 * 1024
MAX_TAIL_BYTES = 64 * 1024
MAX_BLOCK_TEXT = 20_000
MAX_TOOL_INPUT = 4_000


def agent_dir(values: dict[str, Any]) -> Path:
    configured = values.get("agentDir")
    if isinstance(configured, str) and configured.strip():
        return Path(configured).expanduser()
    env = os.environ.get("PI_CODING_AGENT_DIR")
    if env and env.strip():
        return Path(env).expanduser()
    return DEFAULT_AGENT_DIR


def session_roots(values: dict[str, Any]) -> tuple[Path, ...]:
    configured = values.get("sessionDir")
    if isinstance(configured, str) and configured.strip():
        return (Path(configured).expanduser(),)
    return (agent_dir(values) / "sessions",)


@dataclass(frozen=True, slots=True)
class PiSessionFile:
    path: Path
    native_id: str
    cwd: str
    title: str
    name: str | None
    updated_at: datetime
    message_count: int | None


def _read_json_line(line: str) -> dict[str, Any] | None:
    text = line.strip()
    if not text:
        return None
    try:
        value = json.loads(text)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
    return "\n".join(parts).strip()


def _truncate(text: str, limit: int = MAX_BLOCK_TEXT) -> str:
    return text if len(text) <= limit else f"{text[:limit]}…"


def _recent_name(path: Path) -> str | None:
    """从文件尾部找最近一次 session_info 的名字；避免为列表整文件解析。"""
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - MAX_TAIL_BYTES))
            tail = handle.read().decode("utf-8", "replace")
    except OSError:
        return None
    name: str | None = None
    for line in tail.splitlines():
        record = _read_json_line(line)
        if record is None or record.get("type") != "session_info":
            continue
        value = record.get("name")
        name = value.strip() if isinstance(value, str) and value.strip() else None
    return name


def encode_session_dir(cwd: str) -> str:
    """复刻 Pi 的 `--<cwd>--` 目录名：去掉开头分隔符，再把分隔符与冒号换成 -。"""

    resolved = str(Path(cwd).expanduser().resolve())
    stripped = re.sub(r"^[/\\]", "", resolved)
    return f"--{re.sub(r'[/\\:]', '-', stripped)}--"


def create_session_file(
    values: dict[str, Any],
    cwd: str,
    *,
    title: str | None = None,
    native_id: str | None = None,
) -> PiSessionFile:
    """按 Pi 的格式预建会话文件；首条 prompt 之前就能被 AA 引用。"""

    target_cwd = str(Path(cwd).expanduser().resolve())
    directory = session_roots(values)[0] / encode_session_dir(target_cwd)
    directory.mkdir(parents=True, exist_ok=True)
    session_id = native_id or str(uuid.uuid4())
    now = datetime.now(UTC)
    timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    stamp = now.strftime("%Y-%m-%dT%H-%M-%S-") + f"{now.microsecond // 1000:03d}Z"
    path = directory / f"{stamp}_{session_id}.jsonl"
    records: list[dict[str, Any]] = [
        {
            "type": "session",
            "version": 3,
            "id": session_id,
            "timestamp": timestamp,
            "cwd": target_cwd,
        }
    ]
    if title and title.strip():
        records.append(
            {
                "type": "session_info",
                "id": uuid.uuid4().hex[:8],
                "parentId": None,
                "timestamp": timestamp,
                "name": title.strip(),
            }
        )
    body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records)
    path.write_text(body + "\n", encoding="utf-8")
    summary = read_summary(path)
    if summary is None:
        raise OSError(f"创建的会话文件不可读：{path}")
    return summary


def read_summary(path: Path) -> PiSessionFile | None:
    try:
        with path.open("rb") as handle:
            head = handle.read(MAX_LISTING_BYTES).decode("utf-8", "replace")
    except OSError:
        return None
    lines = head.splitlines()
    if not lines:
        return None
    header = _read_json_line(lines[0])
    if header is None or header.get("type") != "session" or not isinstance(header.get("id"), str):
        return None
    cwd = header.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        return None
    first_user = ""
    for line in lines[1:]:
        record = _read_json_line(line)
        if record is None or record.get("type") != "message":
            continue
        message = record.get("message")
        if isinstance(message, dict) and message.get("role") == "user":
            first_user = _message_text(message.get("content"))
            if first_user:
                break
    try:
        updated_at = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    except OSError:
        updated_at = datetime.now(UTC)
    name = _recent_name(path)
    title = name or first_user.splitlines()[0][:80] if first_user else name or path.stem
    return PiSessionFile(
        path=path,
        native_id=header["id"],
        cwd=cwd,
        title=title or path.stem,
        name=name,
        updated_at=updated_at,
        message_count=None,
    )


def scan_sessions(values: dict[str, Any], connector_id: str) -> list[PiSessionFile]:
    """按修改时间倒序返回可读会话；单个坏文件不影响其它会话。"""

    found: list[PiSessionFile] = []
    seen: set[str] = set()
    for root in session_roots(values):
        if not root.is_dir():
            continue
        try:
            candidates = list(root.rglob("*.jsonl"))
        except OSError:
            continue
        for path in candidates:
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            summary = read_summary(path)
            if summary is not None:
                found.append(summary)
    found.sort(key=lambda item: item.updated_at, reverse=True)
    return found


def platform_session_id(connector_id: str, summary: PiSessionFile) -> str:
    return stable_runtime_session_id(connector_id, "pi", str(summary.path.resolve()))


def read_entries(path: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                record = _read_json_line(line)
                if record is not None:
                    entries.append(record)
    except OSError:
        return []
    return entries


def current_branch(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """只保留当前叶子的祖先链；线性会话就是原顺序。"""

    if not entries:
        return []
    header = entries[0] if entries[0].get("type") == "session" else None
    body = entries[1:] if header is not None else entries
    by_id = {
        entry["id"]: entry
        for entry in body
        if isinstance(entry.get("id"), str)
    }
    if not by_id:
        return entries
    leaf: dict[str, Any] | None = None
    for entry in reversed(body):
        if isinstance(entry.get("id"), str):
            leaf = entry
            break
    if leaf is None:
        return entries
    chain: list[dict[str, Any]] = []
    cursor: dict[str, Any] | None = leaf
    while cursor is not None:
        chain.append(cursor)
        parent_id = cursor.get("parentId")
        cursor = by_id.get(parent_id) if isinstance(parent_id, str) else None
    chain.reverse()
    return ([header] if header is not None else []) + chain


def _source(external_id: str, entry: dict[str, Any], suffix: str | None = None) -> TimelineSource:
    native_id = entry.get("id") if isinstance(entry.get("id"), str) else None
    return TimelineSource(
        runtime="pi",
        external_session_id=external_id,
        native_item_id=native_id,
        native_item_type=entry.get("type") if isinstance(entry.get("type"), str) else None,
        derived_key=suffix,
    )


def _item_id(entry: dict[str, Any], suffix: str = "") -> str:
    native = entry.get("id") if isinstance(entry.get("id"), str) else "entry"
    return f"pi:{native}{suffix}"


def build_timeline(
    session_id: str,
    external_id: str,
    entries: list[dict[str, Any]],
) -> list[RuntimeTimelineItem]:
    """把当前分支投影成平台时间线。

    工具调用与结果合并成一个 tool item：调用时是 running，结果回来后替换为
    done/failed 并带上 output。这样 iOS 只看到一个工具卡片。
    """

    items: list[Any] = []
    tool_index: dict[str, int] = {}

    for entry in current_branch(entries):
        if entry.get("type") != "message":
            continue
        message = entry.get("message")
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        if role == "system":
            continue
        if role == "user":
            text = _message_text(message.get("content"))
            if not text:
                continue
            items.append(
                MessageTimelineItem(
                    id=_item_id(entry),
                    type="message",
                    status="done",
                    role="user",
                    content=TextMessageContent(text=_truncate(text)),
                    source=_source(external_id, entry),
                )
            )
            continue
        if role == "toolResult":
            tool_call_id = message.get("toolCallId")
            tool_name = message.get("toolName")
            output = _truncate(_message_text(message.get("content")))
            index = tool_index.get(tool_call_id) if isinstance(tool_call_id, str) else None
            if index is not None:
                previous = items[index]
                call_content = getattr(previous, "content", None)
                items[index] = ToolTimelineItem(
                    id=previous.id,
                    type="tool",
                    status="failed" if message.get("isError") is True else "done",
                    role="tool",
                    content=ToolResultContent(
                        kind="tool_result",
                        title=call_content.title if isinstance(call_content, ToolCallContent) else (
                            tool_name if isinstance(tool_name, str) else None
                        ),
                        input=call_content.input if isinstance(call_content, ToolCallContent) else None,
                        output=output,
                    ),
                    source=previous.source,
                    turn_id=previous.turn_id,
                )
            else:
                items.append(
                    ToolTimelineItem(
                        id=_item_id(entry, ":result"),
                        type="tool",
                        status="failed" if message.get("isError") is True else "done",
                        role="tool",
                        content=ToolResultContent(
                            kind="tool_result",
                            title=tool_name if isinstance(tool_name, str) else None,
                            output=output,
                        ),
                        source=_source(external_id, entry, "result"),
                    )
                )
            continue
        if role != "assistant":
            continue

        content = message.get("content")
        text = _message_text(content)
        thinking = ""
        tool_calls: list[dict[str, Any]] = []
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "thinking" and isinstance(block.get("thinking"), str):
                    thinking += block["thinking"]
                elif block.get("type") == "toolCall":
                    tool_calls.append(block)
        turn_id = entry.get("id") if isinstance(entry.get("id"), str) else None
        stop_reason = message.get("stopReason")
        status = "cancelled" if stop_reason == "aborted" else (
            "failed" if stop_reason == "error" else "done"
        )
        if thinking.strip():
            items.append(
                SystemTimelineItem(
                    id=_item_id(entry, ":thinking"),
                    type="system",
                    status="done",
                    role="assistant",
                    content=ReasoningSystemContent(kind="reasoning", text=_truncate(thinking.strip())),
                    source=_source(external_id, entry, "thinking"),
                    turn_id=turn_id,
                )
            )
        if text.strip():
            items.append(
                MessageTimelineItem(
                    id=_item_id(entry),
                    type="message",
                    status=status,
                    role="assistant",
                    content=TextMessageContent(text=_truncate(text.strip())),
                    source=_source(external_id, entry),
                    turn_id=turn_id,
                )
            )
        for index, call in enumerate(tool_calls):
            call_id = call.get("id")
            name = call.get("name")
            arguments = call.get("arguments")
            try:
                serialized = json.dumps(arguments, ensure_ascii=False, indent=2, default=str)
            except (TypeError, ValueError):
                serialized = str(arguments)
            item = ToolTimelineItem(
                id=_item_id(entry, f":tool:{index}"),
                type="tool",
                status="running" if stop_reason == "pending" else "done",
                role="assistant",
                content=ToolCallContent(
                    kind="tool_call",
                    title=name if isinstance(name, str) else None,
                    input=_truncate(serialized, MAX_TOOL_INPUT),
                ),
                source=_source(external_id, entry, f"tool:{index}"),
                turn_id=turn_id,
            )
            items.append(item)
            if isinstance(call_id, str):
                tool_index[call_id] = len(items) - 1
        if not text.strip() and not thinking.strip() and not tool_calls:
            # 纯系统/空消息：不产出一个空卡片。
            continue

    projected = [item.to_platform_item(session_id, order) for order, item in enumerate(items)]
    return [item for item in projected if item is not None]


def first_user_text(entries: Iterable[dict[str, Any]]) -> str:
    for entry in entries:
        if entry.get("type") != "message":
            continue
        message = entry.get("message")
        if isinstance(message, dict) and message.get("role") == "user":
            text = _message_text(message.get("content"))
            if text:
                return text
    return ""
