"""`pi --mode rpc` 子进程：JSONL 帧、命令关联与事件分发。"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MAX_LINE_BYTES = 64 * 1024 * 1024


class PiRpcError(RuntimeError):
    pass


class PiRpcProcess:
    def __init__(
        self,
        *,
        command: str,
        args: list[str],
        cwd: Path,
        env: dict[str, str],
        on_event: Callable[[dict[str, Any]], None],
        on_exit: Callable[[], None] | None = None,
    ) -> None:
        self.command = command
        self.args = args
        self.cwd = cwd
        self.env = env
        self._on_event = on_event
        self._on_exit = on_exit
        self._process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._sequence = 0
        self._closed = False

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    async def start(self, timeout: float) -> None:
        if self._process is not None:
            return
        try:
            self._process = await asyncio.create_subprocess_exec(
                self.command,
                *self.args,
                cwd=str(self.cwd),
                env=self.env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=MAX_LINE_BYTES,
            )
        except OSError as exc:
            raise PiRpcError(f"无法启动 pi：{exc}") from exc
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        # 等一个 get_state，确认 RPC 真的可用（而不是进程刚起就退出）。
        await self.request("get_state", timeout=timeout)

    async def _read_stdout(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        stream = self._process.stdout
        try:
            while True:
                try:
                    line = await stream.readline()
                except (ValueError, asyncio.LimitOverrunError) as exc:
                    logger.warning("pi rpc oversized line: %s", exc)
                    continue
                if not line:
                    break
                text = line.decode("utf-8", "replace").strip()
                if not text:
                    continue
                try:
                    record = json.loads(text)
                except ValueError:
                    logger.warning("pi rpc non-json line: %.200s", text)
                    continue
                if not isinstance(record, dict):
                    continue
                if record.get("type") == "response":
                    request_id = record.get("id")
                    future = self._pending.pop(request_id, None) if isinstance(request_id, str) else None
                    if future is not None and not future.done():
                        future.set_result(record)
                    continue
                try:
                    self._on_event(record)
                except Exception:
                    logger.exception("pi rpc event handler failed")
        finally:
            self._fail_pending("pi rpc 进程已退出")
            process = self._process
            if process is not None:
                await process.wait()
            self._process = None
            if self._on_exit is not None:
                try:
                    self._on_exit()
                except Exception:
                    logger.exception("pi rpc exit handler failed")

    async def _read_stderr(self) -> None:
        assert self._process is not None and self._process.stderr is not None
        stream = self._process.stderr
        while True:
            line = await stream.readline()
            if not line:
                return
            text = line.decode("utf-8", "replace").rstrip()
            if text:
                logger.info("pi stderr: %s", text[:2000])

    async def request(
        self,
        command: str,
        timeout: float = 60.0,
        **fields: Any,
    ) -> dict[str, Any]:
        if not self.running:
            raise PiRpcError("pi rpc 进程未运行")
        self._sequence += 1
        request_id = f"aa-{self._sequence}"
        payload = {"id": request_id, "type": command, **fields}
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        try:
            await self._write(payload)
            record = await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            raise PiRpcError(f"pi rpc 命令超时：{command}") from exc
        finally:
            self._pending.pop(request_id, None)
        if record.get("success") is not True:
            error = record.get("error")
            raise PiRpcError(str(error) if error else f"pi rpc 命令失败：{command}")
        data = record.get("data")
        return data if isinstance(data, dict) else {}

    async def send(self, command: str, **fields: Any) -> None:
        if not self.running:
            raise PiRpcError("pi rpc 进程未运行")
        await self._write({"type": command, **fields})

    async def _write(self, payload: dict[str, Any]) -> None:
        assert self._process is not None and self._process.stdin is not None
        data = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        async with self._write_lock:
            self._process.stdin.write(data)
            await self._process.stdin.drain()

    def _fail_pending(self, message: str) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(PiRpcError(message))

    async def close(self, *, grace: float = 3.0) -> None:
        if self._closed:
            return
        self._closed = True
        process = self._process
        if process is None:
            return
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=grace)
            except TimeoutError:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
        if process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
            with contextlib.suppress(Exception):
                await process.stdin.wait_closed()
        # 先让读循环看到 EOF 并把 stdout/stderr 收干净，再取消剩余任务。
        tasks = [task for task in (self._reader_task, self._stderr_task) if task is not None]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=grace)
            for task in pending:
                task.cancel()
            for task in pending:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._process = None


def build_env(agent_dir: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env["PI_CODING_AGENT_DIR"] = str(agent_dir)
    env.setdefault("PYTHONUNBUFFERED", "1")
    if extra:
        env.update(extra)
    return env
