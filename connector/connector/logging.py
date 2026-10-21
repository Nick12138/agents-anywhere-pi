from __future__ import annotations

import asyncio
import inspect
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

# Loguru calls sinks from whichever thread logged, so a synchronous, thread-safe
# notifier is preferred; a coroutine notifier needs a running loop there.
RpcLogNotifier = Callable[[str, Any], Awaitable[None] | None]
DEFAULT_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def configure_connector_logging(*, debug: bool = False) -> None:
    level = "DEBUG" if debug else "INFO"
    logger.remove()
    logger.add(sys.stderr, level=level, format=DEFAULT_LOG_FORMAT)
    _route_stdlib_logging(level)


class _InterceptHandler(logging.Handler):
    """把标准库 logging 转到 loguru。

    runtime 模块用的是 `logging.getLogger(__name__)`，不接过来的话它的 INFO 日志
    会直接丢掉（父 logger 没有 handler），出问题时根本看不到运行时内部发生了什么。
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            sink_level = logger.level(record.levelname).name
        except ValueError:
            sink_level = record.levelno
        logger.opt(exception=record.exc_info).log(
            sink_level,
            f"[{record.name}] {record.getMessage()}",
        )


def _route_stdlib_logging(level: str) -> None:
    handler = _InterceptHandler()
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # 已知吵的库降到 WARNING，避免 INFO 日志被刷屏。
    for name in ("asyncio", "httpx", "httpcore", "websockets", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)


class RpcLogSink:
    def __init__(self, notifier: RpcLogNotifier) -> None:
        self.notifier = notifier
        self._tasks: set[asyncio.Task[None]] = set()
        self._sink_id: int | None = None

    def install(
        self,
        *,
        level: str = "INFO",
        remove_default_sink: bool = False,
    ) -> RpcLogSink:
        if remove_default_sink:
            logger.remove()
        self._sink_id = logger.add(self._write, level=level, format="{message}",
                                   filter=lambda record: record["extra"].get("meshcall_service") != "aa.local.v2.Logs"
                                   and not record["message"].startswith("RPC call aa.local.v2.Logs."))
        return self

    async def close(self) -> None:
        if self._sink_id is not None:
            logger.remove(self._sink_id)
            self._sink_id = None
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _write(self, message: Any) -> None:
        record = message.record
        payload = {
            "time": _clip(record["time"].isoformat(), 80),
            "level": _clip(record["level"].name, 20),
            "name": _clip(record["name"] or "", 200),
            "message": _clip(record["message"], 16000),
        }
        extra = {key.removeprefix("meshcall_"): value for key, value in record["extra"].items()
                 if key in {"meshcall_service", "meshcall_method", "meshcall_call_id"}}
        if extra:
            payload["details"] = _clip(json.dumps(extra, default=str), 16000)
        exception = record.get("exception")
        if exception is not None:
            payload["exception"] = _clip(exception, 16000)

        result = self.notifier("connector/log", payload)
        if not inspect.isawaitable(result):
            return
        task = asyncio.ensure_future(result)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


def install_rpc_log_sink(
    notifier: RpcLogNotifier,
    *,
    level: str = "INFO",
    remove_default_sink: bool = False,
) -> RpcLogSink:
    return RpcLogSink(notifier).install(
        level=level,
        remove_default_sink=remove_default_sink,
    )


def _clip(value: Any, limit: int) -> str:
    """Keep records inside the log service's field limits instead of dropping them."""
    text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"
