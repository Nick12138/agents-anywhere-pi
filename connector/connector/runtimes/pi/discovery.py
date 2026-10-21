"""Locate and version-check the local `pi` CLI."""

from __future__ import annotations

import asyncio
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connector.runtimes.pi.provider_config import configured_agent_dir

PROBE_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class PiDiscovery:
    available: bool
    executable: str | None
    version: str | None
    reason: str | None


def resolve_executable(values: dict[str, Any]) -> str | None:
    configured = values.get("piPath")
    if isinstance(configured, str) and configured.strip():
        candidate = Path(configured).expanduser()
        if candidate.is_file():
            return str(candidate)
        found = shutil.which(configured)
        return found
    return shutil.which("pi")


async def discover(values: dict[str, Any]) -> PiDiscovery:
    executable = resolve_executable(values)
    if executable is None:
        return PiDiscovery(False, None, None, "未找到 pi 可执行文件（可安装 Pi CLI 或在配置里指定路径）")
    env = dict(os.environ)
    env["PI_CODING_AGENT_DIR"] = str(configured_agent_dir(values))
    try:
        process = await asyncio.create_subprocess_exec(
            executable,
            "--version",
            cwd=str(configured_agent_dir(values)),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        return PiDiscovery(False, executable, None, f"无法执行 pi：{exc}")
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=PROBE_TIMEOUT_SECONDS)
    except TimeoutError:
        process.kill()
        await process.wait()
        return PiDiscovery(False, executable, None, "pi --version 超时")
    if process.returncode != 0:
        detail = (stderr or stdout).decode("utf-8", "replace").strip()
        return PiDiscovery(False, executable, None, detail or "pi --version 返回非零状态")
    version = stdout.decode("utf-8", "replace").strip().splitlines()
    return PiDiscovery(True, executable, version[0] if version else None, None)
