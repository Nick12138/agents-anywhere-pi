"""Pi runtime configuration schema and normalization."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from connector.runtime_protocol import RuntimeInvalidRequestError

PI_CONFIG_SCHEMA_REVISION = 1
DEFAULT_REQUEST_TIMEOUT_MS = 60_000
DEFAULT_STARTUP_TIMEOUT_MS = 30_000


def pi_config_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "piPath": {
                "type": "string",
                "minLength": 1,
                "maxLength": 4096,
                "title": "pi 可执行文件",
                "description": "默认从 PATH 找 pi；也可以填绝对路径。",
            },
            "agentDir": {
                "type": "string",
                "minLength": 1,
                "maxLength": 4096,
                "title": "Pi agent 目录",
                "description": "默认 ~/.pi/agent；认证、模型与会话都在这里。",
            },
            "sessionDir": {
                "type": "string",
                "minLength": 1,
                "maxLength": 4096,
                "title": "会话目录覆盖",
                "description": "留空使用 <agentDir>/sessions。",
            },
            "extensions": {
                "type": "boolean",
                "default": True,
                "title": "加载扩展与技能",
                "description": "关闭后使用 --no-extensions，避免与桌面 PiAbyss 的插件实例重复。",
            },
            "requestTimeoutMs": {
                "type": "integer",
                "minimum": 100,
                "maximum": 600_000,
                "default": DEFAULT_REQUEST_TIMEOUT_MS,
            },
            "startupTimeoutMs": {
                "type": "integer",
                "minimum": 100,
                "maximum": 600_000,
                "default": DEFAULT_STARTUP_TIMEOUT_MS,
            },
        },
        "additionalProperties": False,
    }


def default_config_values() -> dict[str, Any]:
    return {
        "extensions": True,
        "requestTimeoutMs": DEFAULT_REQUEST_TIMEOUT_MS,
        "startupTimeoutMs": DEFAULT_STARTUP_TIMEOUT_MS,
    }


def normalized_config_values(raw: dict[str, Any]) -> dict[str, Any]:
    values = {**default_config_values(), **raw}
    for key in ("piPath", "agentDir", "sessionDir"):
        value = values.get(key)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise RuntimeInvalidRequestError(f"{key} must be a non-empty string")
        values[key] = value.strip()
    if not isinstance(values["extensions"], bool):
        raise RuntimeInvalidRequestError("extensions must be a boolean")
    for key in ("requestTimeoutMs", "startupTimeoutMs"):
        value = values.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or not 100 <= value <= 600_000:
            raise RuntimeInvalidRequestError(f"{key} must be an integer between 100 and 600000")
    return values


def configured_agent_dir(values: dict[str, Any]) -> Path:
    configured = values.get("agentDir")
    if isinstance(configured, str) and configured:
        return Path(configured).expanduser()
    return Path.home() / ".pi" / "agent"
