"""Pi Coding Agent runtime provider (local `pi --mode rpc` + session files)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from jsonschema import Draft202012Validator

from connector.runtime_protocol import (
    AgentRuntime,
    RuntimeConfig,
    RuntimeConfigSchema,
    RuntimeHostClient,
    RuntimeInvalidRequestError,
    RuntimeProvider,
    RuntimeResourceClaim,
    RuntimeSourceKey,
    RuntimeTypeDescriptor,
)
from connector.runtime_protocol.filesystem import filesystem_resource_key
from connector.runtimes.pi import discovery, provider_config
from connector.runtimes.pi.runtime import PiRuntime

PI_CAPABILITIES: dict[str, bool] = {
    "modelCatalog": True,
    "permissionCatalog": False,
    "sessionDiscovery": True,
    "sessionSnapshot": True,
    "sessionState": True,
    "sessionNotices": True,
    "createAndStartSession": True,
    "startTurn": True,
    "steerTurn": True,
    "interruptTurn": True,
    "commands": False,
    "interactions": True,
    "attachments": False,
    "ipc": True,
}


class PiProvider(RuntimeProvider):
    def __init__(self) -> None:
        self._last_values = provider_config.default_config_values()
        self._last_discovery: discovery.PiDiscovery | None = None

    @property
    def runtime_type(self) -> str:
        return "pi"

    @property
    def implementation_type(self) -> str:
        return "local-process"

    @property
    def display_name(self) -> str:
        return "Pi Coding Agent"

    @property
    def description(self) -> str:
        return "本地 Pi CLI（pi --mode rpc）与原生会话文件"

    @property
    def instance_policy(self) -> str:
        return "multiple"

    @property
    def max_instances(self) -> int | None:
        return None

    async def discover(self) -> RuntimeTypeDescriptor:
        result = await discovery.discover(self._last_values)
        self._last_discovery = result
        return RuntimeTypeDescriptor(
            runtime_type=self.runtime_type,
            display_name=self.display_name,
            description=self.description,
            implementation_type=self.implementation_type,
            available=result.available,
            capabilities=dict(PI_CAPABILITIES),
            reason=result.reason,
            config_schema=self._config_schema(),
            instance_policy=self.instance_policy,
            max_instances=self.max_instances,
            recommended=False,
            metadata={
                "configured": result.available,
                "storageMode": "pi-native",
                "sameSessionWriterLimit": 1,
                "crossProcessWriterExclusion": False,
                "sessionFormatVersion": 3,
                "executable": result.executable,
                "version": result.version,
            },
        )

    def _config_schema(self) -> RuntimeConfigSchema:
        return RuntimeConfigSchema(
            runtime=self.runtime_type,
            revision=provider_config.PI_CONFIG_SCHEMA_REVISION,
            schema=provider_config.pi_config_schema(),
            ui_schema={
                "order": [
                    "piPath",
                    "agentDir",
                    "sessionDir",
                    "extensions",
                    "startupTimeoutMs",
                    "requestTimeoutMs",
                ],
                "piPath": {"component": "path"},
                "agentDir": {"component": "path"},
                "sessionDir": {"component": "path"},
            },
            defaults=provider_config.default_config_values(),
            metadata={
                "storageMode": "pi-native",
                "sameSessionWriterLimit": 1,
                "crossProcessWriterExclusion": False,
            },
        )

    async def get_config_schema(self) -> RuntimeConfigSchema:
        return self._config_schema()

    async def validate_config(self, values: Mapping[str, Any]) -> RuntimeConfig:
        raw = dict(values)
        schema = provider_config.pi_config_schema()
        errors = sorted(
            Draft202012Validator(schema).iter_errors(raw),
            key=lambda error: list(error.absolute_path),
        )
        if errors:
            path = "/" + "/".join(str(part) for part in errors[0].absolute_path)
            raise RuntimeInvalidRequestError(
                f"pi config is invalid at {path or '/'}: {errors[0].message}"
            )
        normalized = provider_config.normalized_config_values(raw)
        self._last_values = normalized
        return RuntimeConfig(
            runtime=self.runtime_type,
            revision=provider_config.PI_CONFIG_SCHEMA_REVISION,
            values=normalized,
            schema=schema,
            ui_schema=self._config_schema().ui_schema,
            metadata={
                "storageMode": "pi-native",
                "sameSessionWriterLimit": 1,
                "crossProcessWriterExclusion": False,
            },
        )

    async def create_runtime(
        self,
        config: RuntimeConfig,
        host: RuntimeHostClient,
    ) -> AgentRuntime:
        version = self._last_discovery.version if self._last_discovery else None
        return PiRuntime(config=config, host=host, runtime_version=version)

    def resource_claims(self, config: RuntimeConfig) -> tuple[RuntimeResourceClaim, ...]:
        values = dict(config.values)
        root = provider_config.configured_agent_dir(values)
        return (
            RuntimeResourceClaim(
                kind="pi_agent_dir",
                key=filesystem_resource_key(str(root)),
                label=f"Pi agent directory {str(root)!r}",
            ),
        )

    def session_source_key(self, config: RuntimeConfig) -> RuntimeSourceKey:
        values = dict(config.values)
        root = provider_config.configured_agent_dir(values)
        return RuntimeSourceKey(
            kind="pi_agent_dir",
            key=filesystem_resource_key(str(root)),
        )
