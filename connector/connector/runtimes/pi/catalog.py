"""Pi 模型目录投影。

两个数据源：

- `pi --mode rpc get_available_models`：模型与 thinking 档位（provider 只有 id）；
- `<agentDir>/models.json`：PiAbyss「设置 → 模型服务」维护的供应商名、启用列表
  （`piabyssEnabledProviders`）与按供应商的模型白名单（`piabyssProviderModels`）。

AA 的模型目录是**实时 RPC 读取**（服务端不会缓存 ingest），所以这里过滤即可：
在 PiAbyss 里启停供应商/模型，下一次打开 AA 的模型选择器就会同步。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connector.runtime_protocol import RuntimeModelItem, RuntimeReasoningItem

ENABLED_PROVIDERS_KEY = "piabyssEnabledProviders"
PROVIDER_MODELS_KEY = "piabyssProviderModels"


@dataclass(frozen=True, slots=True)
class ModelConfig:
    provider_names: dict[str, str]
    # None = 配置里没有启用列表，不过滤（兼容旧配置）。
    enabled_providers: frozenset[str] | None
    provider_models: dict[str, tuple[str, ...]]


def read_model_config(agent_dir: Path) -> ModelConfig:
    try:
        raw = json.loads((agent_dir / "models.json").read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return ModelConfig({}, None, {})
    if not isinstance(raw, dict):
        return ModelConfig({}, None, {})

    providers = raw.get("providers")
    names: dict[str, str] = {}
    if isinstance(providers, dict):
        for provider_id, value in providers.items():
            if not isinstance(provider_id, str) or not isinstance(value, dict):
                continue
            name = value.get("name")
            if isinstance(name, str) and name.strip():
                names[provider_id] = name.strip()

    enabled_raw = raw.get(ENABLED_PROVIDERS_KEY)
    enabled: frozenset[str] | None = None
    if isinstance(enabled_raw, list):
        enabled = frozenset(item for item in enabled_raw if isinstance(item, str) and item)

    allowed: dict[str, tuple[str, ...]] = {}
    allowed_raw = raw.get(PROVIDER_MODELS_KEY)
    if isinstance(allowed_raw, dict):
        for provider_id, model_ids in allowed_raw.items():
            if not isinstance(provider_id, str) or not isinstance(model_ids, list):
                continue
            ids = tuple(item for item in model_ids if isinstance(item, str) and item)
            if ids:
                allowed[provider_id] = ids
    return ModelConfig(names, enabled, allowed)


def provider_names(agent_dir: Path) -> dict[str, str]:
    """兼容旧调用：只取 provider 展示名。"""

    return read_model_config(agent_dir).provider_names


def build_models(
    raw_models: list[Any],
    config: ModelConfig,
) -> tuple[RuntimeModelItem, ...]:
    models: list[RuntimeModelItem] = []
    for raw in raw_models:
        if not isinstance(raw, dict):
            continue
        provider = raw.get("provider")
        model_id = raw.get("id")
        if not isinstance(provider, str) or not provider or not isinstance(model_id, str) or not model_id:
            continue
        if config.enabled_providers is not None and provider not in config.enabled_providers:
            continue
        allowed = config.provider_models.get(provider)
        if allowed is not None and model_id not in allowed:
            continue
        provider_label = config.provider_names.get(provider) or provider
        model_name = raw.get("name") if isinstance(raw.get("name"), str) and raw.get("name") else model_id
        selection = f"model:{provider}/{model_id}"
        reasoning: list[RuntimeReasoningItem] = []
        level_map = raw.get("thinkingLevelMap")
        if isinstance(level_map, dict):
            for level, mapped in level_map.items():
                if not isinstance(level, str) or mapped is None:
                    continue
                reasoning.append(
                    RuntimeReasoningItem(
                        id=level,
                        title=level,
                        selection_id=f"{selection}#{level}",
                        metadata={
                            "provider": provider,
                            "providerName": provider_label,
                            "model": model_id,
                            "modelId": model_id,
                        },
                    )
                )
        models.append(
            RuntimeModelItem(
                id=f"{provider}/{model_id}",
                title=f"{provider_label} / {model_name}",
                selection_id=selection,
                description=f"{provider} · {model_id}",
                reasoning_items=tuple(reasoning),
                metadata={
                    "provider": provider,
                    "providerName": provider_label,
                    "model": model_id,
                    "modelId": model_id,
                    "reasoning": bool(raw.get("reasoning")),
                },
            )
        )
    models.sort(key=lambda item: ((item.metadata or {}).get("providerName", ""), item.title))
    return tuple(models)
