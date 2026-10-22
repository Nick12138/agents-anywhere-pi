from __future__ import annotations

import asyncio

import httpx

from connector.core.config import ConnectorConfig
from connector.core.control_config import config_from_params, config_to_payload
from connector.server.client import BackendRpcClient


def make_config(**overrides) -> ConnectorConfig:
    values = {
        "server_url": "https://web.agents-anywhere.com",
        "connector_id": "conn_1",
        "connector_token": "secret",
    }
    values.update(overrides)
    return ConnectorConfig(**values)  # type: ignore[arg-type]


def bare_client(config: ConnectorConfig) -> BackendRpcClient:
    """只为了调用与后端连接相关的小方法，不启动任何后台任务。"""
    client = object.__new__(BackendRpcClient)
    client.config = config  # type: ignore[attr-defined]
    return client


def test_backend_bypasses_system_proxy_by_default() -> None:
    """默认直连后端：本机挂着 Clash 之类的代理时，走代理会把后端 TLS 掐断。

    实测（同一台机器、同一个 auth 请求）：
      经代理 → ConnectError: [SSL: UNEXPECTED_EOF_WHILE_READING]
      直连   → 200
    症状就是 connector 无限重连、runtime 同步全部失败。
    """
    config = make_config()
    assert config.bypass_proxy_for_backend is True
    client = bare_client(config)
    assert client._backend_bypasses_proxy() is True

    http_client: httpx.AsyncClient = client._new_http_client(5)
    try:
        assert http_client.trust_env is False
    finally:
        asyncio.run(http_client.aclose())


def test_backend_uses_system_proxy_when_asked() -> None:
    """需要经公司代理出网的部署，把 bypassProxyForBackend 设为 false 即可恢复旧行为。"""
    config = make_config(bypass_proxy_for_backend=False)
    client = bare_client(config)
    assert client._backend_bypasses_proxy() is False

    http_client: httpx.AsyncClient = client._new_http_client(5)
    try:
        assert http_client.trust_env is True
    finally:
        asyncio.run(http_client.aclose())


def test_loopback_backend_never_uses_proxy() -> None:
    config = make_config(server_url="http://127.0.0.1:8000", bypass_proxy_for_backend=False)
    assert bare_client(config)._backend_bypasses_proxy() is True


def test_config_flag_round_trips_through_settings_and_control_api() -> None:
    mapping = {
        "serverUrl": "https://web.agents-anywhere.com",
        "connectorId": "conn_1",
        "connectorToken": "secret",
    }
    # 老配置里没有这个键，缺省就是「直连」
    assert ConnectorConfig.from_mapping(mapping).bypass_proxy_for_backend is True
    assert config_from_params(mapping).bypass_proxy_for_backend is True

    mapping["bypassProxyForBackend"] = False
    config = ConnectorConfig.from_mapping(mapping)
    assert config.bypass_proxy_for_backend is False
    assert config_from_params(mapping).bypass_proxy_for_backend is False
    assert config_to_payload(config)["bypassProxyForBackend"] is False
