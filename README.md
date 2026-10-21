# Agents Anywhere connector + Pi runtime

这是 [Agents Anywhere](https://github.com/anywhere-labs/Agents-Anywhere) 的
`connector` 子目录的 fork，额外加入了 **Pi Coding Agent runtime**，让 PiAbyss 能
通过 Agents Anywhere 在手机上看会话、发消息、切模型。

上游仓库本身没有 `pi` runtime，所以用这个仓库作为 PiAbyss「远程控制」的
Connector 安装源。

## 安装

PiAbyss → 设置 → 远程控制 → 高级设置 → Connector 安装源：

```
git+https://github.com/Nick12138/agents-anywhere-pi#subdirectory=connector
```

保存后点「重装」。装完 PiAbyss 的运行时卡片应显示「Pi runtime 已就绪」，
Agents Anywhere 刷新设备页会出现 **Pi Coding Agent**。

运行的机器还需要有 `pi` 命令（或在 AA 的 Pi runtime 配置里填 `piPath` 绝对路径）。
Pi 的会话、模型与认证都在 `~/.pi/agent`。

## 维护方式

这个仓库是唯一维护的 connector 源码，日常改动直接在这里提交、推送；
新设备从同一个 `git+https` 地址安装，不需要再手工同步第二份副本。

上游更新时，把 `anything-labs/Agents-Anywhere` 的 `connector/` 变更合并进来即可
（本仓库没有保留上游 git 历史，所以是目录级同步，不是 git merge）。

如果自建 Agents Anywhere 服务端，还需要应用
[`patches/server-allow-pi-runtime.patch`](patches/server-allow-pi-runtime.patch)，
把 `pi` 加进服务端已知 runtime 类型；官方云端不需要。

## 和上游的差异

- 新增 `connector/connector/runtimes/pi/`：会话发现、JSONL 时间线投影、
  按会话懒启动 `pi --mode rpc`、模型目录、扩展交互（select/confirm/input/editor）。
- 模型标题为 `供应商 / 模型名`，并按 `~/.pi/agent/models.json` 的
  `piabyssEnabledProviders` / `piabyssProviderModels` 过滤（跟随 PiAbyss
  设置 → 模型服务）。
- 支持从 AA 新建会话：预建 Pi 会话文件后走同一条 RPC 通道发首条消息。
- 注册 `pi`：`runtimes/providers.py`、`server/capabilities.py` 白名单、
  协议 schema enum。
- 版本号带本地后缀（`2.1.0+piabyss.N`），避免 uv 复用旧 wheel。

`connector/` 与 `contracts/` 之外的上游代码不在本仓库内；本仓库只用于安装
Connector，不提供 AA 服务端/客户端。

## 测试

```bash
uv run --project connector --with pytest pytest -q \
  connector/tests/test_pi_runtime.py connector/tests/test_connector_architecture.py
```

## 许可

MIT。上游版权归 Agents Anywhere contributors；Pi runtime 部分由 PiAbyss
contributors 维护。见 [LICENSE](LICENSE)。
