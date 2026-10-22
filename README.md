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
Pi 的会话、模型与认证都在 `~/.pi/agent`。安装 Pi CLI：

```bash
npm i -g @earendil-works/pi-coding-agent@1.1.0   # 与 PiAbyss 内置版本一致
```

Windows 上 Connector 会自己把 `pi` 解析成 `%APPDATA%\npm\pi.cmd`，
所以装好即可，不必手工填 `piPath`。

## 多台设备 / 本地开发

每台机器都装一份 Connector（各自 `~/.agents-anywhere/connector.json` 配对），
会话文件在各自机器的 `~/.pi/agent/sessions`，互不影响。

要在某台机器上「改代码即生效」（不用每次重装），在仓库根目录执行：

```powershell
# 前提：先在 PiAbyss 里完成一次重装，并把 connector 停止
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup-local-connector.ps1
# 顺便把安装源也改成这个本地目录：
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup-local-connector.ps1 -UseLocalSource
```

脚本会把已安装的 `site-packages/connector` 换成指向本仓库的目录联接（junction），
之后改代码只需在 PiAbyss 里「停止 → 启动」。**不要再点「重装」**，否则联接会被
换回普通拷贝。macOS/Linux 等价做法：

```bash
ln -sfn "$PWD/connector/connector" ~/.pi/agent/piabyss/remote/tools/anywhere-cli/lib/python3.*/site-packages/connector
```

## 维护方式

这个仓库是唯一维护的 connector 源码，日常改动直接在这里提交、推送；
新设备从同一个 `git+https` 地址安装，不需要再手工同步第二份副本。

上游更新时，**先读 [`docs/UPSTREAM-SYNC.md`](docs/UPSTREAM-SYNC.md)**：里面有基线 sha、
更新流程、冲突高发点和陷阱清单。要点是本仓库没有保留上游 git 历史，
所以只能按 `connector/`、`contracts/` 做目录级 diff + 3 路 apply，**不能用 git merge**。

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
- 附件：图片（≤8MB、最多 8 张）转 base64 走 rpc 的 `images`，pi 会写进会话
  JSONL，模型直接看得到；其他文件落到 connector 数据目录，并把本机路径写进
  提示词让 pi 自己 `read`。时间线里会带上附件元信息（文件名/类型/大小）。
- 流式输出：`pi --mode rpc` 的 `message_update` 会把 text / thinking / toolCall
  都投影成 `status=running` 的临时卡片（文本、思考、工具各一张），text 会投影成一张
  `status=running` 的临时气泡，按 120ms 节流 `timeline.itemUpsert` 推给服务端；
  回合结束的完整快照会接管并把临时气泡替换成正式条目。
- 失败可见：`stopReason=error` 的回合会投影成一条 error 气泡（以前空正文被整个
  跳过，手机端什么都看不到）；pi 自动重试期间也会显示「Connection error（自动重试 1/3）」。
- 时间线增量：`list_sessions` 上报 `sync.requires_timeline_sync`（对比文件
  mtime+size 指纹），回合开始/结束时立刻推一次 `timeline.sync`，手机上不用等
  30 秒轮询、也不会漏掉新回复。会话目录每 3 秒重扫一次，桌面端新建的会话能
  自己冒出来。
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
