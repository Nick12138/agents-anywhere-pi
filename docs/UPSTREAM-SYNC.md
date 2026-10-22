# 上游同步指南（Agents Anywhere → 本仓库）

> 给未来的自己／AI 代理看：**动手之前请完整读完第 5 节（更新流程）和第 6 节（陷阱）**。
> 本文记录的是 2026-10-22 实测的事实，上游更新后这些数字会过期，但结构和判断方法不变。

## 1. 这个仓库是什么

上游 [Agents Anywhere](https://github.com/anywhere-labs/Agents-Anywhere)（MIT 声明，但仓库根目录没有 LICENSE 文件）的 **connector 部分 + 自研 Pi runtime**。

- 只用来安装/运行本机 Connector，不提供 AA 服务端，也不含任何客户端代码
- PiAbyss 的「设置 → 远程控制」把 `connectorSource` 指向**本仓库的本地目录**，因此 PiAbyss 装的就是这里构建出来的 wheel

| 项 | 值（2026-10-22） |
|---|---|
| 本仓库 | `Nick12138/agents-anywhere-pi`（GitHub），本地 `C:\Users\liu\Documents\我的项目\agents-anywhere-pi` |
| 上游 | `https://github.com/anywhere-labs/Agents-Anywhere` |
| 本地分支 | `main`，24 个提交，最新 `480484f2` |
| 基线快照 | 上游 `0020cf7f`（2026-10-21T10:52:25Z，PR #390「fix(android): Markdown 引用块闪退」时的 main） |
| 版本号 | `connector/pyproject.toml` → `2.1.0+piabyss.17` |
| 拷贝范围 | `connector/`（近乎全量）+ `contracts/protocol` + `patches/` + `scripts/` |

## 2. 关键结论：它不是 fork，不能用 git merge

三条独立证据（2026-10-22 实测）：

| 检查 | 结果 |
|---|---|
| `git remote -v` | 只有 `origin`，**没有 `upstream`** |
| GitHub API | `isFork: false`，`parent: null` |
| `git merge-base <上游> main` | **空**（无共同祖先） |
| 本地根提交 `d6c8c046` | 上游返回 `No commit found` |

后果：**不要尝试 `git merge upstream/main` 或 `git rebase`**，历史无关联，会变成灾难。
只能按**目录级 diff + 3 路 apply** 同步（见第 5 节）。

另外 `contracts/` 只是**子集**：本地只有 `contracts/protocol`，上游还有 `contracts/local-machine`、`contracts/dsh-bridge`、`contracts/runtime-control`。
之所以砍掉后两个，是因为本仓库不用 AA Desktop、不用 DSH 插件——**如果哪天要用，得把它们补回来**。

## 3. 你的接缝有多宽（决定同步成本）

相对基线 `0020cf7f`，本仓库对上游原有文件的改动**只有 7 个文件、不到 60 行**：

| 上游文件 | 改动 | 为什么 |
|---|---|---|
| `connector/runtimes/providers.py` | +1 行 | 注册 `PiProvider()` |
| `connector/server/runtime_session_rpc.py` | 20 行 | Pi 会话 RPC 适配 |
| `connector/server/client.py` | 15 行 | 后端连接/重试 |
| `connector/core/config.py` | 10 行 | 新增 pi 配置项 |
| `connector/core/control_config.py` | 3 行 | 同上 |
| `connector/core/logging.py` | 30 行 | 把标准库 logging 接到 loguru |
| `connector/server/capabilities.py` | +1 行 | `pi` 进白名单 |
| `connector/pyproject.toml` | 1 行 | 版本号后缀 `+piabyss.N` |

其余 **5788 行全是新增**，主要在新增目录 `connector/connector/runtimes/pi/`：

| 文件 | 行数 |
|---|---|
| `runtimes/pi/runtime.py` | 1835 |
| `runtimes/pi/sessions.py` | 483 |
| `runtimes/pi/rpc.py` | 217 |
| `runtimes/pi/provider.py` | 179 |
| `runtimes/pi/catalog.py` | 131 |
| `runtimes/pi/provider_config.py` | 94 |
| `runtimes/pi/discovery.py` | 63 |
| `tests/test_pi_runtime.py` | 1341 |

**所以上游怎么改都改不到你的代码，只会通过接口间接影响你。** 这就是同步很便宜的根本原因。

## 4. 上游更新要不要跟：决策表

上游的改动分四类：

| 类 | 范围 | 要不要动手 |
|---|---|---|
| ① 客户端/服务端 | `web-next`、`android`、`desktop-next`、`server` | **完全不用**。跑在官方云和你手机上，自动生效 |
| ② Connector ↔ Server 协议 | `connector/connector/server/` 的协议层 | **基本不用**。上游承诺向后兼容：2.0.x connector 能连 2.1.0 Server，只是不上报 `phase`。官方云升级不会让你掉线 |
| ③ 本机控制通道 | MeshCall / DSH 插件 / AA Desktop | **与你无关**。2.1.0 那条「插件+Connector+Desktop 必须一起升」的硬切换，是因为本机控制通道换了 MeshCall 0.3；你由 PiAbyss 直接 `anywhere-cli start`（`kind: cli`），两条都不走 |
| ④ **Runtime 接口** | `connector/connector/runtime_protocol/`、`runtimes/providers.py` | **只有这类需要看**。你的 `PiProvider` 继承的就是这里的 `RuntimeProvider` / `RuntimeConfig` / `RuntimeTypeDescriptor` / `RuntimeHostClient` |

近 3 周（v2.0.3 之后）实测提交频率：

```
connector                       37 个提交
  └ connector/runtimes          21
  └ connector/runtime_protocol   7   ← 只有这个跟你有关
contracts                       14
web-next                        41   （无关）
android                         20   （无关）
```

`runtime_protocol` 那 7 个提交近期的形态都是**新增可选字段**（10-11 加 `phase`、10-13 附件通知容错与字符哈希规范化、10-20 附件通知传输失败隔离），**没有删接口**。

**判断规则（照这个做，5 分钟出结论）**：

- `runtime_protocol/` 只**新增**可选字段 → 不处理也能跑；想要新功能才跟进
- `runtime_protocol/` **删除/改签名**，或 `default_runtime_providers()` 变签名 → 必须跟进
- `contracts/` 版本目录跳号（如 `local-machine/2.0` → `3.0`）→ 属于 class ③，先确认自己是否真的在用
- Server 侧要新权限/新能力上报（`capabilities.py` 白名单）→ 必须跟，否则新功能在云端被拒

**只有两种情况必须动手**：想用上游新功能；上游给接口加了必填项。

## 5. 更新流程（SOP）

### 5.0 前置：第一次做时补齐 upstream remote

```bash
cd /c/Users/liu/Documents/我的项目/agents-anywhere-pi
git remote add upstream https://github.com/anywhere-labs/Agents-Anywhere.git   # 只需一次
git fetch upstream main
```

> 注意：`git fetch upstream` 只往 `.git/objects` 写对象，不动工作区、不建分支。

### 5.1 先看改了什么，别急着 apply

```bash
# 本仓库记录的基线（换新版后要把这个 sha 更新到第 8 节的记录表）
BASE=0020cf7f
git diff --stat $BASE upstream/main -- connector contracts
```

重点看两个路径（第 4 节规则）：

```bash
git diff $BASE upstream/main -- connector/connector/runtime_protocol connector/connector/runtimes/providers.py
```

如果这里**空**且你没想用新功能 → **停止，什么都不用做**。

### 5.2 在临时分支上做，不污染 main

```bash
git switch -c sync/upstream-<日期>
git diff $BASE upstream/main -- connector contracts | git apply -3
```

- 因为本仓库 `connector/` 的基线 blob 与上游 `0020cf7f` 一致，这个 diff **大部分能干净应用**
- 冲突几乎只会出现在第 3 节那 7 个接缝文件里，按「保留 Pi 注册 / 采纳上游逻辑」的原则手工合并
- **`contracts/` 只想保留 `protocol`**：如果 diff 带进了 `local-machine`、`dsh-bridge`、`runtime-control`，删掉（除非你决定开始用 AA Desktop / DSH 插件）

### 5.3 冲突高发点（按历史经验）

| 位置 | 注意 |
|---|---|
| `runtimes/providers.py` | 必须保留 `PiProvider()` 在元组里 |
| `server/capabilities.py` | 必须保留 `pi` 白名单 |
| `pyproject.toml` 的 `version` | 上游会写 `2.1.0`，你要改回 `2.1.0+piabyss.18`（N 递增，见 6.1） |
| `core/logging.py` | 上游若重构日志，你接 loguru 的 30 行要重新落位 |
| `server/client.py` 的 `trust_env`/`proxy` | 你有「后端连接默认绕过系统代理」的本地修改，别被上游覆盖 |

### 5.4 验证

```bash
# 1) 单元测试（必跑）
uv run --project connector --with pytest pytest -q \
  connector/tests/test_pi_runtime.py connector/tests/test_connector_architecture.py

# 2) 语法/编译冒烟
uv run --project connector python -c "import connector.runtimes.providers as p; print([x.runtime_type for x in p.default_runtime_providers()])"
# 期望输出里包含 pi
```

### 5.5 装回 PiAbyss 并真机确认

```bash
git switch main && git merge --ff-only sync/upstream-<日期>
git push origin main
```

然后在 PiAbyss「设置 → 远程控制」里**重装一次 Connector**（`connectorSource` 已指向本地目录，重装会用新代码构建 wheel），再确认：

- [ ] 设备状态是「运行中」，不是「启动中」
- [ ] 手机/网页端能看到 Pi 会话列表
- [ ] 发一条消息能收到流式回复（思考/工具卡片也出现）
- [ ] 「停止」有反应
- [ ] 附件（图片）能发出去
- [ ] 交互请求（select/confirm/input）能显示并回复

### 5.6 回滚

```bash
git switch main
git reset --hard <同步前的 sha>     # 本地
git push --force-with-lease origin main
```

再在 PiAbyss 里重装 Connector。**同步前先 `git tag pre-sync-<日期>` 打标签**，回滚更省事。

## 6. 陷阱清单

### 6.1 版本号必须递增，否则 uv 复用旧 wheel

`connector/pyproject.toml` 的 `version` 是 `2.1.0+piabyss.17`。**每次改动都要把 N+1**，否则 `uv tool install --from <本地目录>` 可能拿缓存里的旧构建，你会以为改动没生效。

### 6.2 上游「2.1.0」与你本地后缀

上游 2.1.0 正式发布后，前缀应跟着升（`2.1.0+piabyss.N` → `2.1.1+piabyss.N` 之类）。**前缀跟着上游的 MINOR/PATCH 走，后缀永远递增**。

### 6.3 别装 AA Desktop / DSH 插件

一旦装了，就会撞上 2.1.0 的硬切换规则（本机三件套版本必须完全一致），你这份带本地后缀的 connector 会被判定为「无法管理」。要用 AA 桌面端的话，先看第 8 节确认上游正式版本号。

### 6.4 服务端 patch 只对自建有效

`patches/server-allow-pi-runtime.patch` 是把 `pi` 加进服务端已知 runtime 类型。**官方云端不需要**，只在自建 AA 服务端时才要应用。

### 6.5 上游仓库没有 LICENSE 文件

README 徽章写 MIT，但 `gh api repos/anywhere-labs/Agents-Anywhere/license` 返回 404，全库只有 `_deprecated/` 下的旧 LICENSE。
本仓库根目录的 `LICENSE` 是**我们自己补的 MIT 声明**（版权行含 "PiAbyss contributors (pi runtime additions)"）。
风险评估：低（README 明示 MIT），但严格讲上游的授权是「未声明状态」。如果要对外分发，建议先在上游 issue 里问一句。

### 6.6 Windows 环境相关

- 上游有多处修 Windows 的中文用户名、含空格路径、保留端口段问题（#144/#164/#217/#380/#384）——**这些修复值得优先跟**，因为本机就是 `Nick-Office` + 中文项目路径
- 你的 `runtimes/pi/discovery.py` 在 Windows 上从 PATH 找 `pi` 可执行文件（历史提交 `115452cf`），上游若改进程启动方式要复核
- PiAbyss 传的 `connectorSource` 是**带中文和空格的本地路径**，`uv tool install --from` 对它敏感；同步后重装失败先怀疑路径

### 6.7 别用 `git pull`/`merge` 同步上游

见第 2 节。历史上没有共同祖先，任何 merge 都会把整棵上游树拉进来（3000+ 文件），把仓库变成完整 clone。

## 7. 已知的上游未发布状态（2026-10-22）

写这份文档时，上游处于一个「代码跑在前面」的窗口期，值得在下次同步时优先确认：

| 项 | 状态 |
|---|---|
| GitHub Release | 最新正式版仍 **v2.0.3（2026-09-30）** |
| PyPI `anywhere-cli` | 最新 **2.0.3**（2026-09-30 发布） |
| `main` 上的未发布内容 | **v2.0.3 之后 137 个提交 / 300 个文件** |
| `docs/releases/2.1.0.md` | 已写好（最后更新 2026-10-22 07:14），**未发版** |
| `docs/releases/2.1.1.md` | 已写好（2026-10-22 03:56），内容 = 当天合并的 PR #393 |
| 上游 main 最新 | `6fb6b165`（2026-10-22T07:59Z，PR #387） |

**2.1.0 的关键内容**（不是全部，只列与你相关的）：

- 手写 JSON-RPC → MeshCall 0.3（本机控制通道，**class ③，与你无关**）
- DSH 桥接契约 2.0（**class ③**）
- 管理统计 API `/admin/dashboard/*` → `/admin/usage/*`；DB 迁移 `v2_42`~`v2_44`，旧看板数据不保留（**服务端侧，与你无关**）
- runtime 状态新增可选 `phase`（**class ④，与你有关**：不实现也能跑，实现了「同步中」才能显示成进度而不是错误）
- 大历史快照分页上传、单会话过大只隔离该会话（**class ②，connector 内自动受益**）
- Windows 保留端口 / 中文用户名 / 空格路径的一批修复（**建议优先跟**，见 6.6）

下次同步时：先 `git diff $BASE upstream/main --stat`，再看 2.1.0 是否已正式发布；已发布的话把 `docs/releases/2.1.0.md` 里「已知限制」和「各端最低配套版本」过一遍。

## 8. 同步记录表

**每次同步后更新这张表，并同步修改第 1 节的「基线快照」和第 5.1 节的 `BASE`。**

| 日期 | 上游基线 sha | 上游版本 | 本地版本 | 结果 |
|---|---|---|---|---|
| 2026-10-21 | `0020cf7f` | main（2.1.0 未发布） | `2.1.0+piabyss.17` | 初始快照 + 24 个 Pi runtime 提交 |
| （待同步） | `6fb6b165` | main | — | 落后 8 个提交，含 #334/#380/#383/#384/#387/#393/#394/#375 |

## 9. 速查命令

```bash
cd /c/Users/liu/Documents/我的项目/agents-anywhere-pi

# 落后多少个提交（上游 main 相对本地基线）
git fetch upstream main && git log --oneline 0020cf7f..upstream/main | wc -l

# 只看会不会打到我的接口
git diff 0020cf7f upstream/main -- \
  connector/connector/runtime_protocol \
  connector/connector/runtimes/providers.py \
  connector/connector/server/capabilities.py

# 全量看看（含 contracts）
git diff --stat 0020cf7f upstream/main -- connector contracts

# 我在上游文件上改了哪些行（同步时防止被覆盖）
git diff 0020cf7f main -- $(git diff --name-only 0020cf7f main -- connector | grep -v 'runtimes/pi/')

# 跑测试
uv run --project connector --with pytest pytest -q \
  connector/tests/test_pi_runtime.py connector/tests/test_connector_architecture.py
```

## 10. 一句话总结

**上游更新 95% 与你无关；真正需要跟的只有 `runtime_protocol/` 的接口变化，以及 Windows 相关的修复。**
同步是「目录级 diff + 3 路 apply」，不是 git merge；接缝只有 7 个文件、不到 60 行，所以这件事很便宜——**别等到攒了几百个提交才做**。
