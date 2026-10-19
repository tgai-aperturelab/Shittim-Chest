# ShittimChest · 什亭之匣

面向 QQ 的事务处理与多人协作 Agent，由光圈科技实验室共同开发。支持连续会话、工具执行、文件处理与交付、询问其他成员、运行时技能和 MCP。采用 Python 3.12；当前版本为 `0.1.0a6`，仍处于快速迭代阶段。

工程把 AstrBot 的部分 QQ 协议行为、Hermes 的部分执行与工具算法接入同一应用，保留来源与修改记录。模型循环、任务范围、工具效果、材料和交付在应用内协作；完整的 AstrBot/Hermes 应用并未随项目嵌入。`shittim_core` 是开发包名，不是 QQ 自称。

| 想做什么 | 从这里开始 |
| --- | --- |
| 第一次参与，安装并提交修改 | [贡献指南](CONTRIBUTING.md) |
| 找一个可以认领的任务 | [成员任务入口](docs/member-tasks.md) |
| 理解模块和数据流 | [架构导览](docs/architecture.md) |
| 了解审阅、权限与交接 | [协作约定](GOVERNANCE.md) |
| 报告敏感问题 | [安全报告说明](SECURITY.md) |

**开发和离线测试不需要 QQ 账号、模型 API Key 或生产服务器权限。** 默认测试使用临时目录、合成身份和本机替身服务。公开仓库包含当前 Python 实现、测试、通用工具及许可证；真实账号配置、会话数据、部署脚本和原始运行报告留在运维环境。

本项目按 [AGPL-3.0-or-later](LICENSE) 提供源码；Hermes 等第三方部分保留各自声明，见 [NOTICE.md](NOTICE.md)。

## 环境准备

在本仓库根目录建立 Python 3.12 环境，安装锁定依赖。旧开发工作区中对应目录为 `fusion/`。开发验证使用 `requirements-dev.lock`，运行环境使用 `requirements-runtime.lock`；两个文件固定版本，但不宣称已经生成所有平台的 wheel 哈希锁。

Windows PowerShell：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --only-binary=:all: -r requirements-dev.lock setuptools==84.0.0
.\.venv\Scripts\python.exe -m pip install --no-build-isolation --no-deps -e .
.\.venv\Scripts\python.exe -m shittim_core --help
```

Linux：

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install --only-binary=:all: -r requirements-dev.lock setuptools==84.0.0
.venv/bin/python -m pip install --no-build-isolation --no-deps -e .
.venv/bin/python -m shittim_core --help
```

需要的系统依赖包括执行 shell、`rg`，Linux 沙箱还需可运行的 `bwrap`。Browser use 已按用户要求移除；网页搜索与HTTP正文提取保留。

## 选择配置并检查

- [config.sandbox.example.json](config.sandbox.example.json)：推荐的 Linux 起点。包含当前全部 24 个内置工具，`execution_backend=bubblewrap`、`execution_network=isolated`。
- [config.execution.example.json](config.execution.example.json)：同样包含全部内置工具，显式使用 `local_host`。适合已授权的本地开发验证；终端具有服务账号的实际宿主权限，**不是沙箱**，网络为 host。
- [config.example.json](config.example.json)：保留的空授权模板，适合逐项配置。它不代表当前实现只有六个工具。

复制选定文件到 `var/config.local.json` 后填写真实的机器人账号、获准成员和群、模型路由与路径。`allowed_users`、`allowed_groups` 默认空，不会自动接管普通 QQ 消息。群输入还须满足成员和群的共同范围。示例启用工具的配置意图不等于所需外部环境已经就绪。

`data_dir`、技能根目录和 Python runtime 的相对路径均相对**配置文件所在目录**解析；复制配置后必须重新核对。模型样例路由须在本次服务上验证；`vision_model` 的占位值必须替换为确认支持图片的模型，不能据普通文字模型可用推断视觉可用。

```powershell
.\.venv\Scripts\python.exe -m shittim_core --check --config var/config.local.json
```

Linux 改用 `.venv/bin/python`。`--check` 不创建应用数据库、不启动监听器或模型客户端，也不发起 MCP discovery；报告分别列出 implemented、registered、configured_grant、静态 ready 和 `live_readiness=not_checked`。配置检查不能证明 模型能调用、MCP 能连接或 QQ 已收发。

配置只填写 `token_env`、`api_key_env` 等环境变量**名称**，不保存密钥值。应用不自动加载 `.env`；由本次进程的秘密配置方式注入变量，不复用根目录 Pi 的配置。只有准备好隔离账号、端口及授权范围之后才启动：

```sh
.venv/bin/python -m shittim_core --serve --config var/config.local.json
```

应用接收 NapCat 反向通用 OneBot WebSocket `/onebot`，鉴权 Token、`X-Self-ID`、事件账号须与配置 `bot_id` 一致。真实联调使用独立的测试账号、端口和数据目录。`listening` 只证明监听成功；关闭报告 `unsettled` 时，在途工作尚未全部结算，不应启动争用同一数据目录的第二实例。

## 当前工具与完整使用链路

| 能力 | 内置工具 | 使用条件与结果 |
| --- | --- | --- |
| 真实执行和进程 | `terminal`、`process_manage` | 明确执行后端；后台进程可查询、写入、等待和停止，结果进入效果记录 |
| 工作文件 | `read_file`、`write_file`、`patch`、`search_files` | 当前任务工作区、真实字节与内容 hash；搜索依赖 `rg` |
| 材料与交付 | `read_material`、`create_material`、`send_material`、`import_material`、`export_file` | 已登记材料与当前上下文范围；工作文件先导出登记，再经 outbox 交付 |
| 人员协作 | `ask_person`、`wait_for_input`、`manage_inquiry` | 明确、核验的收件人及问题关联；等待后能接续，迟到结果保留真实归属 |
| 运行时 Skills | `skills_list`、`skill_view`、`skill_manage` | 显式库和任务技能；读取完整指令、引用，导入完整包后再调用执行工具 |
| 网页研究 | `web_search`、`web_extract` | 真实搜索与正文提取；网络授权、超时、逐跳与下载上限 |
| 图片理解 | `vision_analyze` | 读取当前范围内已登记的图片，由配置的视觉模型分析；模型实际兼容性另测 |
| QQ 合并转发 | `read_forward`、`send_forward` | 读取收到的转发容器；发送只能使用已核验的真实消息来源，转发作者标签不成为权限 |
| 范围内历史 | `history_search`、`history_read` | 只检索当前上下文原始输入及已确认发送的可见答复；压缩后原文仍可找回，不读取系统提示、推理或工具原始日志 |
| MCP | 启动时按服务器与远端名称生成的工具 | 已配置服务器、实际发现、精确工具许可、环境引用和效果范围；不是固定内置名称 |

`read_material` 除文本外支持 PDF、DOCX、XLSX 的受限正文提取；扫描件、嵌入对象、复杂版式等不能据扩展名推定已识别。图片交给 `vision_analyze`；其他文件可导入工作区，用已准备好的执行环境和库处理。任意格式的专业生成或渲染质量仍须用实际成果验证。

完整配置样例选用已提供的 `bing_rss` 搜索后端。可按部署环境改用支持的搜索提供方；地区、语言和高级查询操作符的实际语义由提供方决定，不能仅因参数被接受就声称过滤已经生效。搜索结果要打开来源核对，提供方不可用应如实报告。

接收文件可以启用 `platform_public_files=true`，通过真实 NapCat 文件元数据取得公共网络材料，并保留来源、范围、大小限制；样例已经开启。它不授权模型随意提供 URL，也不允许私网地址。`allowed_download_hosts` 仍支持明确核验的旧式主机白名单；不要为了让下载通过而将模型生成的主机加入白名单。

两个完整能力样例开启 `progress_enabled=true`，以 `progress_min_interval_seconds=15` 限频发送固定阶段提示；Settings 默认关闭，可按会话体验决定是否启用。提示经同一交付通道发送，不包含工具参数或模型推理，也不代表最终成果；停止或旧控制代次不能继续发送。

QQ 正文使用纯文本；成果文件保留自己的正确格式。文件生成、登记、发送回执及对端内容核验分别记录，不能将“写出了文件”描述为“成员已收到”。unknown 不自动重做、重发，也不能靠新 call_id 或新任务名绕过未决效果。

## Linux 执行隔离与任务技能

沙箱配置需要独立、完整的 Python 安装前缀（含 `bin/python` 或 `bin/python3` 和 `lib/`），填入 `execution_python_runtime`。它不能与任务工作区目录重叠；不要假设一个依赖宿主路径的虚拟环境在重新挂载后仍然可运行。应以实际 Linux 验证证明解释器、标准库、所需依赖和脚本均可运行。

bubblewrap 将当前任务映射到 `/workspace`，Python runtime 只读映射到 `/runtime`；不挂入宿主 home、应用 state、其他任务工作区或文件系统 socket。默认隔离 PID、文件系统和网络，缺失 bwrap 或内核禁止所需 namespace 时会报错，**不会自动降级到 local_host**。它未实现 CPU、内存和磁盘配额，也不能消除内核漏洞。显式改成 `execution_network=host` 会开放宿主网络，包括内网，应按实际授权决定。

网页、模型和 MCP 客户端由应用单独管理；终端的 `execution_network=isolated` 不等于这些应用工具断网。

运行时 Skills 与开发 Codex 的全局技能完全分离。默认不扫描用户 home。需要共享只读技能库时，例如：

```json
"runtime_skill_roots": [
  {"id": "team", "path": "./runtime-skills", "source": "operator-reviewed-library"}
]
```

`skill_view` 返回完整可分页指令及相对引用。沙箱看不到只读库的宿主路径，应使用 `skill_manage` 的 `import_library`（别名 `copy`），指定 `source` 精确 ID 和新的任务目标 `name`。完整包复制到任务 `.runtime-skills/`，返回 `/workspace/...` 及工作区相对路径；源文件 hash、来源和导入记录保留。默认最多 512 文件、64 MiB 总量，单文件仍受配置上限约束；链接、越界、超限及已有目标拒绝。复制不执行脚本、不安装依赖，partial import 保留目录和 unknown 记录等待核对。

## MCP 配置中的环境引用

样例默认 `mcp_servers=[]`，不会向假服务器连接。按所选服务器增加配置；只授予准备使用的远端工具名，不把模型生成的 endpoint、command 或 env 当成授权。下面值都为占位示例：

```json
"mcp_servers": [
  {
    "name": "team-data",
    "transport": "stdio",
    "command": "/opt/team-mcp/bin/python",
    "args": ["-m", "team_mcp_server"],
    "cwd": "/opt/team-mcp",
    "env": {"TEAM_API_KEY": "FUSION_TEAM_API_KEY"},
    "allowed_tools": ["lookup_record"],
    "timeout_seconds": 30
  },
  {
    "name": "team-remote",
    "transport": "streamable_http",
    "url": "https://mcp.example.invalid/mcp",
    "headers": {"Authorization": "FUSION_MCP_AUTHORIZATION"},
    "allowed_tools": ["lookup_document"],
    "timeout_seconds": 30
  }
]
```

`env` 和 `headers` 的值都是环境变量名称；`FUSION_MCP_AUTHORIZATION` 的实际值应包含所需的完整鉴权头，如 Bearer 前缀，由秘密配置注入。`cwd` 必须真实存在。stdio 启动的进程、远端 MCP 自身权限和网络行为不自动进入终端 bubblewrap 沙箱；须按服务器本身的用途准备和限制环境。连接/发现失败、缺凭据或拒绝的工具需分别报告，不等于已授予可用能力。

## NapCat 文件共享与 PrivateTmp

`send_material` 要求 `napcat_shared_local_files=true`，意味着候选与 NapCat 能以**同一绝对路径**读取已登记材料。两个完整能力样例均声明该前提，启动前必须核验；不同机器或不共享挂载时不能仅靠这个布尔值解决文件交付。

若 NapCat systemd 服务使用 `PrivateTmp=true`，候选写在 `/tmp` 或 `/var/tmp` 的成果可能对 NapCat 不可见。使用专用共享位置，例如 `/var/lib/fusion-candidate/`，保持两个服务的私有临时目录策略。只通过专用读取组让 NapCat 穿越共享父目录并读取 `materials/`：

- 父目录允许该组执行/穿越，通常无需列目录权限；材料目录可用 setgid 保持文件组归属，并让该组只读材料文件。
- 候选服务拥有写权限。通过专用组、精确 ACL 或服务的受控文件权限确保新材料确实组可读；核验实际新文件，不只检查目录 mode。
- `state.sqlite3`、WAL/SHM、实例锁、进程输出和 `workspaces/` 保持候选私有。**不要递归开放整个 data_dir 或把整个运行状态设为共享组可读。**
- 用 NapCat 的实际服务身份验证可读取一个已登记测试材料，并确认无法读取 state 和 workspaces；再核对真实上传回执及对端内容 hash。

单独文件传递成功不能替代这套权限与命名空间核验。具体部署/权限修改属于受控切换步骤，本 README 不意味着已对现有服务执行。

## 验证与真实账号测试

```sh
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check src tests tools
.venv/bin/python -m ruff format --check src tests tools
.venv/bin/python -m pyright --pythonpath .venv/bin/python
.venv/bin/python tools/run_validation.py --output evidence/validation
```

Windows 用对应 `.venv\Scripts\python.exe`。验证脚本新建独立记录目录，保留首次失败、退出码、环境、源码 hash 与检查结果。缺失Linux/bwrap或真实账号导致的skip/not_run不是通过。离线 pytest 默认排除真实模型与 QQ 标记。

| 证据 | 能说明什么 |
| --- | --- |
| 配置诊断 | 静态字段、许可与部分依赖存在性；没有实际调用 |
| 离线/本机集成 | 替身协议、真实临时 SQLite、真实本地文件/进程等各自测试范围 |
| 真实模型 | 已报告模型路由上的实际工具决策和结果处理 |
| 真实 QQ | 被测账号、传输、收发回执及实际对端材料；报告须标明模型和引擎 |
| 生产切换 | 独立的备份、单消费者交接、运行验证和回退；候选测试不能代替 |

[双账户测试驱动](tests/live_qq/CANDIDATE-PROBE.md) 与 [测试成员驱动](tests/live_qq/README.md) 提供显式启用的真实验证路径。运行前必须核验当前 QQ 登录身份、账号在线状态和测试范围；扫码登录状态会失效，不能依赖过去“已登录”的结论。

测试输入用随机 nonce 和指定发送者限定，并过滤自发消息。若账号仍连接其他消费者，其他消费者也可能响应测试消息。群聊还须考虑 @ 与引用触发。两账号不证明两个不同成员共同参与同一任务；额外成员组合需要相应的真实身份验证。

真实批次报告可能含账号和会话关联信息，默认保存于被 Git 忽略的 `evidence/`。提交问题时只提供脱敏复现材料。仓库的自动检查只代表该次离线验证范围，不代表真实模型、QQ 或生产部署已经验收。

## 来源与许可证

项目按 AGPL-3.0-or-later 标识，见 [LICENSE](LICENSE)。Hermes 移植部分保留 MIT 声明，固定来源、hash、实际改写与限制见 [NOTICE.md](NOTICE.md) 和 `third_party/` manifests。开发 skills 不注入 QQ 运行提示词；运行时技能只有显式配置或任务内创建/导入的来源。
