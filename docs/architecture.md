# 架构导览

ShittimChest 是单个 Python 应用。它接收 NapCat 的 OneBot 消息，在持久化任务和权限范围内运行模型及工具，再通过交付记录返回结果。

```text
NapCat / OneBot WebSocket
          ↓
qq：鉴权、消息转换、请求与回执
          ↓
app：输入接纳、引用与附件、服务生命周期
          ↓
collaboration：会话定位、协作、停止与继续
          ↓
agent + context：模型循环、工具决策、上下文整理
          ↓
tools + engine + materials：执行、文件、网页、技能、MCP
          ↓
delivery：交付意图、发送、回执

state：贯穿输入、任务、效果与交付的 SQLite 状态
```

## 代码与测试位置

路径均相对仓库根目录。

| 模块 | 核心责任 | 主要测试 |
| --- | --- | --- |
| `src/shittim_core/domain/` | 地址、输入、授权、任务和结果类型 | `tests/properties/`、各模块契约 |
| `src/shittim_core/qq/` | OneBot 编解码、连接、请求关联 | `tests/qq/` |
| `src/shittim_core/app/` | 配置、输入管线、运行端口、应用组装 | `tests/integration/test_app*.py`、`test_runtime.py` |
| `src/shittim_core/collaboration/` | 上下文定位、询问、控制与继续 | `tests/integration/test_inquiry_controls.py`、`test_conversation_focus.py` |
| `src/shittim_core/agent/` | 模型调用、工具循环、预算和结果转换 | `tests/agent/` |
| `src/shittim_core/context/` | 上下文构造、压缩和历史边界 | `tests/context/` |
| `src/shittim_core/state/` | 持久化、并发控制、恢复与效果记录 | `tests/state/`、`tests/recovery/` |
| `src/shittim_core/tools/` | 工具注册、参数、授权和应用适配 | `tests/integration/` 中对应工具测试 |
| `src/shittim_core/engine/` | 本地执行、进程树、文件操作和隔离 | `test_execution_tools.py`、`test_bubblewrap.py`、`test_process_tree.py` |
| `src/shittim_core/materials/` | 下载、登记、文件名和受限文档提取 | `tests/materials/` |
| `src/shittim_core/delivery/` | 发送状态、回执、未知结果处理 | `test_delivery_batches.py`、`test_material_pipeline.py` |

启动入口为 `src/shittim_core/__main__.py`。配置从 `app/config.py` 进入，应用组装在 `app/service.py`。研究移植算法时查看 `engine/hermes/` 的来源注释和 `third_party/` 对应清单。

## 修改时需要保持的行为

发言人、来源会话和引用材料不能混为一体。工具输出和引用内容不产生授权。发送或执行结果不明时保留 unknown 状态，不通过换一个调用 ID 重做可能已发生的操作。停止控制应影响后续执行和发送，但不能假装撤回已发生的效果。

状态约束见 [状态契约](../src/shittim_core/state/contract.md)。行为变更应通过对应集成测试解释用户可观察到的结果。

## 边界

默认测试不证明真实 QQ、模型路由或 Linux 隔离能力可用。`local_host` 执行具有服务账号的宿主权限；`bubblewrap` 是显式选择的 Linux 后端，环境不支持时不会自动降级。浏览器自动操作已移除，网页搜索与 HTTP 正文提取仍保留。
