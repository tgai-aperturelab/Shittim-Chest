# 参与开发

欢迎社团成员和外部贡献者。可以从文档、复现问题、测试或代码开始；不要求先申请生产服务器权限。项目使用 Python 3.12，日常开发在 Windows 或 Linux 上均可进行。

## 第一次运行

1. Fork 本仓库，克隆自己的 Fork，在仓库根目录执行下面的安装步骤。
2. 创建工作分支，例如 `git switch -c docs/config-guide`。
3. 跑通离线检查，再从 [成员任务入口](docs/member-tasks.md) 选择一项工作。在对应 Issue 说明想做的范围，避免重复投入。

Windows PowerShell：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --only-binary=:all: -r requirements-dev.lock
.\.venv\Scripts\python.exe -m pip install --no-build-isolation --no-deps -e .
.\.venv\Scripts\python.exe -m shittim_core --help
.\.venv\Scripts\python.exe tools/run_validation.py
```

Linux：

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install --only-binary=:all: -r requirements-dev.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps -e .
.venv/bin/python -m shittim_core --help
.venv/bin/python tools/run_validation.py
```

安装需要访问 Python 包源。`pyright` 还需要 Node.js；建议安装 Node.js 24 LTS 并放入 PATH，避免首次检查时再自动下载 Node。终端工具使用的 shell 和 `rg` 按操作系统另行安装。默认离线检查不会连接真实模型或 QQ，但会启动仅供测试的本机服务、临时数据库和子进程。

验证脚本依次运行 pytest、Ruff、Pyright 和依赖检查，结果写入 `evidence/validation/` 中新的独立目录，退出码为 0 才表示本次检查通过。Linux bubblewrap 的真实隔离测试需要显式提供环境，默认跳过；被跳过的检查不算通过。`qq_live`、`model_live` 默认排除。

## 修改时看哪里

完整模块图见 [架构导览](docs/architecture.md)。常用的定向检查如下，将 `python` 替换为上面的虚拟环境解释器路径：

| 修改范围 | 优先检查 |
| --- | --- |
| QQ 消息转换、引用与传输 | `python -m pytest tests/qq tests/integration/test_reference_pipeline.py -q` |
| 文件、附件及交付 | `python -m pytest tests/materials tests/integration/test_material_pipeline.py -q` |
| 模型循环、上下文 | `python -m pytest tests/agent tests/context -q` |
| 协作、停止、恢复 | `python -m pytest tests/state tests/recovery tests/integration/test_runtime.py -q` |
| 执行工具与进程 | `python -m pytest tests/integration/test_execution_tools.py tests/integration/test_process_tree.py -q` |

代码变更先跑相关检查，提交前跑 `python tools/run_validation.py`。只改文档时检查命令、链接与示例；无需为了文档再添加没有行为价值的测试。

## 提交 Pull Request

每个 PR 尽量解决一个明确问题。标题说明结果，正文填写：问题或目标、实现范围、如何验证、尚未覆盖的环境。修复问题请关联 Issue；较大的接口变化先在 Issue 中给出具体设计与使用例子。

```sh
git status --short
git diff --check
git add <本次修改的文件>
git diff --cached
git commit -m "docs: explain local configuration"
git push -u origin HEAD
```

然后在 GitHub 从自己的分支向本仓库 `main` 发起 PR。未完成也可以提交 Draft PR，便于讨论。自动检查通过后由维护者审阅；合并代码与部署服务分别进行。

如使用 AI 辅助生成代码，提交者仍负责理解、检查来源和验证行为。在 PR 里说明重要的生成或移植内容即可，不需要上传完整对话、私人提示词或工具日志。

## 凭据、数据和真实联调

- 本机配置放在 `var/` 或命名为 `*.local.json`，密钥通过环境变量提供。示例只写变量名和合成身份。
- 不提交真实 QQ 号、群号、聊天原文、附件、数据库、访问令牌、服务器私钥和原始运行报告。复现问题时使用临时目录及合成数据。
- 测试代码不能默认启用真实 QQ、真实模型或生产部署。真实联调需要维护者分配的测试环境和明确范围；不要修改在线机器人的连接来获得测试入口。
- 源码公开不意味着生产配置、模型额度和成员资料对开发者开放。

可另行安装 [Gitleaks CLI](https://github.com/gitleaks/gitleaks) 并在提交前执行 `gitleaks git --pre-commit --staged --redact=100`。GitHub 自动检查也扫描提交历史；扫描器不能识别所有个人信息，提交前仍需查看差异。

## 来源与许可

贡献按项目现有 AGPL-3.0-or-later 许可提供。第三方复制或移植须保留原版权和许可证，在 `third_party/` 记录仓库、固定提交、文件路径及实际改动。不要移除已有的 MIT 声明或把整个工程改标为 MIT。

优先讨论代码和证据，尊重不同经验的成员；审阅时说明问题、影响和可行修改方式。不要在公开讨论中披露他人账号信息。
