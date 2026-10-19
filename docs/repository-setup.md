# 仓库管理说明

以下设置由 GitHub 组织或仓库管理员执行。本文件说明目标配置，不表示远端已经启用。

1. 将默认分支设为 `main`，启用 Issues 和 Pull Requests。
2. 对 `main` 设置规则：禁止强制推送和删除；要求 PR、至少一名审阅者、解决审阅讨论以及当前 CI 检查通过。第一次 Actions 完成后再从实际检查名称选择 required checks，避免填写不存在的名字。
3. 组织基础权限维持 Read；日常成员用 Fork 或有限 Write 团队，维护者另行管理合并和版本。至少保留两名可恢复访问的组织管理员。
4. 在 Security 设置中启用 Private vulnerability reporting，以及账号方案支持的 secret scanning / push protection。发现历史泄露应先撤销或轮换秘密，不能只删除文件。
5. Actions 使用 GitHub 托管 runner，默认令牌只读。Fork PR 不注入生产秘密、不接入自托管生产机器；不通过 `pull_request_target` 运行贡献者代码。
6. 不设置合并后自动部署真实 QQ 服务。部署由运维负责人核对版本、配置、备份和实际运行证据后独立执行。

`ci.yml` 在 Python 3.12 的 Windows 与 Linux 环境安装锁定依赖并运行项目现有验证入口，随后构建 wheel。`secrets.yml` 使用固定版本及校验和的 Gitleaks CLI 扫描历史。工具扫描不能替代对真实账号、聊天原文和附件的人工检查。

发布对应版本时，在服务用户可见的帮助或介绍入口提供本仓库对应提交/版本的源码链接，并保持必要的构建和配置说明可获取。公开开发仓库与更新在线服务是两个独立操作。
