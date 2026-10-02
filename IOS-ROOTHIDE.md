# 第三方 iOS / RootHide 适配

这是 WorkBuddy2API Panel 的第三方适配，不是 RootHide 官方软件。源码基线固定为上游
`1b6abaa0cce4a960749494cad56d67fe87a6ee1f`（`1.11.11-panel`）；保留原项目 MIT
许可证及版权声明。构建使用 Go 1.22.5、`GOOS=ios`、`GOARCH=arm64` 和 Xcode
iPhoneOS SDK，最低系统版本为 iOS 15.0（因此不高于目标 iOS 15.5）。

## 当前交付边界

工作流产出的是 iOS/ARM64 Mach-O 命令行程序，而不是 Linux ARM64 或 macOS ARM64
程序。它会检查 Mach-O 平台、CPU、最低系统版本、依赖、load commands、adhoc 签名、
entitlements 输出、绝对构建路径和归档清单。

**仍待 iOS 15.5 RootHide 真机验证。** 当前环境没有 iPhone，也没有 RootHide 的打包/
重定向工具和可信的 `libroot` / `libvroot` SDK。因此目前不生成 `iphoneos-arm64e` 的
`.deb`：只改 Debian Architecture、CPU subtype 或硬编码某个 `.jbroot` 都不能证明
RootHide 兼容。确认官方运行库版本、路径重定向和安装规则，并完成真机启动、网页、
OAuth、模型列表、聊天及流式响应测试后，才能增加 `.deb`。系统运行库不会复制进归档。

## 使用

1. 从 GitHub Actions 的 `ios-roothide` workflow 下载 artifact，并核对 `SHA256SUMS`。
2. 在 RootHide 环境中将归档解压到用户可执行的位置；不要依赖固定 `.jbroot` 名称。
3. 创建仅自己可读的工作目录，把 `config.ios.example.json` 复制为 `config.json`，替换
   明显的密钥占位符，然后运行：

   ```sh
   chmod 700 wb2api
   ./wb2api --work-dir /path/chosen/by/user --config config.json
   ```

4. 打开 `http://127.0.0.1:7863/panel/`；健康检查为
   `http://127.0.0.1:7863/healthz`。默认仅监听 loopback；只有明确理解暴露风险时才修改
   `listen`。

凭据、配置、状态、请求元数据和日志都应留在用户选择的工作目录。示例默认关闭签到、
成长、旅行、活跃、保活、夜猫子及余额刷新等自动任务，以免产生额外账号活动；需要时
可逐项启用。这是相对上游默认行为的唯一有意配置差异。请求指纹脱敏仍默认开启，程序
没有新增遥测或凭据回传。

停止程序时向**本次启动的 PID**发送 `TERM`（前台运行可按 Ctrl-C）；本适配不安装
开机服务，也不操作其他进程。账号授权请在手机网页面板自行完成，不要向 issue、构建
日志或公开仓库提交 token、`auths/`、真实配置或运行日志。

## 可复现验证分类

- 上游测试：workflow 中 `go test ./...`。
- 交叉编译成功：只有 macOS runner 的 iPhoneOS SDK 构建步骤成功才成立。
- 静态检查通过：只有 `verify-ios-artifact.sh` 的全部检查成功才成立。
- 真机运行成功：目前**不成立，待真机验证**。

推送 `work`、`ios-roothide` 或 `ios-roothide/**` 分支会自动运行构建；也可在 Actions
页面手动运行。`ios-v*` tag 在构建及归档校验通过后会发布标记为 prerelease 的 Release，
上传压缩包和 `SHA256SUMS`。这不改变“待真机验证”的状态，也不会生成未验证的 `.deb`。
