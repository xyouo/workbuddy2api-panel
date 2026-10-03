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

相关 PR 与 `main` push 会自动运行构建；也可在 Actions 页面手动运行。`ios-v*` tag
在构建及归档校验通过后会发布标记为 prerelease 的 Release，
上传压缩包和 `SHA256SUMS`。这不改变“待真机验证”的状态，也不会生成未验证的 `.deb`。

## Codex CLI 0.160.0（第三方 Responses 适配）

本项目不是 OpenAI 官方服务；它把 Responses 请求无状态转换为 WorkBuddy 上游实际支持的 Chat Completions。先从 `GET /v1/models` 选择真实返回的模型 ID，不要把其他模型冒充为 OpenAI 模型。`~/.codex/config.toml` 最小示例：

```toml
model = "cn:<GET-/v1/models-返回的模型>"
model_provider = "workbuddy"
model_reasoning_summary = "none"

[model_providers.workbuddy]
name = "WorkBuddy2API third-party adapter"
base_url = "http://127.0.0.1:7863/v1"
env_key = "WORKBUDDY_API_KEY"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
```

启动前设置 `WORKBUDDY_API_KEY='<占位密钥>'`。实现支持文本、HTTP/data URL 图片、function tools、Codex 自由文本 custom tools（包括 Codex 0.160.0 使用的 grammar 格式；转换为带 `input` 字段的函数并可逆还原）、工具结果和 SSE。客户端必须在每次请求提交完整历史；`previous_response_id`、`background`、Responses WebSocket、`/responses/compact`、hosted tools和加密 reasoning/签名会明确返回错误，不会被静默忽略。流式文本增量在上游每次写入后同步转换并 Flush，不等待完整回复；取消沿原请求 context 传播，异常断流不会生成成功完成事件。

## Claude Code（第三方 Messages 适配）

```sh
export ANTHROPIC_BASE_URL='http://127.0.0.1:7863'
export ANTHROPIC_AUTH_TOKEN='<占位密钥>'
export ANTHROPIC_MODEL='cn:<GET-/v1/models-返回的模型>'
claude
```

路由是 `${ANTHROPIC_BASE_URL}/v1/messages`，包括 Claude Code 常见的 `?beta=true`；接受 `x-api-key` 或 Bearer。支持 system、文本、HTTP/base64 图片、tools/input_schema、tool_use/tool_result、多工具及 Messages SSE。`anthropic-version` 必填；未知 `anthropic-beta` 值被视为客户端声明而非能力承诺。thinking/signature、prompt caching、context management 和非文本 tool_result 会明确报错。`/v1/messages/count_tokens` 返回 501，而不伪造精确 token 数；Claude Code 应使用自身的上下文估算并直接发送 Messages 请求。协议可连接不表示 Anthropic 官方支持或全部 Claude 功能兼容。

两种客户端都建议先开新会话做文本与一次工具闭环，再迁移工作；修改旧会话 provider 只改变后续网络目的地，不会转换旧历史，也不能保证目标模型理解旧模型历史。

## CI、Artifact 与发布

* “iOS 构建”：相关 PR 与 `main` push 自动测试、`go vet`、使用真实 iPhoneOS SDK 编译并上传 Artifact；`workflow_dispatch` 可手动运行。`ios-v*` tag 从 tag 提交重复全部步骤并创建 prerelease。
* “Docker 构建（手动）”和“其他平台构建（手动）”：仅 `workflow_dispatch`；`publish=false`（默认）只构建，明确选择 `publish=true` 才发布。
* iOS Artifact 名为 `workbuddy2api-panel-ios-arm64`，内含 `workbuddy2api-panel-ios-arm64.tar.gz` 与 `SHA256SUMS`。归档仅含程序、示例配置、许可证、本文和 `BUILD-INFO.txt`。

下载后先执行 `shasum -a 256 -c SHA256SUMS`，解包并按 RootHide 环境使用你信任的本地签名工具重新签名，再以前台方式 `./wb2api -config ./config.ios.example.json` 验证，停止用 `Ctrl-C`。CI 的 adhoc 签名不保证真机直接启动；不要硬编码 `.jbroot`，本项目尚未验证 libvroot 路径重定向，也不提供 arm64e 或 `.deb`。最低部署目标为 iOS 15.0（兼容目标设备 iOS 15.5），新产物仍须真机验证。

### 可复现的本地协议检查

```sh
go test ./...
go vet ./...
GOOS=ios GOARCH=arm64 CGO_ENABLED=1 go build ./cmd/server # 需 macOS + iPhoneOS SDK/clang；CI 使用完整命令
```

测试使用合成账号和 mock HTTP 上游，验证转换、实时 SSE、工具关联及既有 Chat Completions 回归；它不是实际 Codex/Claude 登录、生产账号或 iOS 真机验证。本修复环境未安装并登录 Codex CLI 0.160.0 或 Claude Code，因此客户端进程工具闭环仍待验证，不能把 HTTP 集成测试解释为真实客户端通过。
