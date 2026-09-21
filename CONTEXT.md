# natsx 上下文

本文件定义 `natsx` 与其使用方共享的核心词汇。它只记录领域含义与能力边界，
不记录具体实现、API 签名、存储或部署决定。

## 角色与边界

**适配器**：把 NATS 的 wire 协议收敛成稳定 Rust API 的库；只提供 Core 发布/订阅与
JetStream 拉取消费原语，不含服务端实现、领域模型或消息总线语义。
_Avoid_: 消息总线框架（本仓库不做 topic 层级治理、路由或投递编排）

**Core NATS 订阅**：实时转发的订阅流，只交付订阅建立之后发布的消息。
_Avoid_: 消息队列（Core NATS 是实时流，不承担排队与重投）

**JetStream 持久流**：把消息持久化到 stream 并由 consumer 显式拉取与确认的路径；它要求
服务端启用 JetStream，且本仓库只覆盖拉取消费。
_Avoid_: 持久化开关（JetStream 是独立的服务端子系统，不是 Core 订阅的附加选项）

**有界缓冲**：订阅与客户端命令队列都有容量上限（`subscription_capacity` /
`client_capacity`），满时按背压处理。
_Avoid_: 队列大小（有界是硬约束，不是性能调优参数）

## 语义与生命周期

**subject**：NATS 的寻址名；发布路径禁止通配符，订阅路径允许 `*` / `>`，两条路径的合法性
规则不同。
_Avoid_: 主题（「主题」不能表达通配符在发布/订阅两条路径上的差异）

**stream**：JetStream 中绑定一组 subject 的持久存储单元；名字禁止空白、点号与通配符，
与 subject 命名规则不同。
_Avoid_: topic（NATS 里没有 topic，stream 是持久化单元而非寻址名）

**durable consumer**：有持久名字与位点的 consumer，重连后从上次确认处继续；durable 名为
`None` 表示 ephemeral，位点不跨连接保留。
_Avoid_: 订阅者（durable 强调的是位点持久化，不是订阅关系）

**有限拉取（pull）**：consumer 主动按条或按批取消息，取到后必须显式确认。
_Avoid_: 推送消费（pull 与 push 是两种投递模型，确认责任方不同）

**drain**：优雅关停——结束订阅转发任务并 `flush` 在途消息；`close` 则是立即关闭。
_Avoid_: 关闭连接（drain 会等待在途消息，语义强于单纯断开）

## 错误与可靠性

**TLS 策略（TlsPolicy）**：`Prefer` / `Require` / `Disable` 三档，按 host 推导默认值；
`validate()` 拒绝「非 loopback + 非 Require」组合。
_Avoid_: 加密开关（「开/关」二值不足以表达 Prefer 这种「允许明文、服务端要求则升级」的中间态）

**显式确认**：JetStream 投递的四种回应——`ack` 成功、`nak` 要求重投、`progress` 续期、
`term` 终止重投；本仓库不自动 ack。
_Avoid_: 自动确认（自动 ack 会掩盖处理失败，本仓库刻意不提供）

**秘密脱敏**：`password` / `token` / `nkey_seed` 与 URL 内嵌 userinfo 在 `Debug` 中渲染为
`***`，且禁止落盘到 TOML（只能经环境变量或 builder 注入）。
_Avoid_: 隐藏字段（脱敏是渲染层约束，不改变字段本身的可读性）

**结构化健康**：`health_check()` 返回 `connected` / `server` / `rtt_ms` / `jetstream`；
信息不可用时返回空串或 `false`，表示**未知**而非已证否。
_Avoid_: 就绪探针（健康结果是数据，判读与告警策略由调用方决定）
