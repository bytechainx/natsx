# Changelog — natsx

本文件记录 `natsx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/nats` 抽取而来（抽取时点为 `0.3.10`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 新增

- 特性 002 三类测试面：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部 `TDD-PROBE` 表
  覆盖公开接口契约登记的全部 10 个入口）、`tests/sdd_spec.rs`（`docs/标准.md` 五章 1:1 的
  `SPEC-MAP` 断言）、`tests/aidd_boundary.rs`（9 条对抗/边界用例与 AIDD 复核表）。
- `tests/live_nats.rs`：真连服用例（发布/订阅往返 + ping RTT + request-reply + close 收尾），
  恒 `#[ignore]`，凭据只读 `FOUNDATIONX_NATSX_*` 环境变量。

## [0.1.0] - 2026-09-21

### 新增

- `NatsPool`：共享 `async-nats` 客户端句柄的连接池（可克隆），提供 `connect` / `new` /
  `publish` / `publish_with_headers` / `subscribe` / `request` / `ping` / `flush` /
  `health_check` / `stats` / `close` / `drain`。
- Core NATS 订阅面：`NatsSubscription`（`next()` 或 `Stream`）与 `NatsMessage`。
- `NatsConfig` / `NatsConfigBuilder` / `TlsPolicy` / `url_is_loopback`：`from_env` /
  `from_toml` / `validate` / `builder`，`SCHEMA_VERSION = 1`，附 `ENV_*` 常量。
- JetStream 面：`JetStream`（`publish` / `publish_json` / stream 管理 / `consumer`）、
  `JetStreamConsumer`（有限拉取，单条与批量）、`JetStreamDelivery` 的
  `ack` / `nak` / `progress` / `term`，以及 `JetStreamConsumerConfig` / `PullConsumerConfig` /
  `StreamConfig` / `StreamInfo`。
- 校验纯函数：`validate_subject` / `validate_publish_subject` / `validate_stream_name` /
  `validate_consumer_name` / `validate_operation_timeout`，在发出网络请求前拒绝非法名称。
- 结构化可观测：`NatsHealth`（`connected` / `server` / `rtt_ms` / `jetstream` / `detail`）与
  `NatsPoolStats`（发布、连接、断线与慢消费者计数）。
- 统一错误 `NatsError` / `NatsResult`（`#[non_exhaustive]` + `is_retryable`）。

### 变更

- 移除对主工程内部 crate（`kernel` / `contracts` 等）的依赖：错误模型下沉为 crate 内
  `src/error.rs` 的 `NatsError`，配置解析、连接池与 JetStream 封装全部改为 crate 内自洽实现。
- TLS 收敛为显式 `TlsPolicy`（loopback 默认 `Prefer`，非 loopback 默认 `Require`），
  `validate()` 拒绝「非 loopback + 非 Require」组合；显式 `tls_policy` 优先于 `tls` 布尔开关。

### 说明

- **不提供** NATS 服务端、K/V·Object Store 高层封装、请求路由框架或消息总线语义
  （topic 层级治理由调用方负责）；JetStream 覆盖范围限于拉取消费，cluster 拓扑与跨账户
  不在稳定承诺内。
- Core NATS 订阅是实时流，**无历史回放**；需要回放请使用 JetStream。
- `term` **不是** DLQ：只终止重投，不会把消息搬到隔离 subject。
- `publish` 在返回前执行一次 `flush`；`new` / `connect` 成功不代表服务端声明支持 JetStream。
- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
