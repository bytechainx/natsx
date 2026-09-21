# Changelog — natsx

本文件记录 `natsx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/nats` 抽取而来（抽取时点为 `0.3.10`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 新增

- 特性 002 三类测试面：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部 `TDD-PROBE` 表
  覆盖公开接口契约登记的全部 10 个入口）、`tests/sdd_spec.rs`（`docs/标准.md` 五章 1:1 的
  `SPEC-MAP` 断言）、`tests/aidd_boundary.rs`（10 条对抗/边界用例与 AIDD 复核表）。
- `tests/live_nats.rs`：真连服用例（发布/订阅往返 + ping RTT + request-reply + close 收尾），
  恒 `#[ignore]`，凭据只读 `FOUNDATIONX_NATSX_*` 环境变量。

## [0.1.2] - 2026-09-22

### 修正

- 凭据不再经 TOML 错误消息泄漏：`NatsConfig::from_toml` 与内部的敏感字段预检此前把 `toml`
  的错误原文（含出错行的源码片段）插值进 `NatsError::Serialization`；当出错行正是承载凭据的
  那一行（如 `password = "…` 引号未闭合，或该行触发未知字段错误）时，凭据片段会被回显进日志
  与打点。现改为只保留错误摘要、行号与字节区间，不回显 TOML 源码。
  `docs/标准.md` §2 同步补一条对应该义务的条目。

## [0.1.1] - 2026-09-22

### 修正

- `tests/config_env.rs` 的环境变量夹具改为对 `FOUNDATIONX_NATSX_*` / `FOUNDATIONX_NATS_*`
  全空间封闭（构造时快照并清空全部键、退出时按快照恢复），用例不再随调用环境漂移。
  旧夹具只清理本用例显式设置的少数键：当进程环境里已存在 `FOUNDATIONX_NATSX_URL`
  （联调时 `source natsx.env`，`run-release-gate.sh <crate> --live` 即此形态）时，
  `from_env` 的 `URL` 优先于 `SERVERS`，`from_env_handles_servers_list_and_invalid_values`
  会读到外部注入的 URL 并断言失败。实测：干净环境 7/7 通过，带 live 环境 6/7 通过。
  **无生产代码改动**，公开行为与语义均不变。

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
