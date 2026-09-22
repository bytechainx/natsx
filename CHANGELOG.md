# Changelog — natsx

本文件记录 `natsx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/nats` 抽取而来（抽取时点为 `0.3.10`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 修复

- **重连退避加入 ±25% 抖动**（`reconnect_delay_callback` 路径）：原先纯指数退避是
  确定性序列，大规模部署下服务端重启会引发所有客户端同时重连的同步风暴
  （thundering herd）。抖动幅度与 postgresx `PgRetryConfig` 一致；退避上限仍受
  `reconnect_max_delay` 约束。（对抗审查 P1-1）

### 新增

- **`NatsConfig::slow_consumer_timeout`（可选）**：订阅转发任务判定慢消费者的独立
  超时，与服务端操作截止时间 `operation_timeout` 语义解耦。默认 `None` 回退
  `operation_timeout`，行为向后兼容。配套入口：`NatsConfigBuilder::slow_consumer_timeout`、
  环境变量 `FOUNDATIONX_NATSX_SLOW_CONSUMER_TIMEOUT_MS`、TOML `slow_consumer_timeout_ms`；
  `NatsConfig::effective_slow_consumer_timeout()` 返回生效值，零值被 `validate` 拒绝。
  （对抗审查 P1-3）

## [0.1.4] - 2026-09-22

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，
  把两个门面文件的生产段下沉为子模块。

  **`src/config.rs`（生产段 557 → 268）** —— 新增三个子模块：环境变量加载层
  （`from_env`、`apply_env_overrides` 与 `lookup_env` / `parse_bool` / `parse_usize` / `parse_millis`）
  → `src/config/envvars.rs`（131 行）；TOML 解析层（`from_toml`、`toml_error_summary`、
  `reject_secret_keys`）→ `src/config/tomlfile.rs`（75 行）；配置校验（`validate`）→
  `src/config/validate.rs`（118 行）。门面保留模块文档、全部 `ENV_*` / `DEFAULT_*` 常量、
  `NatsConfig` 与 `NatsConfigBuilder` 的类型定义、`Default` / `Debug`、`redact_url`、
  `builder` / `password` / `token` / `nkey_seed` / `user_password` / `effective_tls_policy` /
  `url_implies_tls` 与**原有内联测试**。

  **`src/jetstream.rs`（生产段 524 → 99）** —— 新增两个子模块：`impl JetStreamConsumer`
  → `src/jetstream/consumer.rs`（132 行）；`impl JetStream` → `src/jetstream/operations.rs`
  （324 行）。门面保留模块文档、两个类型的**结构定义与 `Debug`**（门面要用结构体字面量构造它们，
  而父模块看不到子模块的私有字段）、`validate_stream_create` / `run_bounded_command`
  两个私有辅助与**原有内联测试**。

  两处搬移**均无需放宽任何可见性**：搬走的项要么原本就是 `pub`，要么只在层内互调；`validate` /
  `from_env` / `from_toml` / 全部构建与消费方法签名一字未改。子模块名用 `envvars` / `tomlfile`
  而非 `env` / `toml`，避免 edition 2018 的 uniform path 让本地模块遮蔽 `std::env` 与 `toml`
  依赖 crate（与 `ossx` / `clickhousex` 的处理一致）。门面里只被内联测试使用的
  `validate_operation_timeout` 改由**测试模块内**导入，否则非测试构建报 unused import。
  属**纯搬移**（两文件的行多重集比对均确认**零代码行丢失**，内联测试段除三行新增之外逐字节一致），
  96 项测试与 doctest 结果不变。

## [0.1.3] - 2026-09-22

### 新增

- 特性 002 三类测试面：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部 `TDD-PROBE` 表
  覆盖公开接口契约登记的全部 10 个入口）、`tests/sdd_spec.rs`（`docs/标准.md` 五章 1:1 的
  `SPEC-MAP` 断言）、`tests/aidd_boundary.rs`（10 条对抗/边界用例与 AIDD 复核表）。
- `tests/live_nats.rs`：真连服用例（发布/订阅往返 + ping RTT + request-reply + close 收尾），
  恒 `#[ignore]`，凭据只读 `FOUNDATIONX_NATSX_*` 环境变量。

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/pool.rs` 的建连/关停与数据面下沉为子模块 —— 建连与关停 → `src/pool/connection.rs`、
  发布 / 订阅 / 请求-应答 → `src/pool/pubsub.rs`。门面 `src/pool.rs` 保留模块文档、全部公开类型
  （`NatsPool` / `NatsSubscription` / `NatsMessage` / `NatsHealth` / `NatsPoolStats`）与
  **原有内联测试**，并保留 `config` / `client` / `is_connected` / `ping` / `flush` /
  `health_check` / `stats` 与三个私有辅助（`ready_client` / `register_task` / `take_tasks`）。
  拆分后 `impl NatsPool` 跨三个文件，全部公开方法的签名与行为一律不变；唯一的可见性调整是把
  **仅由门面内联测试使用**的 `join_subscription_tasks` 提为 `pub(super)`。
  `src/pool.rs` 生产段由 **755 → 318** 行（`src/pool/connection.rs` 267、`src/pool/pubsub.rs` 213）。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当 `pool.rs` 生产段距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 45 行时，任一仓的任意改动都可能卡住元仓库的全部 PR。
  属**纯搬移**（行多重集比对确认零代码行丢失），全部 96 项测试与 doctest 结果不变。

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
