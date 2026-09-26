//! `natsx` —— NATS 适配器：Core NATS 发布/订阅 + JetStream 持久消费。
//!
//! 本 crate 是零内部依赖的标准组件库，只依赖 crates.io 公开包，
//! 可被任意 Rust 工程以源码或 git 依赖方式复用。
//!
//! # 快速开始
//!
//! ```no_run
//! use natsx::{NatsConfig, NatsPool, NatsResult};
//!
//! # async fn run() -> NatsResult<()> {
//! let pool = NatsPool::connect(NatsConfig::default()).await?;
//! let mut subscription = pool.subscribe("demo.subject").await?;
//! pool.publish("demo.subject", "hello nats").await?;
//! if let Some(message) = subscription.next().await {
//!     println!("收到 {} 字节", message.payload.len());
//! }
//! println!("rtt = {:?}", pool.ping().await?);
//! pool.drain(std::time::Duration::from_secs(5)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # 公共 API 一览
//!
//! | 入口 | 说明 |
//! | --- | --- |
//! | [`NatsConfig`] / [`NatsConfigBuilder`] | 配置：`from_env` / `from_toml` / `validate` / `builder` |
//! | [`TlsPolicy`] / [`url_is_loopback`] | TLS 策略与地址判定 |
//! | [`NatsPool`] | `connect` / `new` / `publish` / `publish_with_headers` / `subscribe` / `request` / `ping` / `flush` / `health_check` / `stats` / `close` / `drain` |
//! | [`NatsSubscription`] / [`NatsMessage`] | 订阅流（`next()` 或 `Stream`）与消息 |
//! | [`NatsHealth`] / [`NatsPoolStats`] | 结构化健康与统计 |
//! | [`JetStream`] | `publish` / `publish_json` / stream 管理 / `consumer` |
//! | [`JetStreamConsumer`] / [`JetStreamDelivery`] | 有限拉取与 `ack` / `nak` / `progress` / `term` |
//! | [`NatsError`] / [`NatsResult`] | 统一错误与结果别名 |
//!
//! # TLS 策略
//!
//! - loopback（`127.0.0.1` / `localhost` / `::1`）默认 [`TlsPolicy::Prefer`]：允许明文，服务端要求时才升级 TLS；
//! - 非 loopback 默认 [`TlsPolicy::Require`]：`ConnectOptions::require_tls(true)`，握手失败即连接失败；
//! - 显式设置 [`NatsConfig::tls_policy`] 优先，其次 [`NatsConfig::tls`] 布尔开关；
//! - `validate()` 会拒绝“非 loopback + 非 Require”的组合（fail-closed）；
//! - 自定义 CA 经 `add_root_certificates` 注入（根证书集合即该 CA bundle），
//!   仅配置 mTLS 证书时使用 `add_client_certificate`（保留系统根证书）。
//!
//! # 环境变量
//!
//! 规范前缀 `FOUNDATIONX_NATSX_*`（兼容 `FOUNDATIONX_NATS_*`，前者优先）：
//! `URL`、`SERVERS`、`USER`、`PASSWORD`、`TOKEN`、`NKEY_SEED`、`NAME`、`TLS`、
//! `TLS_POLICY`、`TLS_CA_FILE`、`TLS_CERT_FILE`、`TLS_KEY_FILE`、`JETSTREAM`、
//! `CONNECT_TIMEOUT_MS`、`OPERATION_TIMEOUT_MS`、`SUBSCRIPTION_CAPACITY`、
//! `CLIENT_CAPACITY`、`MAX_RECONNECTS`、`RECONNECT_MAX_DELAY_MS`、
//! `IGNORE_DISCOVERED_SERVERS`。详见 [`config`] 中的 `ENV_*` 常量。
//!
//! # 安全约定
//!
//! - `password` / `token` / `nkey_seed` 不进入 `Debug`（渲染为 `***`），URL 内嵌 userinfo 同样脱敏；
//! - 敏感字段禁止出现在 TOML 中，只能经环境变量或 [`NatsConfigBuilder`] 注入；
//! - 生产代码不含 `unwrap` / `expect` / `panic`，错误一律经 [`NatsError`] 返回。

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unreachable_pub)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod config;
pub mod error;
mod jetstream;
mod pool;
pub mod validation;

pub use config::{
    url_is_loopback, NatsConfig, NatsConfigBuilder, TlsPolicy, DEFAULT_CLIENT_NAME, DEFAULT_URL,
    ENV_CLIENT_CAPACITY, ENV_CONNECT_TIMEOUT_MS, ENV_JETSTREAM, ENV_LEGACY_PREFIX,
    ENV_MAX_RECONNECTS, ENV_NAME, ENV_NKEY_SEED, ENV_OPERATION_TIMEOUT_MS, ENV_PASSWORD,
    ENV_PREFIX, ENV_RECONNECT_MAX_DELAY_MS, ENV_SERVERS, ENV_SLOW_CONSUMER_TIMEOUT_MS,
    ENV_SUBSCRIPTION_CAPACITY, ENV_TLS, ENV_TLS_CA_FILE, ENV_TLS_CERT_FILE, ENV_TLS_KEY_FILE,
    ENV_TLS_POLICY, ENV_TOKEN, ENV_URL, ENV_USER, ENV_USERNAME, SCHEMA_VERSION,
};
pub use error::{NatsError, NatsResult};
pub use jetstream::{
    JetStream, JetStreamConsumer, JetStreamConsumerConfig, JetStreamDelivery,
    JetStreamDeliveryMetadata, PullConsumerConfig, StreamConfig, StreamInfo,
};
pub use pool::{NatsHealth, NatsMessage, NatsPool, NatsPoolStats, NatsSubscription};
pub use validation::{
    validate_consumer_name, validate_operation_timeout, validate_publish_subject,
    validate_stream_name, validate_subject,
};

#[cfg(test)]
mod public_api_surface {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_types_exist_and_are_send_sync() {
        assert_send_sync::<NatsPool>();
        assert_send_sync::<NatsSubscription>();
        assert_send_sync::<NatsMessage>();
        assert_send_sync::<NatsHealth>();
        assert_send_sync::<NatsPoolStats>();
        assert_send_sync::<NatsConfig>();
        assert_send_sync::<NatsConfigBuilder>();
        assert_send_sync::<NatsError>();
        assert_send_sync::<TlsPolicy>();
        assert_send_sync::<JetStream>();
        assert_send_sync::<JetStreamConsumer>();
        assert_send_sync::<JetStreamDelivery>();
        assert_send_sync::<JetStreamDeliveryMetadata>();
        assert_send_sync::<JetStreamConsumerConfig>();
        assert_send_sync::<PullConsumerConfig>();
        assert_send_sync::<StreamConfig>();
        assert_send_sync::<StreamInfo>();
    }

    #[test]
    fn default_exports_are_wired() {
        let config = NatsConfig::default();
        assert_eq!(config.url, DEFAULT_URL);
        assert_eq!(config.name, DEFAULT_CLIENT_NAME);
        assert_eq!(config.effective_tls_policy(), TlsPolicy::Prefer);
        assert_eq!(SCHEMA_VERSION, 1);
        assert!(url_is_loopback(DEFAULT_URL));
        assert!(validate_stream_name("EVENTS").is_ok());
        assert!(validate_subject("demo.subject").is_ok());
        assert!(validate_publish_subject("demo.subject").is_ok());
        assert!(validate_consumer_name("worker").is_ok());
        assert!(validate_operation_timeout(std::time::Duration::from_secs(1)).is_ok());
        assert!(ENV_PREFIX.starts_with("FOUNDATIONX_"));
        assert!(ENV_LEGACY_PREFIX.starts_with("FOUNDATIONX_"));
        let _ = StreamConfig::new("S", "s.>");
        let _ = PullConsumerConfig::durable("d");
        let _ = JetStreamConsumerConfig::durable("durable");
    }

    #[test]
    fn nats_pool_is_clone_and_reports_state() {
        let pool = NatsPool::new(NatsConfig::default()).expect("构造");
        let cloned = pool.clone();
        assert!(!cloned.is_connected());
        assert!(cloned.client().is_none());
        assert_eq!(cloned.stats(), NatsPoolStats::default());
    }
}
