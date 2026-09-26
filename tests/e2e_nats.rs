#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 离线 E2E：单文件串起「TOML 配置 → validate → 不可达 connect → 数据面 fail-closed」。
//! 不依赖真实 NATS broker。真连服见 `live_nats.rs`（默认 ignore）。

use std::time::{Duration, Instant};

use natsx::{JetStream, NatsConfig, NatsError, NatsPool};

#[tokio::test]
async fn offline_config_to_unreachable_connect_fails_closed() {
    let toml = r#"
schema_version = 1
url = "nats://127.0.0.1:1"
name = "natsx-e2e"
connect_timeout_ms = 300
operation_timeout_ms = 300
"#;
    let config = NatsConfig::from_toml(toml).expect("无敏感字段的 TOML 必须能解析");
    config.validate().expect("loopback 明文允许 Prefer");

    let started = Instant::now();
    let error = NatsPool::connect(config.clone())
        .await
        .expect_err("不可达地址必须返回 Err");
    assert!(
        matches!(error, NatsError::Connection(_) | NatsError::Timeout(_)),
        "{error:?}"
    );
    assert!(error.is_retryable());
    assert!(started.elapsed() < Duration::from_secs(10));

    let pool = NatsPool::new(config).expect("new 只校验、不联网");
    assert!(!pool.is_connected());
    pool.publish("e2e.subject", "payload")
        .await
        .expect_err("未连接不可发布");
    JetStream::from_pool(&pool).expect_err("未连接不可构造 JetStream");
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(!health.connected);
}
