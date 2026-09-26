#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E：
//! 1. 离线失败路径（默认随 `cargo test --test e2e_nats` 跑）
//! 2. 真连全程（`#[ignore]`）：`from_env` → 建连 → health/ping → Core 往返
//!    → request-reply → JetStream 发布/拉取/ack → 删自建 stream → close
//!
//! 凭据只从环境读取（`FOUNDATIONX_NATSX_*` 优先，兼容 `FOUNDATIONX_NATS_*`）。
//! 运维登记（含 prod broker）在 ZoneCNH/opsstack `sre/secrets/env/*.md`，**禁止**抄进本仓。
//! 本机注入示例（路径以操作者机器为准，勿提交）：
//!
//! ```bash
//! set -a
//! source /home/workspace/xhyper.rs/ops/sre/secrets/env/natsx.env
//! set +a
//! cargo test --test e2e_nats -- --ignored --test-threads=1
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use natsx::{
    JetStream, JetStreamConsumerConfig, NatsConfig, NatsError, NatsPool, NatsResult, StreamConfig,
};

fn unique_subject(use_case: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("natsx.e2e.{use_case}.{}.{}", std::process::id(), nanos)
}

fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "natsx-e2e-{prefix}-{}-{counter}-{nanos}",
        std::process::id()
    )
}

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

/// 完整真连 E2E：一条用例走完配置到收尾，不依赖 `live_nats.rs` 拆条。
#[tokio::test]
#[ignore = "需要真实 NATS 与 FOUNDATIONX_NATSX_* / FOUNDATIONX_NATS_*"]
async fn e2e_live_full_journey() {
    let config = NatsConfig::from_env().expect("必须已注入 FOUNDATIONX_NATSX_* 或兼容前缀");
    config.validate().expect("from_env 结果必须能通过 validate");
    if let Some(ca) = config.tls_ca_file.as_deref() {
        assert!(
            std::path::Path::new(ca).is_file(),
            "TLS CA 文件必须存在: {ca}"
        );
    }
    let debug = format!("{config:?}");
    assert!(
        debug.contains("***") || !debug.contains('@'),
        "Debug 不得含 URL userinfo"
    );

    let pool = NatsPool::connect(config).await.expect("建连必须成功");
    assert!(pool.is_connected());

    let health = pool.health_check().await.expect("health_check 恒 Ok");
    assert!(health.connected, "{}", health.detail);
    assert!(health.rtt_ms > 0.0 && health.rtt_ms < 5_000.0);
    let rtt = pool.ping().await.expect("ping");
    assert!(rtt < Duration::from_secs(5));

    core_pubsub(&pool).await;
    core_request_reply(&pool).await;
    if health.jetstream {
        jetstream_roundtrip(&pool)
            .await
            .expect("JetStream 往返必须成功");
    }

    pool.close().await.expect("close");
    assert!(pool.stats().closed);
    assert!(pool.publish("natsx.e2e.after_close", "x").await.is_err());
}

async fn core_pubsub(pool: &NatsPool) {
    let subject = unique_subject("pubsub");
    let mut sub = pool.subscribe(&subject).await.expect("subscribe");
    pool.flush().await.expect("flush SUB");
    let payload = b"natsx-e2e-roundtrip".to_vec();
    pool.publish(&subject, payload.clone())
        .await
        .expect("publish");
    let message = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("recv timeout")
        .expect("message");
    assert_eq!(message.subject, subject);
    assert_eq!(message.payload.as_ref(), payload.as_slice());
    let value = serde_json::json!({ "e2e": true });
    pool.publish_json(&subject, &value)
        .await
        .expect("publish_json");
    let json_msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("json recv timeout")
        .expect("json message");
    assert_eq!(json_msg.payload.as_ref(), value.to_string().as_bytes());
    assert!(pool.stats().published >= 2);
}

async fn core_request_reply(pool: &NatsPool) {
    let subject = unique_subject("request");
    let mut responder = pool.subscribe(&subject).await.expect("responder sub");
    pool.flush().await.expect("flush responder");
    let responder_pool = pool.clone();
    let task = tokio::spawn(async move {
        if let Some(message) = responder.next().await {
            let reply = message.reply.expect("request 必须带 reply");
            responder_pool
                .publish(&reply, message.payload.clone())
                .await
                .expect("reply publish");
        }
    });
    let response = pool
        .request(&subject, b"e2e-ping".to_vec(), Duration::from_secs(5))
        .await
        .expect("request");
    assert_eq!(response.payload.as_ref(), b"e2e-ping");
    task.await.expect("responder");
}

async fn jetstream_roundtrip(pool: &NatsPool) -> NatsResult<()> {
    let js = JetStream::from_pool(pool)?;
    let stream = unique_name("js");
    let subject = unique_subject("js");
    let outcome = async {
        js.create_stream(StreamConfig::new(&stream, &subject))
            .await?;
        js.publish(&subject, b"e2e-js-1".as_slice()).await?;
        let info = js.get_stream(&stream).await?;
        assert_eq!(info.messages, 1);

        let worker = unique_name("c");
        let consumer = js
            .consumer(
                &stream,
                JetStreamConsumerConfig::durable(&worker).filter(&subject),
            )
            .await?;
        let delivery = consumer
            .next_timeout(Duration::from_secs(5))
            .await?
            .expect("pull");
        assert_eq!(delivery.payload().as_ref(), b"e2e-js-1");
        delivery.ack().await?;
        assert_eq!(consumer.pending().await?, 0);
        Ok::<(), NatsError>(())
    }
    .await;
    let _ = js.delete_stream(&stream).await;
    outcome
}
