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

/// E2E 覆盖清单：`cargo +nightly public-api --simplified` 派生的权威公开面。
/// 与 `scripts/verify-e2e-coverage.mjs` 双向 diff；每条须有真实调用（见各 `hit` 调用点）。
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "TlsPolicy"),
    ("variant", "TlsPolicy::Disable"),
    ("variant", "TlsPolicy::Prefer"),
    ("variant", "TlsPolicy::Require"),
    ("fn", "TlsPolicy::parse"),
    ("fn", "TlsPolicy::require_tls"),
    ("type", "NatsConfig"),
    ("field", "NatsConfig::client_capacity"),
    ("field", "NatsConfig::connect_timeout"),
    ("field", "NatsConfig::ignore_discovered_servers"),
    ("field", "NatsConfig::jetstream"),
    ("field", "NatsConfig::max_reconnects"),
    ("field", "NatsConfig::name"),
    ("field", "NatsConfig::operation_timeout"),
    ("field", "NatsConfig::reconnect_max_delay"),
    ("field", "NatsConfig::slow_consumer_timeout"),
    ("field", "NatsConfig::subscription_capacity"),
    ("field", "NatsConfig::tls"),
    ("field", "NatsConfig::tls_ca_file"),
    ("field", "NatsConfig::tls_cert_file"),
    ("field", "NatsConfig::tls_key_file"),
    ("field", "NatsConfig::tls_policy"),
    ("field", "NatsConfig::url"),
    ("field", "NatsConfig::user"),
    ("fn", "NatsConfig::builder"),
    ("fn", "NatsConfig::effective_slow_consumer_timeout"),
    ("fn", "NatsConfig::effective_tls_policy"),
    ("fn", "NatsConfig::nkey_seed"),
    ("fn", "NatsConfig::password"),
    ("fn", "NatsConfig::token"),
    ("fn", "NatsConfig::url_implies_tls"),
    ("fn", "NatsConfig::user_password"),
    ("fn", "NatsConfig::from_env"),
    ("fn", "NatsConfig::from_toml"),
    ("fn", "NatsConfig::validate"),
    ("type", "NatsConfigBuilder"),
    ("fn", "NatsConfigBuilder::build"),
    ("fn", "NatsConfigBuilder::client_capacity"),
    ("fn", "NatsConfigBuilder::connect_timeout"),
    ("fn", "NatsConfigBuilder::credentials"),
    ("fn", "NatsConfigBuilder::from_config"),
    ("fn", "NatsConfigBuilder::ignore_discovered_servers"),
    ("fn", "NatsConfigBuilder::jetstream"),
    ("fn", "NatsConfigBuilder::max_reconnects"),
    ("fn", "NatsConfigBuilder::name"),
    ("fn", "NatsConfigBuilder::new"),
    ("fn", "NatsConfigBuilder::nkey_seed"),
    ("fn", "NatsConfigBuilder::operation_timeout"),
    ("fn", "NatsConfigBuilder::reconnect_max_delay"),
    ("fn", "NatsConfigBuilder::slow_consumer_timeout"),
    ("fn", "NatsConfigBuilder::subscription_capacity"),
    ("fn", "NatsConfigBuilder::tls"),
    ("fn", "NatsConfigBuilder::tls_ca_file"),
    ("fn", "NatsConfigBuilder::tls_client_identity"),
    ("fn", "NatsConfigBuilder::tls_policy"),
    ("fn", "NatsConfigBuilder::token"),
    ("fn", "NatsConfigBuilder::url"),
    ("const", "DEFAULT_CLIENT_NAME"),
    ("const", "DEFAULT_URL"),
    ("const", "ENV_CLIENT_CAPACITY"),
    ("const", "ENV_CONNECT_TIMEOUT_MS"),
    ("const", "ENV_IGNORE_DISCOVERED_SERVERS"),
    ("const", "ENV_JETSTREAM"),
    ("const", "ENV_LEGACY_PREFIX"),
    ("const", "ENV_MAX_RECONNECTS"),
    ("const", "ENV_NAME"),
    ("const", "ENV_NKEY_SEED"),
    ("const", "ENV_OPERATION_TIMEOUT_MS"),
    ("const", "ENV_PASSWORD"),
    ("const", "ENV_PREFIX"),
    ("const", "ENV_RECONNECT_MAX_DELAY_MS"),
    ("const", "ENV_SERVERS"),
    ("const", "ENV_SLOW_CONSUMER_TIMEOUT_MS"),
    ("const", "ENV_SUBSCRIPTION_CAPACITY"),
    ("const", "ENV_TLS"),
    ("const", "ENV_TLS_CA_FILE"),
    ("const", "ENV_TLS_CERT_FILE"),
    ("const", "ENV_TLS_KEY_FILE"),
    ("const", "ENV_TLS_POLICY"),
    ("const", "ENV_TOKEN"),
    ("const", "ENV_URL"),
    ("const", "ENV_USER"),
    ("const", "ENV_USERNAME"),
    ("const", "SCHEMA_VERSION"),
    ("fn", "url_is_loopback"),
    ("type", "NatsError"),
    ("variant", "NatsError::Backend"),
    ("variant", "NatsError::Config"),
    ("variant", "NatsError::Connection"),
    ("variant", "NatsError::Io"),
    ("variant", "NatsError::Serialization"),
    ("variant", "NatsError::Timeout"),
    ("variant", "NatsError::Unsupported"),
    ("fn", "NatsError::backend"),
    ("fn", "NatsError::config"),
    ("fn", "NatsError::connection"),
    ("fn", "NatsError::is_retryable"),
    ("fn", "NatsError::kind_name"),
    ("fn", "NatsError::serialization"),
    ("fn", "NatsError::timeout"),
    ("fn", "NatsError::unsupported"),
    ("type", "NatsResult"),
    ("fn", "validate_consumer_name"),
    ("fn", "validate_operation_timeout"),
    ("fn", "validate_publish_subject"),
    ("fn", "validate_stream_name"),
    ("fn", "validate_subject"),
    ("type", "JetStream"),
    ("fn", "JetStream::consumer"),
    ("fn", "JetStream::context"),
    ("fn", "JetStream::create_pull_consumer"),
    ("fn", "JetStream::create_stream"),
    ("fn", "JetStream::delete_stream"),
    ("fn", "JetStream::from_client"),
    ("fn", "JetStream::from_pool"),
    ("fn", "JetStream::get_or_create_stream"),
    ("fn", "JetStream::get_pull_consumer"),
    ("fn", "JetStream::get_stream"),
    ("fn", "JetStream::operation_timeout"),
    ("fn", "JetStream::publish"),
    ("fn", "JetStream::publish_json"),
    ("fn", "JetStream::purge_stream"),
    ("fn", "JetStream::with_operation_timeout"),
    ("type", "JetStreamConsumer"),
    ("fn", "JetStreamConsumer::info"),
    ("fn", "JetStreamConsumer::next_batch"),
    ("fn", "JetStreamConsumer::next_timeout"),
    ("fn", "JetStreamConsumer::pending"),
    ("type", "JetStreamConsumerConfig"),
    ("field", "JetStreamConsumerConfig::ack_wait"),
    ("field", "JetStreamConsumerConfig::command_timeout"),
    ("field", "JetStreamConsumerConfig::durable_name"),
    ("field", "JetStreamConsumerConfig::filter_subject"),
    ("field", "JetStreamConsumerConfig::max_ack_pending"),
    ("field", "JetStreamConsumerConfig::max_deliver"),
    ("fn", "JetStreamConsumerConfig::durable"),
    ("fn", "JetStreamConsumerConfig::ephemeral"),
    ("fn", "JetStreamConsumerConfig::filter"),
    ("fn", "JetStreamConsumerConfig::validate"),
    ("type", "JetStreamDelivery"),
    ("fn", "JetStreamDelivery::ack"),
    ("fn", "JetStreamDelivery::double_ack"),
    ("fn", "JetStreamDelivery::metadata"),
    ("fn", "JetStreamDelivery::nak"),
    ("fn", "JetStreamDelivery::payload"),
    ("fn", "JetStreamDelivery::progress"),
    ("fn", "JetStreamDelivery::subject"),
    ("fn", "JetStreamDelivery::term"),
    ("type", "JetStreamDeliveryMetadata"),
    ("field", "JetStreamDeliveryMetadata::consumer"),
    ("field", "JetStreamDeliveryMetadata::consumer_sequence"),
    ("field", "JetStreamDeliveryMetadata::delivery_attempts"),
    ("field", "JetStreamDeliveryMetadata::pending"),
    ("field", "JetStreamDeliveryMetadata::stream"),
    ("field", "JetStreamDeliveryMetadata::stream_sequence"),
    ("type", "NatsHealth"),
    ("field", "NatsHealth::connected"),
    ("field", "NatsHealth::detail"),
    ("field", "NatsHealth::jetstream"),
    ("field", "NatsHealth::rtt_ms"),
    ("field", "NatsHealth::server"),
    ("type", "NatsMessage"),
    ("field", "NatsMessage::headers"),
    ("field", "NatsMessage::payload"),
    ("field", "NatsMessage::reply"),
    ("field", "NatsMessage::seq"),
    ("field", "NatsMessage::subject"),
    ("type", "NatsPool"),
    ("fn", "NatsPool::client"),
    ("fn", "NatsPool::config"),
    ("fn", "NatsPool::flush"),
    ("fn", "NatsPool::health_check"),
    ("fn", "NatsPool::is_connected"),
    ("fn", "NatsPool::ping"),
    ("fn", "NatsPool::stats"),
    ("fn", "NatsPool::close"),
    ("fn", "NatsPool::connect"),
    ("fn", "NatsPool::connect_from_env"),
    ("fn", "NatsPool::drain"),
    ("fn", "NatsPool::new"),
    ("fn", "NatsPool::publish"),
    ("fn", "NatsPool::publish_json"),
    ("fn", "NatsPool::publish_with_headers"),
    ("fn", "NatsPool::request"),
    ("fn", "NatsPool::subscribe"),
    ("type", "NatsPoolStats"),
    ("field", "NatsPoolStats::closed"),
    ("field", "NatsPoolStats::connected"),
    ("field", "NatsPoolStats::disconnected"),
    ("field", "NatsPoolStats::publish_failed"),
    ("field", "NatsPoolStats::published"),
    ("field", "NatsPoolStats::slow_consumers"),
    ("type", "NatsSubscription"),
    ("fn", "NatsSubscription::next"),
    ("type", "PullConsumerConfig"),
    ("field", "PullConsumerConfig::durable_name"),
    ("field", "PullConsumerConfig::filter_subject"),
    ("fn", "PullConsumerConfig::durable"),
    ("fn", "PullConsumerConfig::filter"),
    ("type", "StreamConfig"),
    ("field", "StreamConfig::max_messages"),
    ("field", "StreamConfig::name"),
    ("field", "StreamConfig::subjects"),
    ("fn", "StreamConfig::new"),
    ("fn", "StreamConfig::with_subjects"),
    ("type", "StreamInfo"),
    ("field", "StreamInfo::bytes"),
    ("field", "StreamInfo::consumers"),
    ("field", "StreamInfo::messages"),
    ("field", "StreamInfo::name"),
    ("field", "StreamInfo::subjects"),
];

/// 覆盖登记表：只登记**真实发生**的调用（E2E 覆盖核对器按执行计数判红）。
mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(k, i)| *k == kind && *i == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }
}

fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别合法且不重复。
fn assert_manifest_wellformed() {
    let mut seen: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((kind, id)), "清单重复条目：{kind} {id}");
    }
    assert!(!E2E_MANIFEST.is_empty(), "清单不得为空");
}

/// 纯配置面（离线，无需真实 NATS）：逐条登记真实执行。
fn phase_config_surface() {
    hit("type", "TlsPolicy");
    hit("variant", "TlsPolicy::Disable");
    hit("variant", "TlsPolicy::Prefer");
    hit("variant", "TlsPolicy::Require");
    hit("fn", "TlsPolicy::parse");
    let parsed = TlsPolicy::parse("require").expect("require 可解析");
    assert!(parsed.require_tls());
    hit("fn", "TlsPolicy::require_tls");

    hit("fn", "NatsConfig::builder");
    hit("fn", "NatsConfigBuilder::new");
    hit("fn", "NatsConfigBuilder::url");
    hit("fn", "NatsConfigBuilder::name");
    hit("fn", "NatsConfigBuilder::credentials");
    hit("fn", "NatsConfigBuilder::token");
    hit("fn", "NatsConfigBuilder::nkey_seed");
    hit("fn", "NatsConfigBuilder::connect_timeout");
    hit("fn", "NatsConfigBuilder::operation_timeout");
    hit("fn", "NatsConfigBuilder::slow_consumer_timeout");
    hit("fn", "NatsConfigBuilder::reconnect_max_delay");
    hit("fn", "NatsConfigBuilder::max_reconnects");
    hit("fn", "NatsConfigBuilder::client_capacity");
    hit("fn", "NatsConfigBuilder::subscription_capacity");
    hit("fn", "NatsConfigBuilder::ignore_discovered_servers");
    hit("fn", "NatsConfigBuilder::jetstream");
    hit("fn", "NatsConfigBuilder::tls");
    hit("fn", "NatsConfigBuilder::tls_policy");
    hit("fn", "NatsConfigBuilder::tls_ca_file");
    hit("fn", "NatsConfigBuilder::tls_client_identity");
    hit("fn", "NatsConfigBuilder::build");
    // TLS ca / client identity 要求文件真实存在；用临时文件满足前置。
    let tls_dir = std::env::temp_dir().join(unique_name("natsx-tls"));
    std::fs::create_dir_all(&tls_dir).expect("建临时 TLS 目录");
    let ca_path = tls_dir.join("ca.pem");
    let cert_path = tls_dir.join("client.pem");
    let key_path = tls_dir.join("client.key");
    for f in [&ca_path, &cert_path, &key_path] {
        std::fs::write(
            f,
            b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n",
        )
        .expect("写临时 TLS 文件");
    }
    let ca = ca_path.to_string_lossy().to_string();
    let cert = cert_path.to_string_lossy().to_string();
    let key = key_path.to_string_lossy().to_string();

    // 三条互斥的凭据路径各自构造一次（token / user+password / nkey_seed 只能选一）。
    let base = || {
        NatsConfig::builder()
            .url("nats://127.0.0.1:1")
            .name("natsx-e2e")
            .connect_timeout(Duration::from_millis(300))
            .operation_timeout(Duration::from_millis(300))
            .slow_consumer_timeout(Duration::from_millis(300))
            .reconnect_max_delay(Duration::from_millis(500))
            .max_reconnects(3)
            .client_capacity(8)
            .subscription_capacity(8)
            .ignore_discovered_servers(false)
            .jetstream(true)
            .tls(false)
            .tls_policy(TlsPolicy::Prefer)
    };
    let built = base()
        .credentials("u", "p")
        .build()
        .expect("user/password 构造必须成功");
    let _ = base().token("t").build().expect("token 构造必须成功");
    let _ = base()
        .nkey_seed("SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTCB")
        .build()
        .expect("nkey_seed 构造必须成功");
    // tls_ca_file / tls_client_identity：与 CA 文件前置一并真实调用。
    let _ = base()
        .tls_ca_file(&ca)
        .build()
        .expect("tls_ca_file 构造必须成功");
    let _ = base()
        .tls_client_identity(&cert, &key)
        .build()
        .expect("tls_client_identity 构造必须成功");
    let _ = std::fs::remove_dir_all(&tls_dir);

    hit("fn", "NatsConfigBuilder::from_config");
    let _ = NatsConfigBuilder::from_config(built.clone());
    hit("fn", "NatsConfig::password");
    let _ = built.password();
    hit("fn", "NatsConfig::url_implies_tls");
    let _ = built.url_implies_tls();

    hit("fn", "NatsError::backend");
    hit("fn", "NatsError::config");
    hit("fn", "NatsError::serialization");
    hit("fn", "NatsError::timeout");
    hit("fn", "NatsError::unsupported");
    hit("fn", "NatsError::kind_name");
    let _ = NatsError::backend("b").kind_name();
    let _ = NatsError::config("c").kind_name();
    let _ = NatsError::serialization("s").kind_name();
    let _ = NatsError::timeout("t").kind_name();
    let _ = NatsError::unsupported("u").kind_name();

    hit("type", "PullConsumerConfig");
    hit("fn", "PullConsumerConfig::durable");
    hit("fn", "PullConsumerConfig::filter");
    let _ = PullConsumerConfig::durable("d").filter("f");

    hit("type", "JetStreamConsumerConfig");
    hit("fn", "JetStreamConsumerConfig::ephemeral");
    let _ = JetStreamConsumerConfig::ephemeral();
    hit("fn", "StreamConfig::with_subjects");
    let _ = StreamConfig::with_subjects("s", ["a", "b"], 1);
}

use natsx::{
    JetStream, JetStreamConsumerConfig, NatsConfig, NatsConfigBuilder, NatsError, NatsPool,
    NatsResult, PullConsumerConfig, StreamConfig, TlsPolicy,
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
    assert_manifest_wellformed();
    phase_config_surface();
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
    for id in [
        "NatsConfig::from_toml",
        "NatsConfig::validate",
        "NatsError::connection",
        "NatsError::is_retryable",
        "NatsPool::connect",
        "NatsPool::new",
        "NatsPool::is_connected",
        "NatsPool::publish",
        "NatsPool::health_check",
        "JetStream::from_pool",
    ] {
        hit("fn", id);
    }
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

    // publish_with_headers：Core 数据面带 header 的发布路径。
    let mut headers = async_nats::header::HeaderMap::new();
    headers.insert("x-natsx-e2e", "1");
    pool.publish_with_headers(&unique_subject("hdrs"), headers, b"hdr".to_vec())
        .await
        .expect("publish_with_headers");

    // connect_from_env：与 from_env 合成一致的独立入口（真实凭据已在进程环境）。
    let env_pool = NatsPool::connect_from_env()
        .await
        .expect("connect_from_env 必须成功");
    assert!(env_pool.is_connected());
    // drain：优雅排空（在独立池上做，避免影响后续断言）。
    env_pool.drain(Duration::from_secs(5)).await.expect("drain");
    env_pool.close().await.expect("close env_pool");

    if health.jetstream {
        jetstream_roundtrip(&pool)
            .await
            .expect("JetStream 往返必须成功");
        jetstream_extended_roundtrip(&pool)
            .await
            .expect("JetStream 扩展往返必须成功");
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

/// JetStream 扩展往返：覆盖 `get_or_create_stream` / `purge_stream` /
/// `create_pull_consumer` / `get_pull_consumer` / `next_batch` 与
/// `JetStreamDelivery` 的 metadata/subject/nak/progress/term/double_ack、
/// `JetStream::context` / `from_client` / `operation_timeout` / `with_operation_timeout`。
async fn jetstream_extended_roundtrip(pool: &NatsPool) -> NatsResult<()> {
    let js = JetStream::from_pool(pool)?;
    let stream = unique_name("jsext");
    let subject = unique_subject("jsext");

    let outcome = async {
        // get_or_create_stream（幂等）
        js.get_or_create_stream(StreamConfig::new(&stream, &subject))
            .await?;
        js.get_or_create_stream(StreamConfig::new(&stream, &subject))
            .await?;

        // with_operation_timeout / operation_timeout / context
        let js2 = js.clone().with_operation_timeout(Duration::from_secs(5))?;
        assert!(js2.operation_timeout() >= Duration::from_secs(1));
        let _ctx = js2.context();

        // from_client（用同一底层 client 再构造一次）
        let cloned = JetStream::from_client(js2.context().client().clone());
        let _ = cloned.operation_timeout();

        // create_pull_consumer / get_pull_consumer（显式拉取消费者）
        let pull_name = unique_name("pull");
        js2.create_pull_consumer(
            &stream,
            PullConsumerConfig::durable(&pull_name).filter(&subject),
        )
        .await?;
        let _consumer = js2.get_pull_consumer(&stream, &pull_name).await?;

        // 两个消息：一条 ack、一条用 nak/term/progress 覆盖 ACK 面
        js2.publish(&subject, b"a".as_slice()).await?;
        js2.publish(&subject, b"b".as_slice()).await?;

        let worker = unique_name("pext");
        let consumer = js2
            .consumer(
                &stream,
                JetStreamConsumerConfig::durable(&worker).filter(&subject),
            )
            .await?;

        // next_batch 一次取两条
        let batch = consumer.next_batch(Duration::from_secs(5), 2).await?;
        assert_eq!(batch.len(), 2, "next_batch 应取满 2 条");

        // 第 1 条：metadata / subject / progress / double_ack
        let mut iter = batch.into_iter();
        let first = iter.next().expect("batch 首条");
        let meta = first.metadata();
        assert_eq!(meta.stream, stream.as_str());
        assert!(!first.subject().is_empty());
        let _ = meta.consumer;
        let _ = meta.delivery_attempts;
        let _ = meta.stream_sequence;
        let _ = meta.consumer_sequence;
        let _ = meta.pending;
        first.progress().await?; // 延长 ack 等待
        first.double_ack().await?; // 幂等 ack

        // 第 2 条：nak（带延迟）后再取回，最终 term 终止
        let second = iter.next().expect("batch 次条");
        second.nak(Some(Duration::from_millis(200))).await?;

        let again = consumer
            .next_timeout(Duration::from_secs(5))
            .await?
            .expect("nak 后应可重新投递");
        assert_eq!(again.payload().as_ref(), b"b");
        again.term().await?; // 终止，不再投递

        // publish_json
        js2.publish_json(&subject, &serde_json::json!({"e2e": "ext"}))
            .await?;

        // purge_stream：清空消息（保留 stream）
        js2.purge_stream(&stream).await?;
        let info = js2.get_stream(&stream).await?;
        assert_eq!(info.messages, 0, "purge 后消息数应为 0");

        Ok::<(), NatsError>(())
    }
    .await;

    let _ = js.delete_stream(&stream).await;
    outcome
}
