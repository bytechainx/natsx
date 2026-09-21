//! 公共 API 表面：类型存在性、`Send + Sync`、`Clone`、错误分类与 Debug 输出。

use std::time::Duration;

use bytes::Bytes;
use natsx::{
    url_is_loopback, JetStream, JetStreamConsumer, JetStreamConsumerConfig, JetStreamDelivery,
    JetStreamDeliveryMetadata, NatsConfig, NatsConfigBuilder, NatsError, NatsHealth, NatsMessage,
    NatsPool, NatsPoolStats, NatsResult, NatsSubscription, PullConsumerConfig, StreamConfig,
    StreamInfo, TlsPolicy, DEFAULT_CLIENT_NAME, DEFAULT_URL, ENV_CLIENT_CAPACITY,
    ENV_CONNECT_TIMEOUT_MS, ENV_JETSTREAM, ENV_LEGACY_PREFIX, ENV_MAX_RECONNECTS, ENV_NAME,
    ENV_NKEY_SEED, ENV_OPERATION_TIMEOUT_MS, ENV_PASSWORD, ENV_PREFIX, ENV_RECONNECT_MAX_DELAY_MS,
    ENV_SERVERS, ENV_SUBSCRIPTION_CAPACITY, ENV_TLS, ENV_TLS_CA_FILE, ENV_TLS_CERT_FILE,
    ENV_TLS_KEY_FILE, ENV_TLS_POLICY, ENV_TOKEN, ENV_URL, ENV_USER, ENV_USERNAME, SCHEMA_VERSION,
};

fn assert_send_sync<T: Send + Sync>() {}
fn assert_static<T: 'static>() {}

#[test]
fn public_types_are_send_sync_and_static() {
    assert_send_sync::<NatsPool>();
    assert_send_sync::<NatsSubscription>();
    assert_send_sync::<NatsMessage>();
    assert_send_sync::<NatsHealth>();
    assert_send_sync::<NatsPoolStats>();
    assert_send_sync::<NatsConfig>();
    assert_send_sync::<NatsConfigBuilder>();
    assert_send_sync::<NatsError>();
    assert_send_sync::<NatsResult<()>>();
    assert_send_sync::<TlsPolicy>();
    assert_send_sync::<JetStream>();
    assert_send_sync::<JetStreamConsumer>();
    assert_send_sync::<JetStreamDelivery>();
    assert_send_sync::<JetStreamDeliveryMetadata>();
    assert_send_sync::<JetStreamConsumerConfig>();
    assert_send_sync::<PullConsumerConfig>();
    assert_send_sync::<StreamConfig>();
    assert_send_sync::<StreamInfo>();

    assert_static::<NatsPool>();
    assert_static::<JetStream>();
}

#[test]
fn constants_are_exported() {
    assert_eq!(DEFAULT_URL, "nats://127.0.0.1:4222");
    assert_eq!(DEFAULT_CLIENT_NAME, "natsx");
    assert_eq!(SCHEMA_VERSION, 1);
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_NATSX_");
    assert_eq!(ENV_LEGACY_PREFIX, "FOUNDATIONX_NATS_");
    for name in [
        ENV_URL,
        ENV_SERVERS,
        ENV_USER,
        ENV_USERNAME,
        ENV_PASSWORD,
        ENV_TOKEN,
        ENV_NKEY_SEED,
        ENV_NAME,
        ENV_TLS,
        ENV_TLS_POLICY,
        ENV_TLS_CA_FILE,
        ENV_TLS_CERT_FILE,
        ENV_TLS_KEY_FILE,
        ENV_JETSTREAM,
        ENV_CONNECT_TIMEOUT_MS,
        ENV_OPERATION_TIMEOUT_MS,
        ENV_SUBSCRIPTION_CAPACITY,
        ENV_CLIENT_CAPACITY,
        ENV_MAX_RECONNECTS,
        ENV_RECONNECT_MAX_DELAY_MS,
    ] {
        assert!(name.starts_with(ENV_PREFIX), "{name} 应使用规范前缀");
    }
}

#[test]
fn pool_is_cloneable_and_debuggable() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置可构造");
    let cloned = pool.clone();
    assert!(!cloned.is_connected());
    assert!(cloned.client().is_none());
    assert_eq!(cloned.stats(), NatsPoolStats::default());
    assert_eq!(pool.config().url, DEFAULT_URL);
    let debug = format!("{pool:?}");
    assert!(debug.contains("NatsPool"));
    assert!(debug.contains(DEFAULT_URL));
}

#[test]
fn public_types_are_constructible_offline() {
    let message = NatsMessage {
        subject: "orders.created".into(),
        payload: Bytes::from_static(b"payload"),
        reply: Some("_INBOX.reply".into()),
        seq: 9,
        headers: None,
    };
    assert_eq!(message.subject, "orders.created");
    assert_eq!(message.seq, 9);
    assert_eq!(message.payload.as_ref(), b"payload");
    assert_eq!(message.reply.as_deref(), Some("_INBOX.reply"));

    let health = NatsHealth {
        connected: true,
        server: "nats-1".into(),
        rtt_ms: 0.75,
        jetstream: true,
        detail: "flush ok".into(),
    };
    assert!(health.connected);
    assert!(health.jetstream);

    let stats = NatsPoolStats {
        published: 10,
        publish_failed: 1,
        closed: false,
        connected: 1,
        disconnected: 0,
        slow_consumers: 2,
    };
    assert_eq!(stats.published, 10);
    assert_eq!(stats.slow_consumers, 2);
    assert_eq!(NatsPoolStats::default().published, 0);

    let metadata = JetStreamDeliveryMetadata {
        stream: "ORDERS".into(),
        consumer: "worker".into(),
        stream_sequence: 1,
        consumer_sequence: 2,
        delivery_attempts: 1,
        pending: 0,
    };
    assert_eq!(metadata.delivery_attempts, 1);
    assert_eq!(metadata.pending, 0);

    let stream = StreamConfig::new("ORDERS", "orders.>");
    assert_eq!(stream.name, "ORDERS");
    assert_eq!(stream.max_messages, 10_000);
    let multi = StreamConfig::with_subjects("EVENTS", ["a", "b"], 100);
    assert_eq!(multi.subjects, vec!["a".to_string(), "b".to_string()]);

    let pull = PullConsumerConfig::durable("worker").filter("orders.created");
    assert_eq!(pull.filter_subject.as_deref(), Some("orders.created"));

    let durable = JetStreamConsumerConfig::durable("worker");
    assert_eq!(durable.durable_name.as_deref(), Some("worker"));
    assert_eq!(durable.ack_wait, Duration::from_secs(30));
    let ephemeral = JetStreamConsumerConfig::ephemeral();
    assert!(ephemeral.durable_name.is_none());
}

#[test]
fn error_classification_is_exposed() {
    let config_error = NatsError::config("url 不能为空");
    assert!(!config_error.is_retryable());
    assert_eq!(config_error.kind_name(), "config");
    assert!(config_error.to_string().contains("配置无效"));

    let connection_error = NatsError::connection("dns 失败");
    assert!(connection_error.is_retryable());
    assert_eq!(connection_error.kind_name(), "connection");

    let timeout_error = NatsError::timeout("超时");
    assert!(timeout_error.is_retryable());

    let backend_error = NatsError::backend("broker 拒绝");
    assert!(!backend_error.is_retryable());

    let serialization_error = NatsError::serialization("json 非法");
    assert!(!serialization_error.is_retryable());

    let unsupported = NatsError::unsupported("未实现");
    assert!(!unsupported.is_retryable());

    let io_error: NatsError = std::io::Error::other("io").into();
    assert!(io_error.is_retryable());
    assert_eq!(io_error.kind_name(), "io");

    // 错误可实现为 Error trait 对象
    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(NatsError::config("x"));
    assert!(boxed.to_string().contains("配置无效"));
}

#[test]
fn connection_state_predicates_are_observable() {
    let pool = NatsPool::new(NatsConfig::default()).expect("构造");
    assert!(!pool.is_connected());
    assert!(url_is_loopback(pool.config().url.as_str()));
    assert_eq!(TlsPolicy::default(), TlsPolicy::Prefer);
}
