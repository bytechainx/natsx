//! JetStream 单元测试：配置默认值与边界、stream 校验、投递元数据与截止时间。
//!
//! 由本模块的 `#[cfg(test)] mod tests;` 引入，仅在测试构建中编译。

#[cfg(test)]
use super::*;

#[test]
fn stream_and_consumer_type_defaults() {
    let stream = StreamConfig::new("ORDERS", "orders.>");
    assert_eq!(stream.name, "ORDERS");
    assert_eq!(stream.subjects, vec!["orders.>".to_string()]);
    assert_eq!(stream.max_messages, 10_000);

    let multi = StreamConfig::with_subjects("EVENTS", ["events.a", "events.b"], 500);
    assert_eq!(multi.subjects.len(), 2);
    assert_eq!(multi.max_messages, 500);

    let pull = PullConsumerConfig::durable("worker-1").filter("orders.created");
    assert_eq!(pull.durable_name, "worker-1");
    assert_eq!(pull.filter_subject.as_deref(), Some("orders.created"));

    let durable = JetStreamConsumerConfig::durable("worker-3").filter("orders.created");
    assert_eq!(durable.max_deliver, 5);
    assert_eq!(durable.max_ack_pending, 1_024);
    assert_eq!(durable.command_timeout, Duration::from_secs(5));
    durable.validate().expect("默认 durable 配置必须有效");

    let ephemeral = JetStreamConsumerConfig::ephemeral();
    assert!(ephemeral.durable_name.is_none());
    ephemeral.validate().expect("ephemeral 配置必须有效");
}

#[test]
fn consumer_config_rejects_unbounded_values() {
    let base = JetStreamConsumerConfig::durable("worker");

    let mut ack_wait = base.clone();
    ack_wait.ack_wait = Duration::ZERO;
    assert!(ack_wait.validate().is_err());

    let mut max_deliver = base.clone();
    max_deliver.max_deliver = 0;
    assert!(max_deliver.validate().is_err());

    let mut max_ack_pending = base.clone();
    max_ack_pending.max_ack_pending = 0;
    assert!(max_ack_pending.validate().is_err());

    let mut command_timeout = base.clone();
    command_timeout.command_timeout = Duration::ZERO;
    assert!(command_timeout.validate().is_err());

    let mut empty_filter = base.clone();
    empty_filter.filter_subject = Some("   ".into());
    assert!(empty_filter.validate().is_err());

    let mut bad_name = base;
    bad_name.durable_name = Some("bad.name".into());
    assert!(bad_name.validate().is_err());
}

#[test]
fn stream_create_validation_boundaries() {
    validate_stream_create(&StreamConfig::new("ORDERS", "orders.>")).expect("合法 stream 配置");

    let cases = [
        StreamConfig {
            name: "bad.name".into(),
            subjects: vec!["s".into()],
            max_messages: 1,
        },
        StreamConfig {
            name: "S".into(),
            subjects: vec![],
            max_messages: 1,
        },
        StreamConfig {
            name: "S".into(),
            subjects: vec!["   ".into()],
            max_messages: 1,
        },
        StreamConfig {
            name: "S".into(),
            subjects: vec!["s".into()],
            max_messages: 0,
        },
    ];
    for config in cases {
        assert!(
            validate_stream_create(&config).is_err(),
            "必须拒绝: {config:?}"
        );
    }
}

#[test]
fn delivery_metadata_is_constructible_and_debug_safe() {
    let metadata = JetStreamDeliveryMetadata {
        stream: "ORDERS".into(),
        consumer: "worker".into(),
        stream_sequence: 11,
        consumer_sequence: 22,
        delivery_attempts: 3,
        pending: 4,
    };
    assert_eq!(metadata.stream, "ORDERS");
    assert_eq!(metadata.consumer, "worker");
    assert_eq!(metadata.stream_sequence, 11);
    assert_eq!(metadata.consumer_sequence, 22);
    assert_eq!(metadata.delivery_attempts, 3);
    assert_eq!(metadata.pending, 4);
    let debug = format!("{metadata:?}");
    assert!(debug.contains("ORDERS"));
    assert!(debug.contains("worker"));
}

#[tokio::test]
async fn bounded_command_maps_timeout_without_panicking() {
    let value = run_bounded_command(Duration::from_secs(1), "返回值", async { Ok(42u8) })
        .await
        .expect("返回值");
    assert_eq!(value, 42);

    let error = run_bounded_command(
        Duration::from_millis(1),
        "挂起指令",
        std::future::pending::<NatsResult<u8>>(),
    )
    .await
    .expect_err("挂起指令必须按截止时间失败");
    assert!(matches!(error, NatsError::Timeout(_)));
    assert!(error.to_string().contains("挂起指令"));
}

#[test]
fn jetstream_requires_connected_pool() {
    let pool = crate::NatsPool::new(crate::NatsConfig::default()).expect("构造");
    let error = JetStream::from_pool(&pool).expect_err("未连接池不可构造 JetStream");
    assert!(matches!(error, NatsError::Connection(_)));
}

#[test]
fn with_operation_timeout_rejects_zero() {
    assert!(validate_operation_timeout(Duration::ZERO).is_err());
    assert!(validate_operation_timeout(Duration::from_millis(1)).is_ok());
}
