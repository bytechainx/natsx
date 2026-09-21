#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 纯函数行为：loopback 判定、subject / stream / consumer 名校验边界与超时校验。
//!
//! 这些函数不发起任何 IO，是 fail-closed 的第一道关卡。

use std::time::Duration;

use natsx::{
    url_is_loopback, validate_consumer_name, validate_operation_timeout, validate_publish_subject,
    validate_stream_name, validate_subject, JetStreamConsumerConfig, NatsError, PullConsumerConfig,
    StreamConfig, TlsPolicy,
};

#[test]
fn url_is_loopback_boundaries() {
    for loopback in [
        "nats://127.0.0.1:4222",
        "nats://localhost:4222",
        "nats://[::1]:4222",
        "tls://LOCALHOST:4222",
        "nats://127.0.0.1",
        "  nats://127.0.0.1:4222  ",
        "nats://user:pass@localhost:4222",
        "nats://127.0.0.1:4222,nats://10.0.0.1:4222",
    ] {
        assert!(url_is_loopback(loopback), "{loopback} 应判定为 loopback");
    }

    for remote in [
        "nats://10.0.0.5:4222",
        "nats://0.0.0.0:4222",
        "tls://nats.prod.internal:4222",
        "nats://broker.example.com:4222",
        "nats://192.168.1.10:4222",
        "nats://[2001:db8::1]:4222",
    ] {
        assert!(!url_is_loopback(remote), "{remote} 应判定为非 loopback");
    }
}

#[test]
fn tls_policy_parsing_and_display() {
    assert_eq!(
        TlsPolicy::parse("require").expect("require"),
        TlsPolicy::Require
    );
    assert_eq!(
        TlsPolicy::parse("  REQUIRED ").expect("required"),
        TlsPolicy::Require
    );
    assert_eq!(
        TlsPolicy::parse("mandatory").expect("mandatory"),
        TlsPolicy::Require
    );
    assert_eq!(
        TlsPolicy::parse("prefer").expect("prefer"),
        TlsPolicy::Prefer
    );
    assert_eq!(TlsPolicy::parse("auto").expect("auto"), TlsPolicy::Prefer);
    assert_eq!(
        TlsPolicy::parse("disable").expect("disable"),
        TlsPolicy::Disable
    );
    assert_eq!(TlsPolicy::parse("off").expect("off"), TlsPolicy::Disable);
    assert!(TlsPolicy::parse("weird").is_err());

    assert!(TlsPolicy::Require.require_tls());
    assert!(!TlsPolicy::Prefer.require_tls());
    assert!(!TlsPolicy::Disable.require_tls());

    assert_eq!(TlsPolicy::Require.to_string(), "require");
    assert_eq!(TlsPolicy::Prefer.to_string(), "prefer");
    assert_eq!(TlsPolicy::Disable.to_string(), "disable");
    assert_eq!(TlsPolicy::default(), TlsPolicy::Prefer);
}

#[test]
fn subject_validation_boundaries() {
    assert!(validate_subject("orders.created").is_ok());
    assert!(validate_subject("orders.*").is_ok());
    assert!(validate_subject("orders.>").is_ok());

    for invalid in [
        "",
        "   ",
        "orders created",
        "orders\tcreated",
        "orders\ncreated",
    ] {
        let error = validate_subject(invalid).expect_err("非法 subject");
        assert!(matches!(error, NatsError::Config(_)));
    }

    assert!(validate_publish_subject("orders.created").is_ok());
    for invalid in ["orders.*", "orders.>", "*", ">", ""] {
        assert!(
            validate_publish_subject(invalid).is_err(),
            "{invalid:?} 不可发布"
        );
    }
}

#[test]
fn stream_and_consumer_name_boundaries() {
    for valid in ["EVENTS", "stream_name", "S1", "A", "worker_99", "orders123"] {
        validate_stream_name(valid).expect("合法 stream 名");
        validate_consumer_name(valid).expect("合法 consumer 名");
    }

    for invalid in [
        "",
        "bad.name",
        "bad*name",
        "bad>name",
        "has space",
        "\tbad",
        "bad\n",
        "a.b.c",
        "  ",
    ] {
        let stream_error = validate_stream_name(invalid).expect_err("非法 stream 名");
        assert!(matches!(stream_error, NatsError::Config(_)));
        assert!(stream_error.to_string().contains("stream 名"));

        let consumer_error = validate_consumer_name(invalid).expect_err("非法 consumer 名");
        assert!(matches!(consumer_error, NatsError::Config(_)));
        assert!(consumer_error.to_string().contains("consumer 名非法"));
    }

    // 命名规则的错误消息区分“非法字符”与“空值”
    let illegal = validate_stream_name("bad.name").expect_err("非法字符");
    assert!(illegal.to_string().contains("非法"));
    let empty = validate_stream_name("").expect_err("空值");
    assert!(empty.to_string().contains("不能为空"));
}

#[test]
fn operation_timeout_validation_boundaries() {
    assert!(validate_operation_timeout(Duration::from_nanos(1)).is_ok());
    assert!(validate_operation_timeout(Duration::from_secs(60)).is_ok());

    let error = validate_operation_timeout(Duration::ZERO).expect_err("零超时必须拒绝");
    assert!(matches!(error, NatsError::Config(_)));
    assert!(error.to_string().contains("operation_timeout"));
    assert!(!error.is_retryable());
}

#[test]
fn consumer_configs_validate_durable_names() {
    // durable 名称走同一套命名规则
    assert!(JetStreamConsumerConfig::durable("worker_1")
        .validate()
        .is_ok());
    assert!(JetStreamConsumerConfig::durable("bad.name")
        .validate()
        .is_err());

    let pull = PullConsumerConfig::durable("bad*name");
    assert!(validate_consumer_name(&pull.durable_name).is_err());
    let pull = PullConsumerConfig::durable("worker_2");
    validate_consumer_name(&pull.durable_name).expect("合法 durable 名");

    // ephemeral 无 durable 名也合法
    assert!(JetStreamConsumerConfig::ephemeral().validate().is_ok());

    // filter subject 只要求非空
    assert!(JetStreamConsumerConfig::durable("w")
        .filter("orders.>")
        .validate()
        .is_ok());
    assert!(JetStreamConsumerConfig::durable("w")
        .filter("   ")
        .validate()
        .is_err());

    // 边界值
    let mut config = JetStreamConsumerConfig::durable("worker");
    config.ack_wait = Duration::ZERO;
    assert!(config.validate().is_err());
    config.ack_wait = Duration::from_secs(30);
    config.max_deliver = 0;
    assert!(config.validate().is_err());
    config.max_deliver = 1;
    config.max_ack_pending = 0;
    assert!(config.validate().is_err());
    config.max_ack_pending = 1;
    config.command_timeout = Duration::ZERO;
    assert!(config.validate().is_err());
    config.command_timeout = Duration::from_secs(1);
    config.validate().expect("恢复默认边界后应合法");
}

#[test]
fn stream_config_helpers_build_expected_values() {
    let single = StreamConfig::new("ORDERS", "orders.>");
    assert_eq!(single.name, "ORDERS");
    assert_eq!(single.subjects, vec!["orders.>".to_string()]);
    assert_eq!(single.max_messages, 10_000);

    let multi = StreamConfig::with_subjects("EVENTS", ["events.created", "events.updated"], 500);
    assert_eq!(multi.name, "EVENTS");
    assert_eq!(multi.subjects.len(), 2);
    assert_eq!(multi.max_messages, 500);

    let pull = PullConsumerConfig::durable("worker").filter("orders.created");
    assert_eq!(pull.durable_name, "worker");
    assert_eq!(pull.filter_subject.as_deref(), Some("orders.created"));

    let durable = JetStreamConsumerConfig::durable("worker").filter("orders.created");
    assert_eq!(durable.durable_name.as_deref(), Some("worker"));
    assert_eq!(durable.filter_subject.as_deref(), Some("orders.created"));
    assert_eq!(durable.ack_wait, Duration::from_secs(30));
    assert_eq!(durable.max_deliver, 5);
    assert_eq!(durable.max_ack_pending, 1_024);
    assert_eq!(durable.command_timeout, Duration::from_secs(5));
}
