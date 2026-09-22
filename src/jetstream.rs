//! JetStream 薄封装：持久发布（带 ack）、stream 管理、显式确认 pull consumer。
//!
//! # 覆盖范围
//!
//! - 从 [`NatsPool`] 或裸客户端构造 [`JetStream`] 上下文；
//! - `publish` / `publish_json`：发布并等待服务端 ack；
//! - stream：[`StreamConfig`] 创建 / 获取 / 删除 / purge 与信息查询；
//! - consumer：[`JetStreamConsumerConfig`]（durable 或 ephemeral）与
//!   [`PullConsumerConfig`]（legacy 简化形态），durable 名称统一校验；
//! - 消费：[`JetStreamConsumer`] 的有限拉取（单条 / 批量）、
//!   [`JetStreamDelivery`] 的 `ack` / `nak` / `progress` / `term`，以及
//!   stream / consumer / 投递次数等稳定元数据。
//!
//! Cluster 拓扑、跨账户、ObjectStore、KeyValue **不在**本 crate 的稳定承诺内。
//!
//! # 语义提醒
//!
//! - `term` **不是** DLQ：只终止重投，不会把消息搬到隔离 subject；
//! - 所有 broker 指令都受调用侧截止时间约束（`command_timeout`），
//!   超时映射为 [`crate::NatsError::Timeout`]，不会无限挂起。

use std::future::Future;
use std::time::Duration;

use crate::error::{NatsError, NatsResult};
use crate::validation::validate_stream_name;

mod consumer;
mod delivery;
mod operations;
mod types;

pub use delivery::{JetStreamDelivery, JetStreamDeliveryMetadata};
pub use types::{JetStreamConsumerConfig, PullConsumerConfig, StreamConfig, StreamInfo};

/// JetStream 上下文包装（可克隆，内部共享 `async-nats` 上下文）。
#[derive(Clone)]
pub struct JetStream {
    context: async_nats::jetstream::Context,
    operation_timeout: Duration,
}

impl std::fmt::Debug for JetStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStream")
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

/// JetStream pull consumer 的稳定消费面。
#[derive(Clone)]
pub struct JetStreamConsumer {
    inner: async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    command_timeout: Duration,
}

impl std::fmt::Debug for JetStreamConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamConsumer")
            .field("stream", &self.inner.cached_info().stream_name)
            .field("name", &self.inner.cached_info().name)
            .finish_non_exhaustive()
    }
}

/// 校验 stream 创建配置（纯函数，可离线测试）。
fn validate_stream_create(config: &StreamConfig) -> NatsResult<()> {
    validate_stream_name(&config.name)?;
    if config.subjects.is_empty() {
        return Err(NatsError::config("jetstream: stream subjects 不能为空"));
    }
    if config
        .subjects
        .iter()
        .any(|subject| subject.trim().is_empty())
    {
        return Err(NatsError::config("jetstream: stream subject 不能为空"));
    }
    if config.max_messages <= 0 {
        return Err(NatsError::config("jetstream: max_messages 必须大于零"));
    }
    Ok(())
}

/// 在给定截止时间内执行 broker 指令；超时统一映射为 [`NatsError::Timeout`]。
async fn run_bounded_command<T, F>(
    timeout: Duration,
    operation: &'static str,
    command: F,
) -> NatsResult<T>
where
    F: Future<Output = NatsResult<T>>,
{
    tokio::time::timeout(timeout, command)
        .await
        .map_err(|_| NatsError::timeout(format!("jetstream: {operation} 超时")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    // 只被本测试模块使用，故不放在门面顶层（否则非测试构建报 unused import）。
    use crate::validation::validate_operation_timeout;

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
}
