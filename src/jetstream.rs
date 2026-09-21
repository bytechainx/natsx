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

use bytes::Bytes;
use futures_util::StreamExt;

use crate::error::{NatsError, NatsResult};
use crate::pool::NatsPool;
use crate::validation::{validate_consumer_name, validate_operation_timeout, validate_stream_name};

mod delivery;
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

impl JetStreamConsumer {
    /// 有限等待下一条消息。
    ///
    /// 服务端 fetch expiry 正常结束返回 `Ok(None)`；broker/协议错误返回 `Err`。
    /// 客户端硬超时比服务端 expiry 多一秒，仅用于阻止连接异常时无限挂起。
    ///
    /// # Errors
    ///
    /// `timeout` 为零、fetch 创建失败、broker 返回错误或服务端未按期终止时返回错误。
    pub async fn next_timeout(&self, timeout: Duration) -> NatsResult<Option<JetStreamDelivery>> {
        if timeout.is_zero() {
            return Err(NatsError::config("jetstream: fetch timeout 必须大于零"));
        }
        let client_deadline = timeout.saturating_add(Duration::from_secs(1));
        let batch = self
            .inner
            .fetch()
            .max_messages(1)
            .expires(timeout)
            .messages();
        let mut batch = tokio::time::timeout(client_deadline, batch)
            .await
            .map_err(|_| NatsError::timeout("jetstream: 创建有限 fetch 超时"))?
            .map_err(|error| {
                NatsError::backend(format!("jetstream: 创建有限 fetch 失败: {error}"))
            })?;
        match tokio::time::timeout(client_deadline, batch.next()).await {
            Ok(Some(Ok(message))) => {
                JetStreamDelivery::from_raw(message, self.command_timeout).map(Some)
            }
            Ok(Some(Err(error))) => Err(NatsError::backend(format!(
                "jetstream: 拉取消息失败: {error}"
            ))),
            Ok(None) => Ok(None),
            Err(_) => Err(NatsError::timeout(
                "jetstream: 服务端未在 fetch expiry 后终止批次",
            )),
        }
    }

    /// 批量拉取最多 `max_messages` 条消息（受 `timeout` 与服务端 expiry 约束）。
    ///
    /// # Errors
    ///
    /// `timeout` 或 `max_messages` 为零、fetch 创建或消息接收失败时返回错误。
    pub async fn next_batch(
        &self,
        timeout: Duration,
        max_messages: usize,
    ) -> NatsResult<Vec<JetStreamDelivery>> {
        if timeout.is_zero() {
            return Err(NatsError::config(
                "jetstream: next_batch timeout 必须大于零",
            ));
        }
        if max_messages == 0 {
            return Err(NatsError::config(
                "jetstream: next_batch max_messages 必须大于零",
            ));
        }
        let client_deadline = timeout.saturating_add(Duration::from_secs(1));
        let batch = self
            .inner
            .fetch()
            .max_messages(max_messages)
            .expires(timeout)
            .messages();
        let mut batch = tokio::time::timeout(client_deadline, batch)
            .await
            .map_err(|_| NatsError::timeout("jetstream: 创建批次 fetch 超时"))?
            .map_err(|error| {
                NatsError::backend(format!("jetstream: 创建批次 fetch 失败: {error}"))
            })?;
        let mut deliveries = Vec::with_capacity(max_messages);
        loop {
            let next = tokio::time::timeout(client_deadline, batch.next())
                .await
                .map_err(|_| NatsError::timeout("jetstream: 批次消息接收超时"))?;
            match next {
                Some(Ok(message)) => {
                    deliveries.push(JetStreamDelivery::from_raw(message, self.command_timeout)?);
                }
                Some(Err(error)) => {
                    return Err(NatsError::backend(format!(
                        "jetstream: 批次拉取消息失败: {error}"
                    )));
                }
                None => break,
            }
        }
        Ok(deliveries)
    }

    /// 获取 consumer 信息（触发服务端往返）。
    ///
    /// # Errors
    ///
    /// 连接不可用或超时时返回错误。
    pub async fn info(&self) -> NatsResult<async_nats::jetstream::consumer::Info> {
        let mut inner = self.inner.clone();
        run_bounded_command(self.command_timeout, "consumer_info", async move {
            inner.info().await.cloned().map_err(|error| {
                NatsError::backend(format!("jetstream: consumer_info 失败: {error}"))
            })
        })
        .await
    }

    /// 获取 consumer 待投递消息数。
    ///
    /// # Errors
    ///
    /// 同 [`JetStreamConsumer::info`]。
    pub async fn pending(&self) -> NatsResult<u64> {
        Ok(self.info().await?.num_pending)
    }
}

impl JetStream {
    /// 从已连接的 [`NatsPool`] 构造。
    ///
    /// # Errors
    ///
    /// 连接池尚未连接时返回 [`NatsError::Connection`]。
    pub fn from_pool(pool: &NatsPool) -> NatsResult<Self> {
        let client = pool
            .client()
            .ok_or_else(|| NatsError::connection("连接池尚未连接，无法构造 JetStream"))?;
        Ok(Self {
            context: async_nats::jetstream::new(client),
            operation_timeout: pool.config().operation_timeout,
        })
    }

    /// 从裸客户端构造（操作超时默认 5 秒）。
    #[must_use]
    pub fn from_client(client: async_nats::Client) -> Self {
        Self {
            context: async_nats::jetstream::new(client),
            operation_timeout: Duration::from_secs(5),
        }
    }

    /// 底层 `async-nats` JetStream 上下文（高级逃生口）。
    #[must_use]
    pub fn context(&self) -> &async_nats::jetstream::Context {
        &self.context
    }

    /// 当前生效的操作截止时间。
    #[must_use]
    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// 覆盖 JetStream 管理与发布操作的调用侧截止时间。
    ///
    /// # Errors
    ///
    /// `timeout` 为零时返回 [`NatsError::Config`]。
    pub fn with_operation_timeout(mut self, timeout: Duration) -> NatsResult<Self> {
        validate_operation_timeout(timeout)?;
        self.operation_timeout = timeout;
        Ok(self)
    }

    /// 以 JSON 序列化后发布并等待 ack。
    ///
    /// # Errors
    ///
    /// 序列化失败返回 [`NatsError::Serialization`]；其余同 [`JetStream::publish`]。
    pub async fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        subject: &str,
        value: &T,
    ) -> NatsResult<()> {
        let payload = serde_json::to_vec(value)
            .map_err(|error| NatsError::serialization(format!("JSON 序列化失败: {error}")))?;
        self.publish(subject, payload).await
    }

    /// 发布消息并等待 JetStream ack。
    ///
    /// # Errors
    ///
    /// subject 非法、发布失败、等待 ack 失败或超时时返回错误。
    pub async fn publish(&self, subject: &str, payload: impl Into<Bytes>) -> NatsResult<()> {
        if subject.trim().is_empty() {
            return Err(NatsError::config("jetstream: subject 不能为空"));
        }
        let payload = payload.into();
        let ack = run_bounded_command(self.operation_timeout, "publish", async {
            self.context
                .publish(subject.to_string(), payload)
                .await
                .map_err(|error| NatsError::backend(format!("jetstream publish 失败: {error}")))
        })
        .await?;
        run_bounded_command(self.operation_timeout, "publish ack", async {
            ack.await
                .map(|_| ())
                .map_err(|error| NatsError::backend(format!("jetstream publish ack 失败: {error}")))
        })
        .await
    }

    /// 创建 stream（已存在则失败）。
    ///
    /// # Errors
    ///
    /// 配置非法或服务端拒绝时返回错误。
    pub async fn create_stream(&self, config: StreamConfig) -> NatsResult<()> {
        let js_config = self.stream_config(&config)?;
        run_bounded_command(self.operation_timeout, "create_stream", async {
            self.context
                .create_stream(js_config)
                .await
                .map(|_| ())
                .map_err(|error| {
                    NatsError::backend(format!("jetstream create_stream 失败: {error}"))
                })
        })
        .await
    }

    /// 创建或获取 stream（幂等入口）。
    ///
    /// # Errors
    ///
    /// 配置非法或服务端拒绝时返回错误。
    pub async fn get_or_create_stream(&self, config: StreamConfig) -> NatsResult<()> {
        let js_config = self.stream_config(&config)?;
        run_bounded_command(self.operation_timeout, "get_or_create_stream", async {
            self.context
                .get_or_create_stream(js_config)
                .await
                .map(|_| ())
                .map_err(|error| {
                    NatsError::backend(format!("jetstream get_or_create_stream 失败: {error}"))
                })
        })
        .await
    }

    /// 查询 stream 概要信息。
    ///
    /// # Errors
    ///
    /// stream 名非法、stream 不存在或查询超时时返回错误。
    pub async fn get_stream(&self, stream: &str) -> NatsResult<StreamInfo> {
        validate_stream_name(stream)?;
        let handle = run_bounded_command(self.operation_timeout, "get_stream", async {
            self.context
                .get_stream(stream)
                .await
                .map_err(|error| NatsError::backend(format!("jetstream get_stream 失败: {error}")))
        })
        .await?;
        let info = handle.cached_info().clone();
        Ok(StreamInfo {
            name: info.config.name.clone(),
            subjects: info.config.subjects.clone(),
            messages: info.state.messages,
            bytes: info.state.bytes,
            consumers: info.state.consumer_count,
        })
    }

    /// 删除 stream。
    ///
    /// # Errors
    ///
    /// stream 名非法、stream 不存在或删除失败时返回错误。
    pub async fn delete_stream(&self, stream: &str) -> NatsResult<()> {
        validate_stream_name(stream)?;
        run_bounded_command(self.operation_timeout, "delete_stream", async {
            self.context
                .delete_stream(stream)
                .await
                .map(|_| ())
                .map_err(|error| {
                    NatsError::backend(format!("jetstream delete_stream 失败: {error}"))
                })
        })
        .await
    }

    /// 清空 stream 中的全部消息（保留 stream 与 consumer 配置）。
    ///
    /// # Errors
    ///
    /// stream 名非法、purge 失败或超时时返回错误。
    pub async fn purge_stream(&self, stream: &str) -> NatsResult<()> {
        validate_stream_name(stream)?;
        let handle = run_bounded_command(self.operation_timeout, "get_stream", async {
            self.context
                .get_stream(stream)
                .await
                .map_err(|error| NatsError::backend(format!("jetstream get_stream 失败: {error}")))
        })
        .await?;
        run_bounded_command(self.operation_timeout, "purge_stream", async {
            handle.purge().await.map(|_| ()).map_err(|error| {
                NatsError::backend(format!("jetstream purge_stream 失败: {error}"))
            })
        })
        .await
    }

    /// 创建 / 更新 legacy pull consumer（durable）。
    ///
    /// # Errors
    ///
    /// stream 名或 durable 名为空、broker 拒绝时返回错误。
    pub async fn create_pull_consumer(
        &self,
        stream: &str,
        config: PullConsumerConfig,
    ) -> NatsResult<()> {
        validate_stream_name(stream)?;
        validate_consumer_name(&config.durable_name)?;
        let mut pull = async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(config.durable_name),
            ..Default::default()
        };
        if let Some(filter_subject) = config.filter_subject {
            pull.filter_subject = filter_subject;
        }
        run_bounded_command(self.operation_timeout, "create_pull_consumer", async {
            self.context
                .create_consumer_on_stream(pull, stream)
                .await
                .map(|_| ())
                .map_err(|error| {
                    NatsError::backend(format!("jetstream create_pull_consumer 失败: {error}"))
                })
        })
        .await
    }

    /// 创建或更新显式确认 consumer，并返回稳定消费面。
    ///
    /// # Errors
    ///
    /// stream / consumer 配置非法，或 broker 创建 consumer 失败、超时时返回错误。
    pub async fn consumer(
        &self,
        stream: &str,
        config: JetStreamConsumerConfig,
    ) -> NatsResult<JetStreamConsumer> {
        validate_stream_name(stream)?;
        config.validate()?;
        let command_timeout = config.command_timeout;
        let pull = async_nats::jetstream::consumer::pull::Config {
            durable_name: config.durable_name,
            filter_subject: config.filter_subject.unwrap_or_default(),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ack_wait: config.ack_wait,
            max_deliver: config.max_deliver,
            max_ack_pending: config.max_ack_pending,
            ..Default::default()
        };
        let inner = tokio::time::timeout(
            command_timeout,
            self.context.create_consumer_on_stream(pull, stream),
        )
        .await
        .map_err(|_| NatsError::timeout("jetstream: 创建持久 consumer 超时"))?
        .map_err(|error| {
            NatsError::backend(format!("jetstream: 创建持久 consumer 失败: {error}"))
        })?;
        Ok(JetStreamConsumer {
            inner,
            command_timeout,
        })
    }

    /// 获取已有 pull consumer 的底层句柄（高级逃生口）。
    ///
    /// 普通调用方应使用 [`JetStream::consumer`]，由稳定包装面统一有限等待与确认语义。
    ///
    /// # Errors
    ///
    /// stream / consumer 名非法、stream 或 consumer 不存在、超时时返回错误。
    pub async fn get_pull_consumer(
        &self,
        stream: &str,
        consumer: &str,
    ) -> NatsResult<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    > {
        validate_stream_name(stream)?;
        validate_consumer_name(consumer)?;
        let stream_handle = run_bounded_command(self.operation_timeout, "get_stream", async {
            self.context
                .get_stream(stream)
                .await
                .map_err(|error| NatsError::backend(format!("jetstream get_stream 失败: {error}")))
        })
        .await?;
        run_bounded_command(self.operation_timeout, "get_consumer", async {
            stream_handle.get_consumer(consumer).await.map_err(|error| {
                NatsError::backend(format!("jetstream get_consumer 失败: {error}"))
            })
        })
        .await
    }

    /// 校验并把 [`StreamConfig`] 转换为底层配置。
    fn stream_config(
        &self,
        config: &StreamConfig,
    ) -> NatsResult<async_nats::jetstream::stream::Config> {
        validate_stream_create(config)?;
        Ok(async_nats::jetstream::stream::Config {
            name: config.name.clone(),
            subjects: config.subjects.clone(),
            max_messages: config.max_messages,
            ..Default::default()
        })
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
