//! [`JetStream`] 的流与消费者管理面实现（建/查/删/清流、建消费者、发布）。
//!
//! 从门面 `jetstream.rs` 下沉（`MR-STRUCT-007` 腾余量）。结构定义与 `Debug` 留在门面；
//! 搬走的 15 个方法中只有 `stream_config` 是私有（仅本模块内用），其余全是 `pub`，
//! 故**无需任何可见性调整**。`run_bounded_command` / `validate_stream_create` 是门面的
//! 私有辅助，子模块可直接调用。

use std::time::Duration;

use bytes::Bytes;

use crate::error::{NatsError, NatsResult};
use crate::pool::NatsPool;
use crate::validation::{validate_consumer_name, validate_operation_timeout, validate_stream_name};

use super::{
    run_bounded_command, validate_stream_create, JetStream, JetStreamConsumer,
    JetStreamConsumerConfig, PullConsumerConfig, StreamConfig, StreamInfo,
};

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
