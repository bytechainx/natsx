//! JetStream 投递：稳定元数据与显式确认面（`ack` / `nak` / `progress` / `term`）。
//!
//! [`JetStreamDelivery`] 由底层投递消息转换而来，每个终结动作都受调用侧
//! `command_timeout` 约束；`term` **不是** DLQ，只终止重投。

use std::time::Duration;

use bytes::Bytes;

use super::run_bounded_command;
use crate::error::{NatsError, NatsResult};

/// 一次 JetStream 投递的稳定元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JetStreamDeliveryMetadata {
    /// stream 名。
    pub stream: String,
    /// consumer 名。
    pub consumer: String,
    /// stream 内序号；重投时保持不变。
    pub stream_sequence: u64,
    /// consumer 投递序号。
    pub consumer_sequence: u64,
    /// 当前消息已投递次数。
    pub delivery_attempts: u64,
    /// 服务端报告的待投递数。
    pub pending: u64,
}

/// 一次可显式确认的 JetStream 投递。
///
/// `Debug` 不输出 payload，避免业务数据进入日志。
pub struct JetStreamDelivery {
    subject: String,
    payload: Bytes,
    metadata: JetStreamDeliveryMetadata,
    raw: async_nats::jetstream::Message,
    command_timeout: Duration,
}

impl std::fmt::Debug for JetStreamDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamDelivery")
            .field("subject", &self.subject)
            .field("payload_len", &self.payload.len())
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl JetStreamDelivery {
    pub(super) fn from_raw(
        raw: async_nats::jetstream::Message,
        command_timeout: Duration,
    ) -> NatsResult<Self> {
        let info = raw.info().map_err(|error| {
            NatsError::backend(format!(
                "jetstream: 投递缺少合法的 JetStream 元数据: {error}"
            ))
        })?;
        let delivery_attempts = u64::try_from(info.delivered)
            .map_err(|_| NatsError::backend("jetstream: delivery attempts 不能为负"))?;
        let metadata = JetStreamDeliveryMetadata {
            stream: info.stream.to_string(),
            consumer: info.consumer.to_string(),
            stream_sequence: info.stream_sequence,
            consumer_sequence: info.consumer_sequence,
            delivery_attempts,
            pending: info.pending,
        };
        Ok(Self {
            subject: raw.subject.to_string(),
            payload: raw.payload.clone(),
            metadata,
            raw,
            command_timeout,
        })
    }

    /// subject。
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// 消息 payload。
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// 稳定、已复制的投递元数据。
    #[must_use]
    pub fn metadata(&self) -> &JetStreamDeliveryMetadata {
        &self.metadata
    }

    /// 发送确认；消费 `self`，避免同一句柄重复终结。
    ///
    /// # Errors
    ///
    /// broker 不可用或确认发送失败返回 [`NatsError::Connection`]；
    /// 超过配置的 `command_timeout` 返回 [`NatsError::Timeout`]。
    pub async fn ack(self) -> NatsResult<()> {
        let timeout = self.command_timeout;
        run_bounded_command(timeout, "ack", async move {
            self.raw
                .ack()
                .await
                .map_err(|error| NatsError::connection(format!("jetstream: ack 发送失败: {error}")))
        })
        .await
    }

    /// 发送确认并等待服务端确认（dual-ack）；消费 `self`。
    ///
    /// # Errors
    ///
    /// 同 [`JetStreamDelivery::ack`]。
    pub async fn double_ack(self) -> NatsResult<()> {
        let timeout = self.command_timeout;
        run_bounded_command(timeout, "double_ack", async move {
            self.raw.double_ack().await.map_err(|error| {
                NatsError::connection(format!("jetstream: double_ack 失败: {error}"))
            })
        })
        .await
    }

    /// 请求重投（可带延迟）；消费 `self`。
    ///
    /// # Errors
    ///
    /// 同 [`JetStreamDelivery::ack`]。
    pub async fn nak(self, delay: Option<Duration>) -> NatsResult<()> {
        let timeout = self.command_timeout;
        run_bounded_command(timeout, "nak", async move {
            self.raw
                .ack_with(async_nats::jetstream::AckKind::Nak(delay))
                .await
                .map_err(|error| NatsError::connection(format!("jetstream: nak 发送失败: {error}")))
        })
        .await
    }

    /// 通知服务端处理仍在进行，延长 ack wait。
    ///
    /// # Errors
    ///
    /// 同 [`JetStreamDelivery::ack`]。
    pub async fn progress(&self) -> NatsResult<()> {
        run_bounded_command(self.command_timeout, "progress", async {
            self.raw
                .ack_with(async_nats::jetstream::AckKind::Progress)
                .await
                .map_err(|error| {
                    NatsError::connection(format!("jetstream: progress 发送失败: {error}"))
                })
        })
        .await
    }

    /// 终止该消息后续重投；消费 `self`。
    ///
    /// `term` **不是 DLQ**：不会自动把 payload 发布到隔离 subject。
    ///
    /// # Errors
    ///
    /// 同 [`JetStreamDelivery::ack`]。
    pub async fn term(self) -> NatsResult<()> {
        let timeout = self.command_timeout;
        run_bounded_command(timeout, "term", async move {
            self.raw
                .ack_with(async_nats::jetstream::AckKind::Term)
                .await
                .map_err(|error| {
                    NatsError::connection(format!("jetstream: term 发送失败: {error}"))
                })
        })
        .await
    }
}
