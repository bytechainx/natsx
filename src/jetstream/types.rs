//! JetStream 的公开配置与信息类型：stream 概要、stream 配置与 consumer 配置。
//!
//! 这些类型只承载数据与纯校验（[`JetStreamConsumerConfig::validate`]），
//! 不接触网络，因此可离线测试。

use std::time::Duration;

use crate::error::{NatsError, NatsResult};
use crate::validation::{validate_consumer_name, validate_operation_timeout};

/// stream 概要信息（不暴露底层 `async-nats` 类型）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    /// stream 名。
    pub name: String,
    /// 绑定的 subjects。
    pub subjects: Vec<String>,
    /// 当前消息数。
    pub messages: u64,
    /// 当前消息总字节数。
    pub bytes: u64,
    /// 已配置的 consumer 数。
    pub consumers: usize,
}

/// stream 创建配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConfig {
    /// stream 名。
    pub name: String,
    /// 绑定的 subjects。
    pub subjects: Vec<String>,
    /// 最大消息数（必须大于零）。
    pub max_messages: i64,
}

impl StreamConfig {
    /// 单 subject stream（默认保留 10_000 条消息）。
    #[must_use]
    pub fn new(name: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            subjects: vec![subject.into()],
            max_messages: 10_000,
        }
    }

    /// 多 subject stream。
    #[must_use]
    pub fn with_subjects<I, S>(name: impl Into<String>, subjects: I, max_messages: i64) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            subjects: subjects.into_iter().map(Into::into).collect(),
            max_messages,
        }
    }
}

/// legacy pull consumer 配置：仅 durable 名 + 可选 filter subject。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullConsumerConfig {
    /// durable 名（亦作 consumer name）。
    pub durable_name: String,
    /// 可选 filter subject。
    pub filter_subject: Option<String>,
}

impl PullConsumerConfig {
    /// 仅 durable 名。
    #[must_use]
    pub fn durable(name: impl Into<String>) -> Self {
        Self {
            durable_name: name.into(),
            filter_subject: None,
        }
    }

    /// 附加 filter subject。
    #[must_use]
    pub fn filter(mut self, subject: impl Into<String>) -> Self {
        self.filter_subject = Some(subject.into());
        self
    }
}

/// 显式确认的 JetStream consumer 配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JetStreamConsumerConfig {
    /// durable 名（`None` = ephemeral）。
    pub durable_name: Option<String>,
    /// 可选 filter subject。
    pub filter_subject: Option<String>,
    /// 未确认消息的重投等待时间。
    pub ack_wait: Duration,
    /// 单条消息最多投递次数；达到上限不会自动进入 DLQ。
    pub max_deliver: i64,
    /// consumer 允许的最大未确认消息数。
    pub max_ack_pending: i64,
    /// ack/nak/progress/term 等 broker 指令的调用侧截止时间。
    pub command_timeout: Duration,
}

impl JetStreamConsumerConfig {
    /// 以保守的显式确认默认值创建 durable consumer 配置。
    #[must_use]
    pub fn durable(name: impl Into<String>) -> Self {
        Self {
            durable_name: Some(name.into()),
            filter_subject: None,
            ack_wait: Duration::from_secs(30),
            max_deliver: 5,
            max_ack_pending: 1_024,
            command_timeout: Duration::from_secs(5),
        }
    }

    /// 创建 ephemeral（非持久）consumer 配置。
    #[must_use]
    pub fn ephemeral() -> Self {
        Self {
            durable_name: None,
            ..Self::durable("")
        }
    }

    /// 附加 filter subject。
    #[must_use]
    pub fn filter(mut self, subject: impl Into<String>) -> Self {
        self.filter_subject = Some(subject.into());
        self
    }

    /// 校验配置（durable 名、超时与边界值）。
    ///
    /// # Errors
    ///
    /// durable 名非法、`ack_wait` / `command_timeout` 为零、
    /// `max_deliver` / `max_ack_pending` 非正或 `filter_subject` 为空时返回错误。
    pub fn validate(&self) -> NatsResult<()> {
        if let Some(name) = &self.durable_name {
            validate_consumer_name(name)?;
        }
        if self.ack_wait.is_zero() {
            return Err(NatsError::config("jetstream: ack_wait 必须大于零"));
        }
        if self.max_deliver <= 0 {
            return Err(NatsError::config("jetstream: max_deliver 必须大于零"));
        }
        if self.max_ack_pending <= 0 {
            return Err(NatsError::config("jetstream: max_ack_pending 必须大于零"));
        }
        validate_operation_timeout(self.command_timeout)?;
        if let Some(subject) = &self.filter_subject {
            if subject.trim().is_empty() {
                return Err(NatsError::config("jetstream: filter_subject 不能为空"));
            }
        }
        Ok(())
    }
}
