//! [`JetStreamConsumer`] 的稳定消费面实现。
//!
//! 从门面 `jetstream.rs` 下沉（`MR-STRUCT-007` 腾余量）。结构定义与 `Debug` 留在门面
//! （门面要用结构体字面量构造它，而父模块看不到子模块的私有字段）；搬走的四个方法
//! 全是 `pub`，故**无需任何可见性调整**。门面的 `run_bounded_command` 是本模块的祖先，
//! 子模块可直接调用。

use std::time::Duration;

use futures_util::StreamExt;

use crate::error::{NatsError, NatsResult};

use super::{run_bounded_command, JetStreamConsumer, JetStreamDelivery};

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
