//! TOML 解析层：`from_toml` 与两个辅助（错误摘要 / 敏感字段拒绝）。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。子模块名用 `tomlfile` 而非
//! `toml`，避免 edition 2018 的 uniform path 让本地模块遮蔽 `toml` 依赖 crate
//! （同 `ossx` 的 `tomlfile`）。搬走的项全是私有 + 一个 `pub` 方法，无需可见性调整。

use crate::error::{NatsError, NatsResult};

use super::NatsConfig;

impl NatsConfig {
    /// 从 TOML 字符串解析配置。
    ///
    /// 期望根结构为 `schema_version = 1` 加扁平字段；
    /// `password` / `token` / `nkey_seed` / `jwt` 等敏感字段**禁止**出现在 TOML 中。
    ///
    /// # Errors
    ///
    /// TOML 非法、schema_version 不匹配、出现敏感字段或校验失败时返回错误。
    /// 错误消息只含错误摘要与位置（行号 + 字节区间），**不回显 TOML 源码**，
    /// 以免出错行承载凭据时把凭据片段带进日志。
    pub fn from_toml(text: &str) -> NatsResult<Self> {
        reject_secret_keys(text)?;
        let config: Self = toml::from_str(text).map_err(|error| {
            NatsError::serialization(format!(
                "TOML 解析失败: {}",
                toml_error_summary(text, &error)
            ))
        })?;
        config.validate()?;
        Ok(config)
    }
}

/// 渲染 TOML 错误摘要：只保留错误消息与位置，**不带源码片段**。
///
/// `toml` 的错误 `Display` 会把出错行的原始源码一起渲染。当出错行正是承载凭据的那一行
/// （例如 `password = "…` 引号未闭合），凭据片段就会随公开错误消息进入日志与打点，
/// 违反「错误消息不得泄漏敏感值」的安全基线与标准.md §2 的敏感字段治理要求。
/// 因此这里退化为「消息 + 行号 + 字节区间」：保留可定位性，不回显输入内容。
fn toml_error_summary(text: &str, error: &toml::de::Error) -> String {
    let Some(span) = error.span() else {
        return error.message().to_string();
    };
    let line = text
        .get(..span.start)
        .map_or(1, |head| head.matches('\n').count() + 1);
    format!(
        "{}（第 {line} 行，字节区间 {}..{}）",
        error.message(),
        span.start,
        span.end
    )
}

/// 特征化敏感字段：TOML 中出现即拒绝，避免凭据落盘。
fn reject_secret_keys(text: &str) -> NatsResult<()> {
    let value: toml::Value = toml::from_str(text).map_err(|error| {
        NatsError::serialization(format!(
            "TOML 解析失败: {}",
            toml_error_summary(text, &error)
        ))
    })?;
    let table = value
        .as_table()
        .ok_or_else(|| NatsError::config("TOML 根必须为表（key = value 结构）"))?;
    for key in ["password", "token", "nkey_seed", "jwt"] {
        if table.contains_key(key) {
            return Err(NatsError::config(format!(
                "TOML 禁止字段 {key}：敏感凭据必须经环境变量或 Builder 注入"
            )));
        }
    }
    Ok(())
}
