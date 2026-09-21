//! 纯校验函数：subject / stream / consumer 命名与截止时间。
//!
//! 这些函数不发起任何 IO，可离线测试，也是数据面调用前的统一 fail-closed 关卡：
//! 非法名称在**发出网络请求之前**就被拒绝，避免把错误请求打到 broker 上。

use std::time::Duration;

use crate::error::{NatsError, NatsResult};

/// 校验 subject 的基本合法性：非空、无空白字符、无通配符占位残留。
///
/// 通配符 `*`（单层）与 `>`（多层）是否合法取决于用途，
/// 发布路径请使用 [`validate_publish_subject`]，订阅路径允许通配符。
///
/// # Errors
///
/// subject 为空、仅含空白或包含 ASCII 空白字符时返回 [`NatsError::Config`]。
pub fn validate_subject(subject: &str) -> NatsResult<()> {
    if subject.trim().is_empty() {
        return Err(NatsError::config("subject 不能为空"));
    }
    if subject.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(NatsError::config(format!(
            "subject 不能包含空白字符: {subject:?}"
        )));
    }
    Ok(())
}

/// 校验发布用 subject：在 [`validate_subject`] 之上禁止通配符 `*` 与 `>`。
///
/// # Errors
///
/// 违反 [`validate_subject`] 规则，或包含 `*` / `>` 时返回 [`NatsError::Config`]。
pub fn validate_publish_subject(subject: &str) -> NatsResult<()> {
    validate_subject(subject)?;
    if subject.contains('*') || subject.contains('>') {
        return Err(NatsError::config(format!(
            "发布 subject 不允许通配符 `*` / `>`: {subject:?}"
        )));
    }
    Ok(())
}

/// 校验 JetStream stream 名：非空、无空白字符、无 `.` `*` `>`。
///
/// 规则与 `async-nats` 服务端约束对齐；`.` 虽在 NATS subject 中合法，
/// 但在 stream / consumer 名中会被服务端拒绝。
///
/// # Errors
///
/// 名称为空、包含空白字符，或包含 `.` / `*` / `>` 时返回 [`NatsError::Config`]。
pub fn validate_stream_name(name: &str) -> NatsResult<()> {
    if name.is_empty() {
        return Err(NatsError::config("stream 名不能为空"));
    }
    let legal = name
        .bytes()
        .all(|c| !c.is_ascii_whitespace() && c != b'.' && c != b'*' && c != b'>');
    if !legal {
        return Err(NatsError::config(format!(
            "stream 名非法（禁止空白、点号、`*`、`>`）: {name:?}"
        )));
    }
    Ok(())
}

/// 校验 durable / consumer 名：规则与 [`validate_stream_name`] 相同。
///
/// # Errors
///
/// 规则同 [`validate_stream_name`]，错误消息中的对象名替换为 consumer。
pub fn validate_consumer_name(name: &str) -> NatsResult<()> {
    validate_stream_name(name).map_err(|_| {
        NatsError::config(format!(
            "consumer 名非法（禁止空白、点号、`*`、`>`）: {name:?}"
        ))
    })
}

/// 校验操作截止时间：必须严格大于零（fail-closed）。
///
/// # Errors
///
/// `timeout` 为零时返回 [`NatsError::Config`]。
pub fn validate_operation_timeout(timeout: Duration) -> NatsResult<()> {
    if timeout.is_zero() {
        return Err(NatsError::config("operation_timeout 必须大于零"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_boundaries() {
        assert!(validate_subject("orders.created").is_ok());
        assert!(validate_subject("orders.*").is_ok());

        assert!(validate_subject("").is_err());
        assert!(validate_subject("   ").is_err());
        assert!(validate_subject("has space").is_err());
        assert!(validate_subject("tab\there").is_err());
        assert!(validate_subject("nl\nhere").is_err());
    }

    #[test]
    fn publish_subject_rejects_wildcards() {
        assert!(validate_publish_subject("orders.created").is_ok());
        assert!(validate_publish_subject("orders.*").is_err());
        assert!(validate_publish_subject("orders.>").is_err());
        // 发布路径的空值同样被拒绝
        assert!(validate_publish_subject("").is_err());
    }

    #[test]
    fn stream_name_boundaries() {
        assert!(validate_stream_name("EVENTS").is_ok());
        assert!(validate_stream_name("stream_name").is_ok());
        assert!(validate_stream_name("S1").is_ok());

        for invalid in [
            "",
            "bad.name",
            "bad*name",
            "bad>name",
            "has space",
            "\tbad",
            "bad\n",
        ] {
            assert!(
                validate_stream_name(invalid).is_err(),
                "{invalid:?} 必须被拒绝"
            );
        }
    }

    #[test]
    fn consumer_name_matches_stream_rules_with_consumer_message() {
        assert!(validate_consumer_name("worker_1").is_ok());
        for invalid in ["", "a.b", "x*y", "a>b", "has space"] {
            let error = validate_consumer_name(invalid).expect_err("非法 consumer 名必须失败");
            assert!(error.to_string().contains("consumer 名非法"));
        }
    }

    #[test]
    fn operation_timeout_boundaries() {
        assert!(validate_operation_timeout(Duration::from_millis(1)).is_ok());
        assert!(validate_operation_timeout(Duration::from_secs(5)).is_ok());
        let error = validate_operation_timeout(Duration::ZERO).expect_err("零超时必须拒绝");
        assert!(error.to_string().contains("operation_timeout"));
    }
}
