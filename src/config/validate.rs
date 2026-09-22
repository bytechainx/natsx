//! 配置校验：fail-closed 的 `validate`。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。`validate` 原本就是 `pub`，
//! 故**公开路径与签名一字未改**。

use std::path::Path;

use crate::error::{NatsError, NatsResult};

use super::{url_is_loopback, NatsConfig, TlsPolicy};

impl NatsConfig {
    /// 校验配置合法性（fail-closed，不发网络请求）。
    ///
    /// 规则：
    /// - URL 非空、可被 `url` 解析、且不得内嵌 userinfo；
    /// - `user` / `password` 必须同时提供或同时缺省；
    /// - `token` 与 `user/password`、NKey seed 互斥；NKey seed 与 `user/password` 互斥；
    /// - 非 loopback 地址必须使用 [`TlsPolicy::Require`]；
    /// - `tls_cert_file` / `tls_key_file` 必须成对出现且文件存在；
    /// - `tls_ca_file` 存在时必须可访问；
    /// - 超时、容量、最大重连数必须为正。
    ///
    /// # Errors
    ///
    /// 任一条不满足时返回 [`NatsError::Config`]。
    pub fn validate(&self) -> NatsResult<()> {
        if self.url.trim().is_empty() {
            return Err(NatsError::config("url 不能为空"));
        }
        let parsed = url::Url::parse(self.url.trim())
            .map_err(|error| NatsError::config(format!("URL 非法: {error}")))?;
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(NatsError::config(
                "URL 禁止内嵌 userinfo；请使用独立 user/password 字段",
            ));
        }

        match (&self.user, &self.password) {
            (Some(user), Some(password)) if !user.is_empty() && !password.is_empty() => {}
            (None, None) => {}
            _ => {
                return Err(NatsError::config("user/password 必须同时提供或同时缺省"));
            }
        }
        if self
            .token
            .as_ref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(NatsError::config("token 不能为空字符串"));
        }
        let has_user_password = self.user.is_some() || self.password.is_some();
        if self.token.is_some() && (has_user_password || self.nkey_seed.is_some()) {
            return Err(NatsError::config(
                "token 与 user/password、NKey seed 互斥，只能二选一",
            ));
        }
        if self
            .nkey_seed
            .as_ref()
            .is_some_and(|seed| seed.trim().is_empty())
        {
            return Err(NatsError::config("NKey seed 不能为空字符串"));
        }
        if self.nkey_seed.is_some() && has_user_password {
            return Err(NatsError::config(
                "NKey seed 与 user/password 互斥，只能二选一",
            ));
        }

        let policy = self.effective_tls_policy();
        if !url_is_loopback(&self.url) && policy != TlsPolicy::Require {
            return Err(NatsError::config(format!(
                "远程服务必须使用 require TLS 策略（当前为 {policy}）"
            )));
        }

        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.reconnect_max_delay.is_zero()
        {
            return Err(NatsError::config(
                "connect/operation/reconnect 超时必须大于零",
            ));
        }
        if self.subscription_capacity == 0 || self.client_capacity == 0 {
            return Err(NatsError::config("subscription/client capacity 必须大于零"));
        }
        if self.max_reconnects == 0 {
            return Err(NatsError::config("max_reconnects 必须为有限正数"));
        }

        match (&self.tls_cert_file, &self.tls_key_file) {
            (Some(cert), Some(key)) => {
                if !(Path::new(cert).is_file() && Path::new(key).is_file()) {
                    return Err(NatsError::config("TLS cert/key 文件路径不存在或不可访问"));
                }
                if policy == TlsPolicy::Disable {
                    return Err(NatsError::config("TLS 证书已配置，但 TLS 策略为 disable"));
                }
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(NatsError::config("TLS cert 文件需要 key 文件同时提供"));
            }
            (None, Some(_)) => {
                return Err(NatsError::config("TLS key 文件需要 cert 文件同时提供"));
            }
        }
        if let Some(ca) = &self.tls_ca_file {
            if !Path::new(ca).is_file() {
                return Err(NatsError::config("TLS CA 文件路径不存在或不可访问"));
            }
        }
        Ok(())
    }
}
