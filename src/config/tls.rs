//! TLS 策略与证书材料：策略解析、loopback 地址判定与 rustls 客户端配置构造。
//!
//! [`TlsPolicy`] 决定是否在连接选项上设置 `require_tls`；[`url_is_loopback`] 支撑
//! 「非 loopback 必须 Require」的 fail-closed 校验；证书材料（CA / mTLS）经
//! [`NatsConfig::apply_tls`] 落到 `async-nats` 连接选项上。

use std::fmt;
use std::path::PathBuf;

use async_nats::rustls::pki_types::pem::PemObject;
use async_nats::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use async_nats::rustls::{ClientConfig, RootCertStore};
use serde::Deserialize;

use super::NatsConfig;
use crate::error::{NatsError, NatsResult};

/// TLS 策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsPolicy {
    /// 不主动要求 TLS（允许明文）；仅建议用于 loopback。
    Disable,
    /// 优先 TLS、允许明文；仅建议用于 loopback。
    #[default]
    Prefer,
    /// 必须 TLS；连接层设置 `require_tls(true)`，握手失败即连接失败。
    Require,
}

impl TlsPolicy {
    /// 解析策略字符串（大小写不敏感）。
    ///
    /// # Errors
    ///
    /// 字符串不属于 `disable|prefer|require` 及其别名时返回 [`NatsError::Config`]。
    pub fn parse(raw: &str) -> NatsResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disable" | "disabled" | "off" | "false" | "0" | "none" => Ok(Self::Disable),
            "prefer" | "optional" | "auto" => Ok(Self::Prefer),
            "require" | "required" | "on" | "true" | "1" | "mandatory" => Ok(Self::Require),
            other => Err(NatsError::config(format!(
                "未知 TLS 策略 {other:?}（期望 disable|prefer|require）"
            ))),
        }
    }

    /// 是否在连接选项上设置 `require_tls(true)`。
    #[must_use]
    pub fn require_tls(self) -> bool {
        matches!(self, Self::Require)
    }
}

impl fmt::Display for TlsPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disable => write!(f, "disable"),
            Self::Prefer => write!(f, "prefer"),
            Self::Require => write!(f, "require"),
        }
    }
}

impl<'de> Deserialize<'de> for TlsPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// 判断 NATS URL 是否指向 loopback（`127.0.0.1` / `localhost` / `::1`）。
///
/// 支持 `scheme://host:port`、`[::1]:port`、带用户信息与多地址逗号分隔等写法。
#[must_use]
pub fn url_is_loopback(url: &str) -> bool {
    matches!(
        extract_host(url).to_ascii_lowercase().as_str(),
        "127.0.0.1" | "localhost" | "::1" | "[::1]"
    )
}

fn extract_host(url: &str) -> String {
    let trimmed = url.trim();
    let without_scheme = if let Some(index) = trimmed.find("://") {
        &trimmed[index + 3..]
    } else {
        trimmed
    };
    // 多地址（逗号分隔）只看第一个
    let single = without_scheme.split(',').next().unwrap_or(without_scheme);
    // 去掉 userinfo@
    let after_user = single.rsplit('@').next().unwrap_or(single);
    // [ipv6]:port
    if let Some(rest) = after_user.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return format!("[{}]", &rest[..end]);
        }
    }
    let host_port = after_user.split('/').next().unwrap_or(after_user);
    host_port
        .split(':')
        .next()
        .unwrap_or(host_port)
        .trim()
        .to_string()
}

impl NatsConfig {
    /// 把 TLS 策略与证书材料落到 `async-nats` 连接选项上。
    ///
    /// - 始终设置 `require_tls(policy.require_tls())`；
    /// - 配置了自定义 CA 时，走 `add_root_certificates`（`async-nats` 0.50 在
    ///   `tls_client_config.is_some()` 时仍会 `load_native_certs`，任一平台 PEM
    ///   不可读即失败；证书列表非空才跳过平台根）。先解析 CA 做 fail-fast。
    /// - 仅配置 mTLS 证书时，使用 `add_client_certificate`（会叠加系统根证书）。
    pub(crate) fn apply_tls(
        &self,
        options: async_nats::ConnectOptions,
    ) -> NatsResult<async_nats::ConnectOptions> {
        let policy = self.effective_tls_policy();
        let options = options.require_tls(policy.require_tls());
        match (&self.tls_ca_file, &self.tls_cert_file, &self.tls_key_file) {
            (Some(ca), cert, key) => {
                let identity = match (cert, key) {
                    (Some(cert), Some(key)) => Some((cert.as_str(), key.as_str())),
                    _ => None,
                };
                let _validated = build_tls_client_config(ca, identity)?;
                let options = options.add_root_certificates(PathBuf::from(ca));
                Ok(match identity {
                    Some((cert, key)) => {
                        options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key))
                    }
                    None => options,
                })
            }
            (None, Some(cert), Some(key)) => {
                Ok(options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key)))
            }
            _ => Ok(options),
        }
    }
}

/// 用自定义 CA（可选 mTLS 身份）构造 rustls 客户端配置。
fn build_tls_client_config(
    ca_file: &str,
    identity: Option<(&str, &str)>,
) -> NatsResult<ClientConfig> {
    let ca_pem = std::fs::read(ca_file)
        .map_err(|error| NatsError::config(format!("读取 TLS CA 文件失败 {ca_file}: {error}")))?;
    let certificates = CertificateDer::pem_slice_iter(&ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| NatsError::config(format!("解析 TLS CA 文件失败 {ca_file}: {error}")))?;
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(certificates);
    if added == 0 {
        return Err(NatsError::config(format!(
            "TLS CA 文件未包含可用证书: {ca_file}"
        )));
    }
    if ignored > 0 {
        tracing::debug!(ca_file, ignored, "TLS CA 文件中存在无法解析的证书条目");
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    match identity {
        Some((cert_file, key_file)) => {
            let cert_pem = std::fs::read(cert_file).map_err(|error| {
                NatsError::config(format!("读取 TLS 客户端证书失败 {cert_file}: {error}"))
            })?;
            let key_pem = std::fs::read(key_file).map_err(|error| {
                NatsError::config(format!("读取 TLS 客户端私钥失败 {key_file}: {error}"))
            })?;
            let chain = CertificateDer::pem_slice_iter(&cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    NatsError::config(format!("解析 TLS 客户端证书失败 {cert_file}: {error}"))
                })?;
            let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|error| {
                NatsError::config(format!("解析 TLS 客户端私钥失败 {key_file}: {error}"))
            })?;
            let config = builder.with_client_auth_cert(chain, key).map_err(|error| {
                NatsError::config(format!("TLS 客户端证书与私钥不匹配: {error}"))
            })?;
            Ok(config)
        }
        None => Ok(builder.with_no_client_auth()),
    }
}
