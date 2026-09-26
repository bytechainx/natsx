# natsx

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

NATS 适配器：Core NATS 发布/订阅 + JetStream 持久消费，零内部依赖的标准 Rust 组件库。

- **连接池**：`NatsPool` 共享 `async-nats` 客户端句柄，`Clone` 后多任务共享同一连接；
- **TLS 策略**：`TlsPolicy::{Prefer, Require, Disable}`，loopback 默认 `Prefer`、非 loopback 默认 `Require`，远程明文在配置层即被拒绝（fail-closed）；
- **认证**：user/password、token、NKey seed，敏感字段不进入 `Debug`；
- **超时与重连**：`connect_timeout`、`operation_timeout`、`max_reconnects` 与指数退避；
- **JetStream**：带 ack 的持久发布、stream 管理、durable/ephemeral pull consumer、有限拉取与 `ack` / `nak` / `progress` / `term`；
- **可观测**：`ping()` 返回往返耗时，`health_check()` 返回 `connected` / `server` / `rtt_ms` / `jetstream`，`stats()` 返回发布/断线/慢消费者计数；
- **统一错误**：`NatsError`（`Config` / `Connection` / `Backend` / `Serialization` / `Io` / `Timeout` / `Unsupported`）+ `NatsResult<T>`，并提供 `is_retryable()`。

## 安装

本 crate **不发布到 crates.io**，通过 git 依赖引入：

```toml
[dependencies]
natsx = { git = "https://github.com/bytechainx/natsx" }
```

## 最小可运行示例

```rust
use natsx::{NatsConfig, NatsPool, NatsResult};
use std::time::Duration;

#[tokio::main]
async fn main() -> NatsResult<()> {
    // 连接（本地默认 nats://127.0.0.1:4222，TLS 策略自动为 Prefer）
    let pool = NatsPool::connect(NatsConfig::default()).await?;

    // 先订阅再发布，避免丢消息
    let mut subscription = pool.subscribe("demo.greeting").await?;
    pool.publish("demo.greeting", "hello nats").await?;

    if let Some(message) = subscription.next().await {
        println!("收到 subject={} payload={:?}", message.subject, message.payload);
    }

    // 健康探测：flush 往返耗时
    println!("rtt = {:?}", pool.ping().await?);

    // 优雅关停（flush + 结束订阅转发任务）
    pool.drain(Duration::from_secs(5)).await?;
    Ok(())
}
```

从环境变量加载配置（推荐用于生产）：

```rust
# use natsx::{NatsResult, NatsPool};
# async fn run() -> NatsResult<()> {
let pool = NatsPool::connect_from_env().await?;
# Ok(())
# }
```

JetStream 持久消费：

```rust
use natsx::{JetStream, JetStreamConsumerConfig, NatsConfig, NatsPool, NatsResult, StreamConfig};
use std::time::Duration;

# async fn run() -> NatsResult<()> {
let pool = NatsPool::connect(NatsConfig::default()).await?;
let jetstream = JetStream::from_pool(&pool)?;
jetstream.get_or_create_stream(StreamConfig::new("ORDERS", "orders.>")).await?;
jetstream.publish("orders.created", "{\"id\":1}").await?;

let consumer = jetstream
    .consumer("ORDERS", JetStreamConsumerConfig::durable("worker-1").filter("orders.created"))
    .await?;

if let Some(delivery) = consumer.next_timeout(Duration::from_secs(2)).await? {
    println!(
        "stream={} consumer={} attempts={}",
        delivery.metadata().stream,
        delivery.metadata().consumer,
        delivery.metadata().delivery_attempts
    );
    delivery.ack().await?; // 也可 nak / progress / term
}
# Ok(())
# }
```

## TLS 策略

| 场景 | 生效策略 | 落地方式 |
| --- | --- | --- |
| loopback（`127.0.0.1` / `localhost` / `::1`） | `Prefer` | `require_tls(false)`：允许明文，服务端要求或 URL 为 `tls://` 时升级 TLS |
| 非 loopback | `Require` | `require_tls(true)`：握手失败即连接失败 |
| 显式 `tls_policy` | 以显式值为准 | 优先级高于 `tls` 布尔开关与 host 推导 |
| `tls = true` | `Require` | 等同显式 `require` |

判定顺序：显式 `tls_policy` → `tls` 布尔开关 → host 自动推导。

- `validate()` 拒绝“非 loopback + 非 `Require`”的组合，错误在发出网络请求之前返回；
- 自定义 CA（`tls_ca_file`）通过 `ConnectOptions::add_root_certificates` 注入，根证书集合即该 CA bundle；
- 仅配置 mTLS 证书（`tls_cert_file` + `tls_key_file`）时使用 `add_client_certificate`，同时保留系统根证书；
- `TlsPolicy::Disable` 表示“不主动要求 TLS”；`async-nats` 未提供强制关闭 TLS 的开关，若服务端强制要求 TLS 仍会升级。

## 配置项

`NatsConfig` 的字段与环境变量（规范前缀 `FOUNDATIONX_NATSX_`，兼容 `FOUNDATIONX_NATS_`，前者优先）。

| 配置字段 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `url` | `URL` / `SERVERS` | `nats://127.0.0.1:4222` | `SERVERS` 为逗号分隔列表，取首项；URL 禁止内嵌 userinfo |
| `user` | `USER` / `USERNAME` | 无 | 必须与 `password` 同时提供 |
| `password` | `PASSWORD` | 无 | 敏感；Debug 输出 `***`，禁止出现在 TOML |
| `token` | `TOKEN` | 无 | 敏感；与 user/password、NKey seed 互斥 |
| `nkey_seed` | `NKEY_SEED` | 无 | 敏感；与 user/password、token 互斥 |
| `name` | `NAME` | `natsx` | 客户端名（服务端可见） |
| `tls` | `TLS` | `false` | 遗留布尔开关，`true` 等价 `Require` |
| `tls_policy` | `TLS_POLICY` | 按 host 推导 | `disable` / `prefer` / `require` |
| `tls_ca_file` | `TLS_CA_FILE` | 无 | 自定义 CA bundle（PEM），文件必须存在 |
| `tls_cert_file` | `TLS_CERT_FILE` | 无 | mTLS 客户端证书，必须与 `tls_key_file` 成对 |
| `tls_key_file` | `TLS_KEY_FILE` | 无 | mTLS 客户端私钥，必须与 `tls_cert_file` 成对 |
| `jetstream` | `JETSTREAM` | `false` | 是否期望使用 JetStream（校验/文档标志） |
| `connect_timeout` | `CONNECT_TIMEOUT_MS` | `5000` | 建连超时（毫秒） |
| `operation_timeout` | `OPERATION_TIMEOUT_MS` | `5000` | 单次操作截止时间（毫秒） |
| `subscription_capacity` | `SUBSCRIPTION_CAPACITY` | `256` | 每订阅缓冲上限 |
| `client_capacity` | `CLIENT_CAPACITY` | `256` | 驱动命令队列容量 |
| `max_reconnects` | `MAX_RECONNECTS` | `60` | 连续重连最大次数（必须为正） |
| `reconnect_max_delay` | `RECONNECT_MAX_DELAY_MS` | `5000` | 单次重连退避上限（毫秒） |
| `ignore_discovered_servers` | `IGNORE_DISCOVERED_SERVERS` | `false` | 仅重连显式 URL |

TOML 配置（`schema_version` 必须为 `1`，敏感字段禁止落盘）：

```toml
schema_version = 1
url = "nats://127.0.0.1:4222"
name = "reader"
tls_policy = "require"
jetstream = true
connect_timeout_ms = 2500
operation_timeout_ms = 3000
```

```rust
# use natsx::{NatsConfig, NatsResult};
# fn run() -> NatsResult<()> {
let toml_text = std::fs::read_to_string("nats.toml").expect("读取配置");
let config = NatsConfig::from_toml(&toml_text)?;
# Ok(())
# }
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
