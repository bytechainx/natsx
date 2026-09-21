#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! natsx 热路径基准测试。
//!
//! 覆盖「默认配置 → validate → TLS 策略判定 → subject 校验」的离线路径，
//! 不连接真实 NATS 服务。运行：`cargo bench --bench hot_path`（加 `-- --quick` 缩减迭代数）。
use std::hint::black_box;
use std::time::Instant;

use natsx::{url_is_loopback, validate_publish_subject, validate_stream_name, NatsConfig};

fn iters() -> u32 {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--test") {
        // cargo test --all-targets 会以测试模式运行本二进制；只做冒烟。
        10
    } else if args.iter().any(|a| a == "--quick") {
        1_000
    } else {
        50_000
    }
}

fn main() {
    let n = iters();

    // 预热
    for _ in 0..3 {
        let config = NatsConfig::default();
        black_box(config.validate().is_ok());
    }

    let config = NatsConfig::default();

    let start = Instant::now();
    for i in 0..n {
        // 配置构建 + 校验（fail-fast 路径）
        let built = NatsConfig::builder().build().expect("build 应成功");
        built.validate().expect("validate 应成功");
        // TLS 策略与地址判定
        black_box(built.effective_tls_policy());
        black_box(url_is_loopback(&built.url));
        // subject / stream 校验纯函数
        black_box(validate_publish_subject("demo.subject").is_ok());
        black_box(validate_stream_name("EVENTS").is_ok());
        black_box(config.validate().is_ok());
        black_box(i);
    }
    let elapsed = start.elapsed();
    println!(
        "bench_natsx_hot_path: iters={n} total={elapsed:?} per_iter={:?}",
        elapsed / n
    );
}
