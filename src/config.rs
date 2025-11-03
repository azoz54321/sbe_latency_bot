#![allow(dead_code)]

use once_cell::sync::Lazy;
use std::time::Duration;

use crate::clock::Clock;

pub static CONFIG: Lazy<Config> = Lazy::new(Config::new);

#[derive(Debug)]
pub struct Config {
    pub transport: TransportConfig,
    pub credentials: Credentials,
    pub channel: ChannelConfig,
    pub sharding: ShardingConfig,
    pub deployment: DeploymentConfig,
    pub trigger: TriggerConfig,
    pub execution: ExecutionConfig,
    pub backpressure: BackpressureConfig,
    pub logging: LoggingConfig,
    pub strategy: StrategyConfig,
    pub metrics: MetricsConfig,
}

impl Config {
    fn new() -> Self {
        Self {
            transport: TransportConfig {
                sbe_ws_url: SBE_WS_URL,
                rest_base_url: "https://api.binance.com",
            },
            credentials: Credentials {
                sbe_ws_api_key: BINANCE_SBE_API_KEY,
                rest_api_key: BINANCE_API_KEY,
                rest_api_secret: BINANCE_API_SECRET,
            },
            channel: ChannelConfig {
                market_capacity: MARKET_CHANNEL_CAPACITY,
                trigger_capacity: TRIGGER_CHANNEL_CAPACITY,
                log_capacity: LOG_CHANNEL_CAPACITY,
            },
            sharding: ShardingConfig {
                streams: &SBE_STREAMS,
                shard_size: SBE_SHARD_SIZE,
                max_shards: SBE_MAX_SHARDS,
                cpu_pin_per_shard: &CPU_PIN_PER_SHARD,
            },
            deployment: DeploymentConfig {
                use_multi_server: USE_MULTI_SERVER,
                active_server: ServerId::ServerA,
                servers: &SERVER_SPECS,
            },
            trigger: TriggerConfig {
                trigger_pct: TRIGGER_PCT,
                window: Duration::from_millis(WINDOW_MS),
                latency_budget: Duration::from_millis(LATENCY_BUDGET_MS),
            },
            execution: ExecutionConfig {
                mode: {
                    #[cfg(feature = "test-mode")]
                    {
                        ExecutionMode::Live
                    }
                    #[cfg(not(feature = "test-mode"))]
                    {
                        ExecutionMode::Shadow
                    }
                },
                order_quote_size_usdt: ORDER_QUOTE_SIZE_USDT,
                retry_on_fail: RETRY_ON_FAIL,
                request_timeout: Duration::from_millis(HTTP_REQUEST_TIMEOUT_MS),
                recv_window_ms: REST_RECV_WINDOW_MS,
            },
            backpressure: BackpressureConfig {
                drop_policy: DROP_POLICY,
                max_queue_age: Duration::from_millis(QUEUE_MAX_AGE_MS),
                ws_ping_p95_threshold: Duration::from_millis(WS_PING_P95_THRESHOLD_MS),
                ws_ping_sustain: Duration::from_secs(WS_PING_SUSTAIN_SECS),
                latency_p95_warning: LATENCY_P95_WARNING_MS,
                latency_p95_fail: LATENCY_P95_FAIL_MS,
            },
            logging: LoggingConfig {
                flush_interval: Duration::from_millis(LOG_FLUSH_MS),
                daily_report_hour_utc: DAILY_REPORT_HOUR_UTC,
            },
            strategy: StrategyConfig {
                warmup_secs: WARMUP_SECS,
                window_ret_60s: Duration::from_millis(WINDOW_RET_60S_MS),
                daily_reset_hour_ksa: DAILY_RESET_HOUR_KSA,
                ahi: AhiConfig {
                    ahi_enter: AHI_ENTER,
                    ahi_enter_after_loss: AHI_ENTER_AFTER_LOSS,
                    ahi_exit: AHI_EXIT,
                    ahi_drop_exit: AHI_DROP_EXIT,
                    enter_window: Duration::from_secs(AHI_WINDOW_ENTER_MINUTES * 60),
                    drop_window: Duration::from_secs(AHI_DROP_WINDOW_MINUTES * 60),
                },
                btc_15m_abs_enter: BTC_15M_ABS_ENTER,
                btc_15m_abs_exit: BTC_15M_ABS_EXIT,
                enable_metrics_test_only: {
                    #[cfg(feature = "test-mode")]
                    {
                        true
                    }
                    #[cfg(not(feature = "test-mode"))]
                    {
                        ENABLE_METRICS_TEST_ONLY
                    }
                },
            },
            metrics: MetricsConfig {
                tick_to_send_targets: LatencyTargets {
                    p50_ms: 6.0,
                    p95_ms: 15.0,
                    p99_ms: 20.0,
                },
            },
        }
    }

    pub fn load() -> &'static Self {
        &CONFIG
    }

    pub fn ksa_now(&'static self, clock: &dyn Clock) -> chrono::DateTime<chrono::FixedOffset> {
        use chrono::FixedOffset;

        let now_utc = clock.now_utc();
        let offset = FixedOffset::east_opt(KSA_OFFSET_SECS).expect("invalid KSA offset");
        now_utc.with_timezone(&offset)
    }

    pub fn todays_reset_time_ksa(
        &'static self,
        base: chrono::DateTime<chrono::FixedOffset>,
    ) -> chrono::DateTime<chrono::FixedOffset> {
        use chrono::{FixedOffset, NaiveDate};

        let offset = FixedOffset::east_opt(KSA_OFFSET_SECS).expect("invalid KSA offset");
        let date: NaiveDate = base.date_naive();
        let hour = self.strategy.daily_reset_hour_ksa;
        date.and_hms_opt(hour as u32, 0, 0)
            .expect("invalid reset hour for KSA")
            .and_local_timezone(offset)
            .single()
            .expect("failed to convert reset time to timezone")
    }

    pub fn next_reset_time_ksa(
        &'static self,
        base: chrono::DateTime<chrono::FixedOffset>,
    ) -> chrono::DateTime<chrono::FixedOffset> {
        let today = self.todays_reset_time_ksa(base);
        if base < today {
            today
        } else {
            today + chrono::Duration::days(1)
        }
    }

    pub fn active_server_spec(&'static self) -> &'static ServerSpec {
        self.deployment
            .servers
            .iter()
            .find(|spec| spec.id == self.deployment.active_server)
            .expect("active server not described in configuration")
    }
}

#[derive(Debug)]
pub struct TransportConfig {
    pub sbe_ws_url: &'static str,
    pub rest_base_url: &'static str,
}

#[derive(Debug)]
pub struct Credentials {
    pub sbe_ws_api_key: &'static str,
    pub rest_api_key: &'static str,
    pub rest_api_secret: &'static str,
}

#[derive(Debug)]
pub struct ChannelConfig {
    pub market_capacity: usize,
    pub trigger_capacity: usize,
    pub log_capacity: usize,
}

#[derive(Debug)]
pub struct ShardingConfig {
    pub streams: &'static [&'static str],
    pub shard_size: usize,
    pub max_shards: usize,
    pub cpu_pin_per_shard: &'static [usize],
}

impl ShardingConfig {
    pub fn shard_assignments(&'static self, server: &'static ServerSpec) -> Vec<ShardAssignment> {
        server
            .shard_indices
            .iter()
            .filter_map(|&shard_index| {
                let start = shard_index * self.shard_size;
                if start >= self.streams.len() {
                    return None;
                }
                let end = usize::min(start + self.shard_size, self.streams.len());
                let symbols = &self.streams[start..end];
                let cpu_core = *self
                    .cpu_pin_per_shard
                    .get(shard_index)
                    .unwrap_or(&self.cpu_pin_per_shard[self.cpu_pin_per_shard.len() - 1]);

                Some(ShardAssignment {
                    shard_index,
                    symbols,
                    cpu_core,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ShardAssignment {
    pub shard_index: usize,
    pub symbols: &'static [&'static str],
    pub cpu_core: usize,
}

impl ShardAssignment {
    pub fn websocket_url(&self, transport: &TransportConfig) -> String {
        if self.symbols.len() == 1 {
            format!("{}/ws/{}@trade", transport.sbe_ws_url, self.symbols[0])
        } else {
            let streams = self
                .symbols
                .iter()
                .map(|sym| format!("{}@trade", sym))
                .collect::<Vec<_>>()
                .join("/");
            format!("{}/stream?streams={}", transport.sbe_ws_url, streams)
        }
    }
}

#[derive(Debug)]
pub struct DeploymentConfig {
    pub use_multi_server: bool,
    pub active_server: ServerId,
    pub servers: &'static [ServerSpec],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerId {
    ServerA,
    ServerB,
}

#[derive(Debug)]
pub struct ServerSpec {
    pub id: ServerId,
    pub shard_indices: &'static [usize],
    pub processor_core: usize,
    pub execution_core: usize,
}

#[derive(Debug)]
pub struct TriggerConfig {
    pub trigger_pct: f64,
    pub window: Duration,
    pub latency_budget: Duration,
}

#[derive(Debug)]
pub struct ExecutionConfig {
    pub mode: ExecutionMode,
    pub order_quote_size_usdt: f64,
    pub retry_on_fail: bool,
    pub request_timeout: Duration,
    pub recv_window_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ExecutionMode {
    Shadow,
    Live,
}

#[derive(Debug)]
pub struct BackpressureConfig {
    pub drop_policy: &'static str,
    pub max_queue_age: Duration,
    pub ws_ping_p95_threshold: Duration,
    pub ws_ping_sustain: Duration,
    pub latency_p95_warning: f64,
    pub latency_p95_fail: f64,
}

#[derive(Debug)]
pub struct LoggingConfig {
    pub flush_interval: Duration,
    pub daily_report_hour_utc: u8,
}

#[derive(Debug)]
pub struct MetricsConfig {
    pub tick_to_send_targets: LatencyTargets,
}

#[derive(Debug)]
pub struct StrategyConfig {
    pub warmup_secs: u64,
    pub window_ret_60s: Duration,
    pub daily_reset_hour_ksa: u8,
    pub ahi: AhiConfig,
    pub btc_15m_abs_enter: f64,
    pub btc_15m_abs_exit: f64,
    pub enable_metrics_test_only: bool,
}

#[derive(Debug)]
pub struct AhiConfig {
    pub ahi_enter: f64,
    pub ahi_enter_after_loss: f64,
    pub ahi_exit: f64,
    pub ahi_drop_exit: f64,
    pub enter_window: Duration,
    pub drop_window: Duration,
}

#[derive(Debug)]
pub struct LatencyTargets {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

// "BINANCE_API_KEY", "MzYrHl9UayAmgCYei9dvHoc1pHbXfqkldJ3vLMtrhaxbgBlGl5VB21fAE7cRLGnA"
// "BINANCE_API_SECRET", "nXugw3xOgEOic2tbehqGoVJb4z5uZG0tq3KL7jsO05Il4qUIEa69Xdsfq03IAbcq"

const SBE_WS_URL: &str = "wss://stream-sbe.binance.com/ws/<symbol>@trade";
const BINANCE_SBE_API_KEY: &str =
    "H57xQl3d5pGXLUQmFvANn7hMmNBJqsUyX2PWszhdytwT0ods3UrZgq60awmaxqzP";
const BINANCE_API_KEY: &str = "MzYrHl9UayAmgCYei9dvHoc1pHbXfqkldJ3vLMtrhaxbgBlGl5VB21fAE7cRLGnA";
const BINANCE_API_SECRET: &str = "nXugw3xOgEOic2tbehqGoVJb4z5uZG0tq3KL7jsO05Il4qUIEa69Xdsfq03IAbcq";

const SBE_SHARD_SIZE: usize = 25;
const SBE_MAX_SHARDS: usize = 15;

const CPU_PIN_PER_SHARD: [usize; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

const SERVER_A_SHARDS: [usize; 8] = [0, 1, 2, 3, 4, 5, 6, 7];
const SERVER_B_SHARDS: [usize; 7] = [8, 9, 10, 11, 12, 13, 14];

const SERVER_SPECS: [ServerSpec; 2] = [
    ServerSpec {
        id: ServerId::ServerA,
        shard_indices: &SERVER_A_SHARDS,
        processor_core: 24,
        execution_core: 25,
    },
    ServerSpec {
        id: ServerId::ServerB,
        shard_indices: &SERVER_B_SHARDS,
        processor_core: 26,
        execution_core: 27,
    },
];

const USE_MULTI_SERVER: bool = true;

const TRIGGER_PCT: f64 = 0.0001;
const WINDOW_MS: u64 = 1_000;
const LATENCY_BUDGET_MS: u64 = 15;

const ORDER_QUOTE_SIZE_USDT: f64 = 50.0;
const RETRY_ON_FAIL: bool = true;
const HTTP_REQUEST_TIMEOUT_MS: u64 = 5;
const REST_RECV_WINDOW_MS: u64 = 500;

const DROP_POLICY: &str = "drop_oldest_over_150ms";
const QUEUE_MAX_AGE_MS: u64 = 150;
const WS_PING_P95_THRESHOLD_MS: u64 = 200;
const WS_PING_SUSTAIN_SECS: u64 = 30;
const LATENCY_P95_WARNING_MS: f64 = 15.0;
const LATENCY_P95_FAIL_MS: f64 = 18.0;

const LOG_FLUSH_MS: u64 = 1_000;
const DAILY_REPORT_HOUR_UTC: u8 = 0;

const MARKET_CHANNEL_CAPACITY: usize = 8192;
const TRIGGER_CHANNEL_CAPACITY: usize = 1024;
const LOG_CHANNEL_CAPACITY: usize = 2048;

const SBE_STREAMS: [&str; 1] = ["btcusdt"];

const WARMUP_SECS: u64 = 60;
const WINDOW_RET_60S_MS: u64 = 60_000;
const DAILY_RESET_HOUR_KSA: u8 = 3;
const AHI_ENTER: f64 = 60.0;
const AHI_ENTER_AFTER_LOSS: f64 = 62.0;
const AHI_EXIT: f64 = 50.0;
const AHI_DROP_EXIT: f64 = 15.0;
const AHI_WINDOW_ENTER_MINUTES: u64 = 3;
const AHI_DROP_WINDOW_MINUTES: u64 = 5;
const BTC_15M_ABS_ENTER: f64 = 0.012;
const BTC_15M_ABS_EXIT: f64 = 0.018;
const ENABLE_METRICS_TEST_ONLY: bool = false;
const KSA_OFFSET_SECS: i32 = 3 * 60 * 60;
