use crate::types::{InstrumentType, Side};
use anyhow::{anyhow, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct InstrumentConfig {
    pub name: String,
    pub symbol: String,
    pub instrument_type: InstrumentType,
    pub side: Side,
    pub target_qty: f64,
    pub min_child_qty: f64,
    pub max_child_pct: f64,
    pub max_order_age_ms: u64,
    pub execution_horizon_ms: u64,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub api_key: String,
    pub api_secret: String,
    pub recv_window: u64,
    pub base_url: String,
    pub ws_public_base: String,
    pub ws_private_url: String,
    pub decision_interval_ms: u64,
    pub instruments: Vec<InstrumentConfig>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let api_key = env::var("BYBIT_API_KEY").unwrap_or_default();
        let api_secret = env::var("BYBIT_API_SECRET").unwrap_or_default();
        if api_key.is_empty() || api_secret.is_empty() {
            return Err(anyhow!(
                "BYBIT_API_KEY/BYBIT_API_SECRET must be set for trading"
            ));
        }

        let recv_window = read_u64("BYBIT_RECV_WINDOW", 5000);
        let base_url =
            env::var("BYBIT_BASE_URL").unwrap_or_else(|_| "https://api-testnet.bybit.com".into());
        let ws_public_base = env::var("BYBIT_WS_PUBLIC")
            .unwrap_or_else(|_| "wss://stream-testnet.bybit.com/v5/public".into());
        let ws_private_url = env::var("BYBIT_WS_PRIVATE")
            .unwrap_or_else(|_| "wss://stream-testnet.bybit.com/v5/private".into());
        let decision_interval_ms = read_u64("DECISION_INTERVAL_MS", 200);

        let spot = build_instrument(
            "SPOT",
            "spot",
            InstrumentType::Spot,
            "BTCUSDT",
            0.01,
        );
        let perp = build_instrument(
            "PERP",
            "perp",
            InstrumentType::Perp,
            "BTCUSDT",
            0.01,
        );
        let option = build_instrument(
            "OPTION",
            "option",
            InstrumentType::Option,
            "BTC-30JUN25-50000-C",
            0.01,
        );

        Ok(Self {
            api_key,
            api_secret,
            recv_window,
            base_url,
            ws_public_base,
            ws_private_url,
            decision_interval_ms,
            instruments: vec![spot, perp, option],
        })
    }
}

fn build_instrument(
    prefix: &str,
    name: &str,
    instrument_type: InstrumentType,
    default_symbol: &str,
    default_qty: f64,
) -> InstrumentConfig {
    let symbol = env::var(format!("{}_SYMBOL", prefix)).unwrap_or_else(|_| default_symbol.into());
    let side = env::var(format!("{}_SIDE", prefix))
        .ok()
        .and_then(|value| Side::from_str(&value).ok())
        .unwrap_or(Side::Buy);
    let target_qty = read_f64(&format!("{}_QTY", prefix), default_qty);
    let min_child_qty = read_f64(&format!("{}_MIN_CHILD_QTY", prefix), default_qty / 10.0);
    let max_child_pct = read_f64(&format!("{}_MAX_CHILD_PCT", prefix), 0.15);
    let max_order_age_ms = read_u64(&format!("{}_MAX_ORDER_AGE_MS", prefix), 1200);
    let execution_horizon_ms = read_u64(&format!("{}_EXECUTION_HORIZON_MS", prefix), 25_000);

    InstrumentConfig {
        name: name.to_string(),
        symbol,
        instrument_type,
        side,
        target_qty,
        min_child_qty,
        max_child_pct,
        max_order_age_ms,
        execution_horizon_ms,
    }
}

fn read_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn read_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}
