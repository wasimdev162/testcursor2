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
    pub strategy: StrategyConfig,
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
        let strategy = StrategyConfig::from_env();

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
            strategy,
        })
    }
}

#[derive(Clone, Debug)]
pub struct StrategyConfig {
    pub imbalance_weight: f64,
    pub flow_weight: f64,
    pub momentum_weight: f64,
    pub mean_reversion_weight: f64,
    pub iceberg_weight: f64,
    pub iceberg_bias: f64,
    pub passive_alpha_threshold: f64,
    pub aggressive_alpha_threshold: f64,
    pub cancel_alpha_threshold: f64,
    pub toxicity_cancel_threshold: f64,
    pub toxicity_passive_threshold: f64,
    pub toxicity_flow_threshold: f64,
    pub spread_widen_cancel_threshold: f64,
    pub spread_widen_passive_threshold: f64,
    pub min_decision_interval_ms: u64,
    pub mean_reversion_window_ms: u64,
    pub large_trade_multiplier: f64,
    pub iceberg_multiplier: f64,
    pub iceberg_persistence: u32,
    pub iceberg_stable_pct: f64,
    pub urgency_aggressive_threshold: f64,
    pub urgency_force_aggressive_threshold: f64,
    pub fill_prob_aggressive_threshold: f64,
    pub spread_move_cancel_factor: f64,
    pub imbalance_depth: usize,
    pub liquidity_depth: usize,
    pub iceberg_depth: usize,
    pub spread_window: usize,
    pub mid_window: usize,
    pub trade_flow_window: usize,
    pub trade_qty_window: usize,
}

impl StrategyConfig {
    pub fn from_env() -> Self {
        Self {
            imbalance_weight: read_f64("STRAT_IMBALANCE_WEIGHT", 0.45),
            flow_weight: read_f64("STRAT_FLOW_WEIGHT", 0.25),
            momentum_weight: read_f64("STRAT_MOMENTUM_WEIGHT", 0.15),
            mean_reversion_weight: read_f64("STRAT_MEAN_REV_WEIGHT", 0.1),
            iceberg_weight: read_f64("STRAT_ICEBERG_WEIGHT", 0.05),
            iceberg_bias: read_f64("STRAT_ICEBERG_BIAS", 0.6),
            passive_alpha_threshold: read_f64("STRAT_PASSIVE_ALPHA", 0.05),
            aggressive_alpha_threshold: read_f64("STRAT_AGGRESSIVE_ALPHA", -0.08),
            cancel_alpha_threshold: read_f64("STRAT_CANCEL_ALPHA", -0.1),
            toxicity_cancel_threshold: read_f64("STRAT_TOXICITY_CANCEL", 0.35),
            toxicity_passive_threshold: read_f64("STRAT_TOXICITY_PASSIVE", 0.2),
            toxicity_flow_threshold: read_f64("STRAT_TOXICITY_FLOW", 0.15),
            spread_widen_cancel_threshold: read_f64("STRAT_SPREAD_WIDEN_CANCEL", 0.5),
            spread_widen_passive_threshold: read_f64("STRAT_SPREAD_WIDEN_PASSIVE", 0.6),
            min_decision_interval_ms: read_u64("STRAT_MIN_DECISION_MS", 150),
            mean_reversion_window_ms: read_u64("STRAT_MEAN_REVERSION_MS", 1200),
            large_trade_multiplier: read_f64("STRAT_LARGE_TRADE_MULT", 3.0),
            iceberg_multiplier: read_f64("STRAT_ICEBERG_MULT", 3.0),
            iceberg_persistence: read_u32("STRAT_ICEBERG_PERSIST", 3),
            iceberg_stable_pct: read_f64("STRAT_ICEBERG_STABLE_PCT", 0.05),
            urgency_aggressive_threshold: read_f64("STRAT_URGENCY_AGGR", 0.7),
            urgency_force_aggressive_threshold: read_f64("STRAT_URGENCY_FORCE_AGGR", 0.85),
            fill_prob_aggressive_threshold: read_f64("STRAT_FILL_PROB_AGGR", 0.2),
            spread_move_cancel_factor: read_f64("STRAT_SPREAD_MOVE_CANCEL", 0.1),
            imbalance_depth: read_usize("STRAT_IMBALANCE_DEPTH", 5),
            liquidity_depth: read_usize("STRAT_LIQUIDITY_DEPTH", 3),
            iceberg_depth: read_usize("STRAT_ICEBERG_DEPTH", 3),
            spread_window: read_usize("STRAT_SPREAD_WINDOW", 40),
            mid_window: read_usize("STRAT_MID_WINDOW", 40),
            trade_flow_window: read_usize("STRAT_TRADE_FLOW_WINDOW", 50),
            trade_qty_window: read_usize("STRAT_TRADE_QTY_WINDOW", 50),
        }
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

fn read_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn read_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}
