use anyhow::{anyhow, Result};
use clap::Parser;

use crate::types::{Category, ExecutionTarget, Instrument, Side};

#[derive(Parser, Debug)]
#[command(author, version, about = "HFT execution engine for Bybit testnet")]
pub struct Cli {
    #[arg(long, env = "BYBIT_API_KEY")]
    pub api_key: String,
    #[arg(long, env = "BYBIT_API_SECRET")]
    pub api_secret: String,
    #[arg(long, default_value = "https://api-testnet.bybit.com")]
    pub rest_base: String,
    #[arg(long, default_value = "wss://stream-testnet.bybit.com/v5/public")]
    pub ws_public_base: String,
    #[arg(long, default_value_t = 10000)]
    pub recv_window: u64,
    #[arg(long, default_value = "BTCUSDT")]
    pub spot_symbol: String,
    #[arg(long, default_value = "BTCUSDT")]
    pub perp_symbol: String,
    #[arg(long, default_value = "BTC-30JUN25-50000-C")]
    pub option_symbol: String,
    #[arg(long, default_value_t = 0.001)]
    pub spot_qty: f64,
    #[arg(long, default_value_t = 0.001)]
    pub perp_qty: f64,
    #[arg(long, default_value_t = 1.0)]
    pub option_qty: f64,
    #[arg(long, default_value = "buy")]
    pub side: String,
    #[arg(long, default_value_t = 120000)]
    pub horizon_ms: u64,
    #[arg(long, default_value_t = 5)]
    pub depth: usize,
    #[arg(long, default_value_t = 0.30)]
    pub max_participation: f64,
    #[arg(long, default_value_t = 8000)]
    pub order_stale_ms: u64,
    #[arg(long, default_value_t = 0.60)]
    pub toxicity_threshold: f64,
    #[arg(long, default_value_t = 0.15)]
    pub min_alpha: f64,
    #[arg(long, default_value_t = 25)]
    pub spread_window: usize,
    #[arg(long, default_value_t = 500)]
    pub decision_interval_ms: u64,
    #[arg(long, default_value_t = 50)]
    pub trade_window: usize,
    #[arg(long, default_value_t = 0.35)]
    pub max_cross_urgency: f64,
    #[arg(long, default_value_t = true)]
    pub enable_ws: bool,
}

#[derive(Debug, Clone)]
pub struct AlgoConfig {
    pub api_key: String,
    pub api_secret: String,
    pub rest_base: String,
    pub ws_public_base: String,
    pub recv_window: u64,
    pub depth: usize,
    pub horizon_ms: u64,
    pub max_participation: f64,
    pub order_stale_ms: u64,
    pub toxicity_threshold: f64,
    pub min_alpha: f64,
    pub spread_window: usize,
    pub decision_interval_ms: u64,
    pub trade_window: usize,
    pub max_cross_urgency: f64,
    pub enable_ws: bool,
    pub targets: Vec<ExecutionTarget>,
}

impl Cli {
    pub fn to_config(self) -> Result<AlgoConfig> {
        let side = match self.side.to_ascii_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => return Err(anyhow!("Unsupported side: {}", other)),
        };

        let targets = vec![
            ExecutionTarget {
                instrument: Instrument {
                    symbol: self.spot_symbol,
                    category: Category::Spot,
                },
                side,
                total_qty: self.spot_qty,
            },
            ExecutionTarget {
                instrument: Instrument {
                    symbol: self.perp_symbol,
                    category: Category::Linear,
                },
                side,
                total_qty: self.perp_qty,
            },
            ExecutionTarget {
                instrument: Instrument {
                    symbol: self.option_symbol,
                    category: Category::Option,
                },
                side,
                total_qty: self.option_qty,
            },
        ];

        Ok(AlgoConfig {
            api_key: self.api_key,
            api_secret: self.api_secret,
            rest_base: self.rest_base,
            ws_public_base: self.ws_public_base,
            recv_window: self.recv_window,
            depth: self.depth,
            horizon_ms: self.horizon_ms,
            max_participation: self.max_participation,
            order_stale_ms: self.order_stale_ms,
            toxicity_threshold: self.toxicity_threshold,
            min_alpha: self.min_alpha,
            spread_window: self.spread_window,
            decision_interval_ms: self.decision_interval_ms,
            trade_window: self.trade_window,
            max_cross_urgency: self.max_cross_urgency,
            enable_ws: self.enable_ws,
            targets,
        })
    }
}
