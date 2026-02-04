mod config;
mod decision;
mod execution;
mod market_data;
mod performance;
mod types;
mod utils;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::Cli;
use crate::decision::DecisionEngine;
use crate::execution::ExecutionEngine;
use crate::market_data::MarketDataHandler;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let config = cli.to_config()?;

    info!(
        "starting execution for {} instruments",
        config.targets.len()
    );

    let (tx, rx) = mpsc::channel(1024);

    if config.enable_ws {
        let market_data = MarketDataHandler::new(
            config.ws_public_base.clone(),
            config.depth,
            tx.clone(),
        );
        let instruments = config
            .targets
            .iter()
            .map(|t| t.instrument.clone())
            .collect::<Vec<_>>();
        market_data.start(instruments).await;
    } else {
        info!("market-data websockets disabled");
    }

    let execution = ExecutionEngine::new(&config);
    let mut decision_engine = DecisionEngine::new(config, execution);
    decision_engine.run(rx).await?;

    info!("execution metrics:");
    for (key, metrics) in decision_engine.metrics_summary() {
        info!(
            "{key}: fill_rate={:.2} exec_vwap={:?} market_vwap={:?} shortfall={:?}",
            metrics.fill_rate(),
            metrics.execution_vwap(),
            metrics.market_vwap(),
            metrics.avg_shortfall()
        );
    }

    Ok(())
}
