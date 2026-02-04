mod config;
mod execution;
mod market_data;
mod performance;
mod strategy;
mod types;
mod utils;

use crate::config::AppConfig;
use crate::execution::{ExecutionAction, ExecutionEngine};
use crate::market_data::MarketDataHandler;
use crate::performance::PerformanceTracker;
use crate::strategy::StrategyManager;
use crate::types::MarketDataKind;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = AppConfig::from_env()?;

    let (md_tx, mut md_rx) = mpsc::channel(2048);
    let (exec_tx, mut exec_rx) = mpsc::channel(1024);

    let client = execution::BybitClient::new(
        config.api_key.clone(),
        config.api_secret.clone(),
        config.recv_window,
        config.base_url.clone(),
    );
    let engine = ExecutionEngine::new(client, &config.instruments);
    engine
        .spawn_private_stream(config.ws_private_url.clone(), exec_tx.clone())
        .await;

    let market_data = MarketDataHandler::new(
        config.ws_public_base.clone(),
        config.instruments.clone(),
        md_tx,
    );
    market_data.run().await?;

    let mut strategies = StrategyManager::new(config.instruments.clone(), config.strategy.clone());
    let mut performance = PerformanceTracker::new(&config.instruments);
    let mut ticker = interval(Duration::from_millis(config.decision_interval_ms));

    loop {
        tokio::select! {
            Some(event) = md_rx.recv() => {
                if let MarketDataKind::Trade(trade) = &event.kind {
                    performance.record_market_trade(event.instrument, trade);
                }
                let actions = strategies.on_market_event(event);
                handle_actions(actions, &engine, &mut performance, &mut strategies).await;
            }
            Some(update) = exec_rx.recv() => {
                strategies.on_execution_update(&update);
                if let execution::ExecutionUpdate::Fill(fill) = update {
                    performance.record_fill(fill);
                }
            }
            _ = ticker.tick() => {
                let actions = strategies.on_timer();
                handle_actions(actions, &engine, &mut performance, &mut strategies).await;
                performance.maybe_report();
            }
        }
    }
}

async fn handle_actions(
    actions: Vec<ExecutionAction>,
    engine: &ExecutionEngine,
    performance: &mut PerformanceTracker,
    strategies: &mut StrategyManager,
) {
    for action in actions {
        match &action {
            ExecutionAction::Place(intent) => {
                performance.record_decision(
                    intent.client_id.clone(),
                    intent.instrument,
                    intent.symbol.clone(),
                    intent.side,
                    intent.decision_price,
                    intent.qty,
                );
            }
            ExecutionAction::Cancel { .. } => {}
        }

        match engine.handle_action(action).await {
            Ok(Some(ack)) => strategies.on_order_ack(&ack),
            Ok(None) => {}
            Err(err) => {
                tracing::error!(error = ?err, "execution action failed");
            }
        }
    }
}
