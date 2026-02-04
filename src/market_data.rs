use crate::config::InstrumentConfig;
use crate::types::{MarketDataEvent, MarketDataKind, OrderBookLevel, OrderBookSnapshot, Side, Trade};
use crate::utils::now_millis;
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub struct MarketDataHandler {
    ws_public_base: String,
    instruments: Vec<InstrumentConfig>,
    sender: Sender<MarketDataEvent>,
}

impl MarketDataHandler {
    pub fn new(ws_public_base: String, instruments: Vec<InstrumentConfig>, sender: Sender<MarketDataEvent>) -> Self {
        Self {
            ws_public_base,
            instruments,
            sender,
        }
    }

    pub async fn run(self) -> Result<()> {
        for instrument in self.instruments {
            let ws_public_base = self.ws_public_base.clone();
            let sender = self.sender.clone();
            tokio::spawn(async move {
                if let Err(err) = run_public_stream(ws_public_base, instrument, sender).await {
                    tracing::error!(error = ?err, "public stream failed");
                }
            });
        }
        Ok(())
    }
}

struct OrderBookState {
    bids: Vec<OrderBookLevel>,
    asks: Vec<OrderBookLevel>,
}

impl OrderBookState {
    fn new() -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn apply_snapshot(&mut self, bids: Vec<OrderBookLevel>, asks: Vec<OrderBookLevel>) {
        self.bids = sort_bids(bids);
        self.asks = sort_asks(asks);
    }

    fn apply_delta(&mut self, bids: Vec<OrderBookLevel>, asks: Vec<OrderBookLevel>) {
        apply_updates(&mut self.bids, bids, true);
        apply_updates(&mut self.asks, asks, false);
        self.bids = sort_bids(std::mem::take(&mut self.bids));
        self.asks = sort_asks(std::mem::take(&mut self.asks));
    }

    fn snapshot(&self, timestamp: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            timestamp,
        }
    }
}

fn sort_bids(mut bids: Vec<OrderBookLevel>) -> Vec<OrderBookLevel> {
    bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    bids.truncate(50);
    bids
}

fn sort_asks(mut asks: Vec<OrderBookLevel>) -> Vec<OrderBookLevel> {
    asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
    asks.truncate(50);
    asks
}

fn apply_updates(levels: &mut Vec<OrderBookLevel>, updates: Vec<OrderBookLevel>, is_bid: bool) {
    for update in updates {
        if update.size == 0.0 {
            if let Some(pos) = levels.iter().position(|level| level.price == update.price) {
                levels.remove(pos);
            }
            continue;
        }
        if let Some(existing) = levels.iter_mut().find(|level| level.price == update.price) {
            existing.size = update.size;
        } else {
            levels.push(update);
        }
    }

    if is_bid {
        levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
    }
    levels.truncate(50);
}

fn orderbook_topic(symbol: &str, depth: usize) -> String {
    format!("orderbook.{}.{}", depth, symbol)
}

fn trade_topic(symbol: &str) -> String {
    format!("publicTrade.{}", symbol)
}

async fn run_public_stream(
    ws_public_base: String,
    instrument: InstrumentConfig,
    sender: Sender<MarketDataEvent>,
) -> Result<()> {
    let ws_url = format!("{}/{}", ws_public_base, instrument.instrument_type.ws_public_path());
    let url = Url::parse(&ws_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    let depth = if instrument.instrument_type == crate::types::InstrumentType::Option {
        25
    } else {
        50
    };
    let sub_msg = serde_json::json!({
        "op": "subscribe",
        "args": [
            orderbook_topic(&instrument.symbol, depth),
            trade_topic(&instrument.symbol)
        ]
    });
    write.send(Message::Text(sub_msg.to_string())).await?;

    let mut orderbook_state = OrderBookState::new();
    while let Some(message) = read.next().await {
        let message = message?;
        if let Message::Text(text) = message {
            let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if parsed.get("op").and_then(Value::as_str) == Some("ping") {
                let pong = serde_json::json!({"op": "pong"});
                write.send(Message::Text(pong.to_string())).await?;
                continue;
            }
            let topic = match parsed.get("topic").and_then(Value::as_str) {
                Some(value) => value,
                None => continue,
            };

            if topic.starts_with("orderbook.") {
                let data = parsed.get("data").ok_or_else(|| anyhow!("missing data"))?;
                let bids = parse_levels(data.get("b"));
                let asks = parse_levels(data.get("a"));
                let msg_type = parsed.get("type").and_then(Value::as_str).unwrap_or("snapshot");
                if msg_type == "snapshot" {
                    orderbook_state.apply_snapshot(bids, asks);
                } else {
                    orderbook_state.apply_delta(bids, asks);
                }
                let timestamp = parsed
                    .get("ts")
                    .and_then(Value::as_u64)
                    .unwrap_or_else(now_millis);
                let snapshot = orderbook_state.snapshot(timestamp);
                let _ = sender
                    .send(MarketDataEvent {
                        instrument: instrument.instrument_type,
                        symbol: instrument.symbol.clone(),
                        kind: MarketDataKind::OrderBook(snapshot),
                    })
                    .await;
            } else if topic.starts_with("publicTrade.") {
                let trades = parse_trades(parsed.get("data"));
                for trade in trades {
                    let _ = sender
                        .send(MarketDataEvent {
                            instrument: instrument.instrument_type,
                            symbol: instrument.symbol.clone(),
                            kind: MarketDataKind::Trade(trade),
                        })
                        .await;
                }
            }
        }
    }
    Ok(())
}

fn parse_levels(value: Option<&Value>) -> Vec<OrderBookLevel> {
    let Some(Value::Array(levels)) = value else {
        return Vec::new();
    };
    levels
        .iter()
        .filter_map(|level| {
            let price = level.get(0)?.as_str()?.parse::<f64>().ok()?;
            let size = level.get(1)?.as_str()?.parse::<f64>().ok()?;
            Some(OrderBookLevel { price, size })
        })
        .collect()
}

fn parse_trades(value: Option<&Value>) -> Vec<Trade> {
    let Some(Value::Array(trades)) = value else {
        return Vec::new();
    };
    trades
        .iter()
        .filter_map(|trade| {
            let price = trade.get("p")?.as_str()?.parse::<f64>().ok()?;
            let qty = trade.get("v")?.as_str()?.parse::<f64>().ok()?;
            let side_raw = trade.get("S")?.as_str().unwrap_or("Buy");
            let side = if side_raw.eq_ignore_ascii_case("buy") {
                Side::Buy
            } else {
                Side::Sell
            };
            let timestamp = trade
                .get("T")
                .and_then(Value::as_u64)
                .unwrap_or_else(now_millis);
            Some(Trade {
                price,
                qty,
                side,
                timestamp,
            })
        })
        .collect()
}
