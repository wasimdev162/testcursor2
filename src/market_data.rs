use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::types::{Instrument, Level, MarketEvent, OrderBookSnapshot, Side, Trade};
use crate::utils::now_ms;

#[derive(Clone)]
pub struct MarketDataHandler {
    ws_public_base: String,
    depth: usize,
    sender: Sender<MarketEvent>,
}

impl MarketDataHandler {
    pub fn new(ws_public_base: String, depth: usize, sender: Sender<MarketEvent>) -> Self {
        Self {
            ws_public_base,
            depth,
            sender,
        }
    }

    pub async fn start(&self, instruments: Vec<Instrument>) {
        for instrument in instruments {
            let handler = self.clone();
            tokio::spawn(async move {
                if let Err(err) = handler.run_stream(instrument.clone()).await {
                    warn!("market-data stream failed: {} ({:?})", err, instrument);
                }
            });
        }
    }

    async fn run_stream(&self, instrument: Instrument) -> Result<()> {
        let url = format!("{}/{}", self.ws_public_base, instrument.category.as_str());
        let mut backoff = 1u64;

        loop {
            info!("connecting to {} for {}", url, instrument.symbol);
            let (ws_stream, _) = match connect_async(&url).await {
                Ok(stream) => {
                    backoff = 1;
                    stream
                }
                Err(err) => {
                    warn!("connect failed: {}, retrying in {}s", err, backoff);
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(32);
                    continue;
                }
            };
            let (mut write, mut read) = ws_stream.split();

            let orderbook_topic = format!("orderbook.{}.{}", self.depth, instrument.symbol);
            let trades_topic = format!("publicTrade.{}", instrument.symbol);
            let subscribe = serde_json::json!({
                "op": "subscribe",
                "args": [orderbook_topic, trades_topic]
            });
            write
                .send(Message::Text(subscribe.to_string().into()))
                .await?;

            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Err(err) = self.handle_message(&instrument, &text).await {
                            debug!("market-data parse error: {}", err);
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Ok(Message::Close(frame)) => {
                        warn!("market-data stream closed: {:?}", frame);
                        break;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!("market-data stream error: {}", err);
                        break;
                    }
                }
            }

            warn!("reconnecting market-data stream in {}s", backoff);
            sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(32);
        }
    }

    async fn handle_message(&self, instrument: &Instrument, text: &str) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;
        if value.get("op").and_then(|v| v.as_str()) == Some("pong") {
            return Ok(());
        }

        let topic = match value.get("topic").and_then(|v| v.as_str()) {
            Some(topic) => topic,
            None => return Ok(()),
        };

        if topic.starts_with("orderbook.") {
            let snapshot = self.parse_orderbook(&value)?;
            self.sender
                .send(MarketEvent::OrderBook {
                    instrument: instrument.clone(),
                    snapshot,
                })
                .await
                .map_err(|_| anyhow!("market-data channel closed"))?;
        } else if topic.starts_with("publicTrade.") {
            let trades = self.parse_trades(&value)?;
            for trade in trades {
                self.sender
                    .send(MarketEvent::Trade {
                        instrument: instrument.clone(),
                        trade,
                    })
                    .await
                    .map_err(|_| anyhow!("market-data channel closed"))?;
            }
        }

        Ok(())
    }

    fn parse_orderbook(&self, value: &Value) -> Result<OrderBookSnapshot> {
        let data = value
            .get("data")
            .ok_or_else(|| anyhow!("missing orderbook data"))?;
        let bids = parse_levels(data.get("b"), self.depth);
        let asks = parse_levels(data.get("a"), self.depth);
        let timestamp = value
            .get("ts")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(now_ms);

        Ok(OrderBookSnapshot {
            bids,
            asks,
            timestamp,
        })
    }

    fn parse_trades(&self, value: &Value) -> Result<Vec<Trade>> {
        let data = value
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing trades data"))?;
        let mut trades = Vec::with_capacity(data.len());
        for entry in data {
            let price = parse_f64(entry.get("p").or_else(|| entry.get("price")))?;
            let qty = parse_f64(entry.get("v").or_else(|| entry.get("size")))?;
            let side_str = entry
                .get("S")
                .or_else(|| entry.get("side"))
                .and_then(|v| v.as_str())
                .unwrap_or("Buy");
            let side = if side_str.eq_ignore_ascii_case("buy") {
                Side::Buy
            } else {
                Side::Sell
            };
            let timestamp = entry
                .get("T")
                .or_else(|| entry.get("ts"))
                .and_then(|v| v.as_u64())
                .unwrap_or_else(now_ms);
            trades.push(Trade {
                price,
                qty,
                side,
                timestamp,
            });
        }
        Ok(trades)
    }
}

fn parse_levels(value: Option<&Value>, depth: usize) -> Vec<Level> {
    let mut levels = Vec::with_capacity(depth);
    let Some(array) = value.and_then(|v| v.as_array()) else {
        return levels;
    };
    for entry in array.iter().take(depth) {
        let Some(level) = entry.as_array() else {
            continue;
        };
        if level.len() < 2 {
            continue;
        }
        let price = parse_f64(level.get(0)).unwrap_or_default();
        let qty = parse_f64(level.get(1)).unwrap_or_default();
        levels.push(Level { price, qty });
    }
    levels
}

fn parse_f64(value: Option<&Value>) -> Result<f64> {
    let Some(value) = value else {
        return Ok(0.0);
    };
    if let Some(num) = value.as_f64() {
        return Ok(num);
    }
    if let Some(text) = value.as_str() {
        return text
            .parse::<f64>()
            .map_err(|_| anyhow!("invalid number: {}", text));
    }
    Ok(0.0)
}
