use crate::config::InstrumentConfig;
use crate::types::{ExecutionFill, InstrumentType, LiveOrder, Side};
use crate::utils::now_millis;
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct OrderIntent {
    pub instrument: InstrumentType,
    pub symbol: String,
    pub side: Side,
    pub qty: f64,
    pub price: Option<f64>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub client_id: String,
    pub decision_price: f64,
}

#[derive(Clone, Debug)]
pub enum ExecutionAction {
    Place(OrderIntent),
    Cancel {
        instrument: InstrumentType,
        symbol: String,
        order_id: Option<String>,
        client_id: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub struct OrderAck {
    pub instrument: InstrumentType,
    pub symbol: String,
    pub order_id: String,
    pub client_id: String,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
}

#[derive(Clone, Debug)]
pub enum ExecutionUpdate {
    Order {
        instrument: InstrumentType,
        symbol: String,
        order_id: String,
        client_id: Option<String>,
        status: String,
        side: Side,
        price: f64,
        qty: f64,
        filled_qty: f64,
    },
    Fill(ExecutionFill),
}

#[derive(Clone)]
pub struct BybitClient {
    api_key: String,
    api_secret: String,
    recv_window: u64,
    base_url: String,
    http: reqwest::Client,
}

impl BybitClient {
    pub fn new(api_key: String, api_secret: String, recv_window: u64, base_url: String) -> Self {
        Self {
            api_key,
            api_secret,
            recv_window,
            base_url,
            http: reqwest::Client::new(),
        }
    }

    fn sign(&self, payload: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|_| anyhow!("invalid api secret"))?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn build_headers(&self, timestamp: u64, signature: &str) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert("X-BAPI-API-KEY", HeaderValue::from_str(&self.api_key)?);
        headers.insert("X-BAPI-TIMESTAMP", HeaderValue::from_str(&timestamp.to_string())?);
        headers.insert(
            "X-BAPI-RECV-WINDOW",
            HeaderValue::from_str(&self.recv_window.to_string())?,
        );
        headers.insert("X-BAPI-SIGN", HeaderValue::from_str(signature)?);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    pub async fn private_post(&self, endpoint: &str, body: &str) -> Result<Value> {
        let timestamp = now_millis();
        let payload = format!(
            "{}{}{}{}",
            timestamp, self.api_key, self.recv_window, body
        );
        let signature = self.sign(&payload)?;
        let url = format!("{}{}", self.base_url, endpoint);
        let response = self
            .http
            .post(url)
            .headers(self.build_headers(timestamp, &signature)?)
            .body(body.to_string())
            .send()
            .await?;
        let value: Value = response.json().await?;
        Ok(value)
    }
}

pub struct ExecutionEngine {
    client: BybitClient,
    instrument_lookup: InstrumentLookup,
}

impl ExecutionEngine {
    pub fn new(client: BybitClient, instruments: &[InstrumentConfig]) -> Self {
        let instrument_lookup = InstrumentLookup::new(instruments);
        Self {
            client,
            instrument_lookup,
        }
    }

    pub async fn handle_action(&self, action: ExecutionAction) -> Result<Option<OrderAck>> {
        match action {
            ExecutionAction::Place(intent) => self.place_order(intent).await.map(Some),
            ExecutionAction::Cancel {
                instrument,
                symbol,
                order_id,
                client_id,
            } => {
                self.cancel_order(instrument, &symbol, order_id, client_id)
                    .await?;
                Ok(None)
            }
        }
    }

    async fn place_order(&self, intent: OrderIntent) -> Result<OrderAck> {
        let order_type = if intent.price.is_some() { "Limit" } else { "Market" };
        let time_in_force = if intent.post_only { "PostOnly" } else { "IOC" };
        let symbol = intent.symbol.clone();
        let client_id = intent.client_id.clone();
        let mut payload = serde_json::json!({
            "category": intent.instrument.category(),
            "symbol": symbol,
            "side": intent.side.as_str(),
            "orderType": order_type,
            "qty": format!("{}", intent.qty),
            "timeInForce": time_in_force,
            "orderLinkId": client_id,
        });

        if let Some(price) = intent.price {
            payload["price"] = Value::String(format!("{}", price));
        }
        if intent.reduce_only {
            payload["reduceOnly"] = Value::Bool(true);
        }

        let body = payload.to_string();
        let response = self.client.private_post("/v5/order/create", &body).await?;
        let ret_code = response.get("retCode").and_then(Value::as_i64).unwrap_or(-1);
        if ret_code != 0 {
            return Err(anyhow!("order rejected: {}", response));
        }
        let result = response.get("result").ok_or_else(|| anyhow!("missing result"))?;
        let order_id = result
            .get("orderId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing orderId"))?
            .to_string();
        let response_client_id = result
            .get("orderLinkId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let resolved_client_id = if response_client_id.is_empty() {
            client_id.clone()
        } else {
            response_client_id
        };

        tracing::info!(
            instrument = ?intent.instrument,
            symbol = %symbol,
            side = ?intent.side,
            price = ?intent.price,
            qty = intent.qty,
            client_id = %resolved_client_id,
            order_id = %order_id,
            "placed order"
        );

        Ok(OrderAck {
            instrument: intent.instrument,
            symbol,
            order_id,
            client_id: resolved_client_id,
            side: intent.side,
            price: intent.price.unwrap_or_default(),
            qty: intent.qty,
        })
    }

    async fn cancel_order(
        &self,
        instrument: InstrumentType,
        symbol: &str,
        order_id: Option<String>,
        client_id: Option<String>,
    ) -> Result<()> {
        let mut payload = serde_json::json!({
            "category": instrument.category(),
            "symbol": symbol,
        });
        if let Some(order_id) = order_id {
            payload["orderId"] = Value::String(order_id.clone());
        }
        if let Some(client_id) = client_id {
            payload["orderLinkId"] = Value::String(client_id.clone());
        }

        let body = payload.to_string();
        let response = self.client.private_post("/v5/order/cancel", &body).await?;
        let ret_code = response.get("retCode").and_then(Value::as_i64).unwrap_or(-1);
        if ret_code != 0 {
            if ret_code == 170213 || ret_code == 170216 {
                tracing::info!(response = ?response, "cancel skipped (already gone)");
                return Ok(());
            }
            tracing::warn!(response = ?response, "cancel rejected");
            return Err(anyhow!("cancel rejected: {}", response));
        }
        tracing::info!(
            instrument = ?instrument,
            symbol = %symbol,
            "canceled order"
        );
        Ok(())
    }

    pub async fn spawn_private_stream(
        &self,
        ws_private_url: String,
        updates: Sender<ExecutionUpdate>,
    ) {
        let api_key = self.client.api_key.clone();
        let api_secret = self.client.api_secret.clone();
        let instrument_lookup = Arc::new(self.instrument_lookup.clone());

        tokio::spawn(async move {
            loop {
                if let Err(err) = run_private_stream(
                    &ws_private_url,
                    &api_key,
                    &api_secret,
                    instrument_lookup.clone(),
                    updates.clone(),
                )
                .await
                {
                    tracing::error!(error = ?err, "private stream failed, reconnecting");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        });
    }
}

async fn run_private_stream(
    ws_private_url: &str,
    api_key: &str,
    api_secret: &str,
    instrument_lookup: Arc<InstrumentLookup>,
    updates: Sender<ExecutionUpdate>,
) -> Result<()> {
    let (ws_stream, _) = connect_async(ws_private_url).await?;
    let (mut write, mut read) = ws_stream.split();

    let expires = now_millis() + 10_000;
    let sign_payload = format!("GET/realtime{}", expires);
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
        .map_err(|_| anyhow!("invalid secret"))?;
    mac.update(sign_payload.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let auth_msg = serde_json::json!({
        "op": "auth",
        "args": [api_key, expires, signature]
    });
    write.send(Message::Text(auth_msg.to_string())).await?;

    let sub_msg = serde_json::json!({
        "op": "subscribe",
        "args": ["order", "execution"]
    });
    write.send(Message::Text(sub_msg.to_string())).await?;

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
            if topic == "order" {
                if let Some(Value::Array(data)) = parsed.get("data") {
                    for item in data {
                        if let Some(update) = parse_order_update(item, &instrument_lookup) {
                            let _ = updates.send(update).await;
                        }
                    }
                }
            } else if topic == "execution" {
                if let Some(Value::Array(data)) = parsed.get("data") {
                    for item in data {
                        if let Some(update) = parse_execution_update(item, &instrument_lookup) {
                            let _ = updates.send(update).await;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn parse_order_update(
    item: &Value,
    instrument_lookup: &InstrumentLookup,
) -> Option<ExecutionUpdate> {
    let symbol = item.get("symbol")?.as_str()?.to_string();
    let category = item.get("category").and_then(Value::as_str);
    let instrument = instrument_lookup.resolve(category, &symbol)?;
    let order_id = item.get("orderId")?.as_str()?.to_string();
    let client_id = item.get("orderLinkId").and_then(Value::as_str).map(|s| s.to_string());
    let status = item.get("orderStatus").and_then(Value::as_str).unwrap_or("").to_string();
    let side = parse_side(item.get("side")?.as_str()?);
    let price = parse_f64(item.get("price")).unwrap_or(0.0);
    let qty = parse_f64(item.get("qty")).unwrap_or(0.0);
    let filled_qty = parse_f64(item.get("cumExecQty")).unwrap_or(0.0);

    Some(ExecutionUpdate::Order {
        instrument,
        symbol,
        order_id,
        client_id,
        status,
        side,
        price,
        qty,
        filled_qty,
    })
}

fn parse_execution_update(
    item: &Value,
    instrument_lookup: &InstrumentLookup,
) -> Option<ExecutionUpdate> {
    let symbol = item.get("symbol")?.as_str()?.to_string();
    let category = item.get("category").and_then(Value::as_str);
    let instrument = instrument_lookup.resolve(category, &symbol)?;
    let order_id = item.get("orderId")?.as_str()?.to_string();
    let client_id = item.get("orderLinkId").and_then(Value::as_str).map(|s| s.to_string());
    let side = parse_side(item.get("side")?.as_str()?);
    let price = parse_f64(item.get("execPrice").or(item.get("price"))).unwrap_or(0.0);
    let qty = parse_f64(item.get("execQty").or(item.get("qty"))).unwrap_or(0.0);
    let timestamp = item
        .get("execTime")
        .and_then(Value::as_u64)
        .unwrap_or_else(now_millis);

    Some(ExecutionUpdate::Fill(ExecutionFill {
        order_id,
        client_id,
        instrument,
        symbol,
        price,
        qty,
        side,
        timestamp,
    }))
}

fn parse_side(value: &str) -> Side {
    if value.eq_ignore_ascii_case("buy") {
        Side::Buy
    } else {
        Side::Sell
    }
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(num) => num.as_f64(),
        _ => None,
    }
}

#[derive(Clone)]
struct InstrumentLookup {
    by_category_symbol: HashMap<(String, String), InstrumentType>,
    by_symbol: HashMap<String, InstrumentType>,
    ambiguous_symbols: HashSet<String>,
}

impl InstrumentLookup {
    fn new(instruments: &[InstrumentConfig]) -> Self {
        let mut by_category_symbol = HashMap::new();
        let mut by_symbol = HashMap::new();
        let mut ambiguous_symbols = HashSet::new();

        for inst in instruments {
            by_category_symbol.insert(
                (inst.instrument_type.category().to_string(), inst.symbol.clone()),
                inst.instrument_type,
            );

            if let Some(existing) = by_symbol.get(&inst.symbol) {
                if *existing != inst.instrument_type {
                    ambiguous_symbols.insert(inst.symbol.clone());
                    by_symbol.remove(&inst.symbol);
                }
            } else if !ambiguous_symbols.contains(&inst.symbol) {
                by_symbol.insert(inst.symbol.clone(), inst.instrument_type);
            }
        }

        Self {
            by_category_symbol,
            by_symbol,
            ambiguous_symbols,
        }
    }

    fn resolve(&self, category: Option<&str>, symbol: &str) -> Option<InstrumentType> {
        if let Some(category) = category {
            if let Some(inst) = self
                .by_category_symbol
                .get(&(category.to_string(), symbol.to_string()))
            {
                return Some(*inst);
            }
        }

        if self.ambiguous_symbols.contains(symbol) {
            tracing::warn!(
                symbol = %symbol,
                "ambiguous symbol without category; skipping update"
            );
            return None;
        }

        self.by_symbol.get(symbol).copied()
    }
}

impl LiveOrder {
    pub fn from_ack(ack: &OrderAck) -> Self {
        LiveOrder {
            order_id: ack.order_id.clone(),
            client_id: ack.client_id.clone(),
            price: ack.price,
            qty: ack.qty,
            side: ack.side,
            created_at: now_millis(),
        }
    }
}
