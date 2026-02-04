use anyhow::{anyhow, Result};
use std::fmt;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::AlgoConfig;
use crate::types::{Fill, Instrument, OrderRequest, OrderResponse, OrderType, Side};
use crate::utils::now_ms;

type HmacSha256 = Hmac<Sha256>;

pub const CODE_INSUFFICIENT_BALANCE: i64 = 110007;

#[derive(Debug)]
pub struct ExecutionError {
    pub code: i64,
    pub msg: String,
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "api error {}: {}", self.code, self.msg)
    }
}

impl std::error::Error for ExecutionError {}

#[derive(Clone)]
pub struct ExecutionEngine {
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window: u64,
    client: Client,
}

impl ExecutionEngine {
    pub fn new(config: &AlgoConfig) -> Self {
        Self {
            rest_base: config.rest_base.clone(),
            api_key: config.api_key.clone(),
            api_secret: config.api_secret.clone(),
            recv_window: config.recv_window,
            client: Client::new(),
        }
    }

    pub async fn place_order(&self, req: OrderRequest) -> Result<OrderResponse> {
        let order_type = match req.order_type {
            OrderType::Limit => "Limit",
            OrderType::Market => "Market",
        };
        let mut body = serde_json::json!({
            "category": req.instrument.category.as_str(),
            "symbol": req.instrument.symbol,
            "side": req.side.as_str(),
            "orderType": order_type,
            "qty": format_decimal(req.qty),
        });

        if let OrderType::Limit = req.order_type {
            let price = req
                .price
                .ok_or_else(|| anyhow!("limit order missing price"))?;
            body["price"] = Value::String(format_decimal(price));
            body["timeInForce"] = Value::String(if req.post_only {
                "PostOnly".to_string()
            } else {
                "GTC".to_string()
            });
        }

        if req.reduce_only {
            body["reduceOnly"] = Value::Bool(true);
        }

        let response = self
            .send_private_post("/v5/order/create", &body)
            .await?;
        let order_id = response
            .get("result")
            .and_then(|v| v.get("orderId"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("missing orderId in response"))?
            .to_string();

        info!("order placed {} {}", order_id, order_type);

        Ok(OrderResponse {
            order_id,
            status: "created".to_string(),
        })
    }

    pub async fn cancel_order(&self, instrument: &Instrument, order_id: &str) -> Result<()> {
        let body = serde_json::json!({
            "category": instrument.category.as_str(),
            "symbol": instrument.symbol,
            "orderId": order_id,
        });
        let _ = self.send_private_post("/v5/order/cancel", &body).await?;
        info!("order canceled {}", order_id);
        Ok(())
    }

    pub async fn fetch_executions(
        &self,
        instrument: &Instrument,
        limit: usize,
    ) -> Result<Vec<Fill>> {
        let query = format!(
            "category={}&symbol={}&limit={}",
            instrument.category.as_str(),
            instrument.symbol,
            limit
        );
        let response = self.send_private_get("/v5/execution/list", &query).await?;
        let mut fills = Vec::new();
        if let Some(list) = response.get("result").and_then(|v| v.get("list")).and_then(|v| v.as_array()) {
            for entry in list {
                if let (Some(order_id), Some(price), Some(qty), Some(side), Some(ts)) = (
                    entry.get("orderId").and_then(|v| v.as_str()),
                    parse_f64(entry.get("execPrice")),
                    parse_f64(entry.get("execQty")),
                    entry.get("side").and_then(|v| v.as_str()),
                    entry
                        .get("execTime")
                        .and_then(|v| v.as_u64())
                        .or_else(|| entry.get("time").and_then(|v| v.as_u64())),
                ) {
                    let side = if side.eq_ignore_ascii_case("buy") {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let exec_id = entry
                        .get("execId")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string());
                    fills.push(Fill {
                        exec_id,
                        order_id: order_id.to_string(),
                        price,
                        qty,
                        side,
                        timestamp: ts,
                    });
                }
            }
        }
        Ok(fills)
    }

    async fn send_private_post(&self, endpoint: &str, body: &Value) -> Result<Value> {
        let payload = serde_json::to_string(body)?;
        self.send_private_request("POST", endpoint, payload).await
    }

    async fn send_private_get(&self, endpoint: &str, query: &str) -> Result<Value> {
        self.send_private_request("GET", endpoint, query.to_string())
            .await
    }

    async fn send_private_request(
        &self,
        method: &str,
        endpoint: &str,
        payload: String,
    ) -> Result<Value> {
        let url = format!("{}{}", self.rest_base, endpoint);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let timestamp = now_ms();
            let signature = self.sign(timestamp, &payload)?;

            let builder = if method == "GET" {
                self.client
                    .get(&format!("{}?{}", url, payload))
            } else {
                self.client.post(&url).body(payload.clone())
            };

            let response = builder
                .header("X-BAPI-API-KEY", &self.api_key)
                .header("X-BAPI-SIGN", signature)
                .header("X-BAPI-TIMESTAMP", timestamp.to_string())
                .header("X-BAPI-RECV-WINDOW", self.recv_window.to_string())
                .header("Content-Type", "application/json")
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let text = resp.text().await?;
                    let value: Value = serde_json::from_str(&text)?;
                    let ret_code = value
                        .get("retCode")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(-1);
                    if ret_code == 0 {
                        return Ok(value);
                    }
                    let message = value
                        .get("retMsg")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    let api_error = ExecutionError {
                        code: ret_code,
                        msg: message.to_string(),
                    };
                    if ret_code == CODE_INSUFFICIENT_BALANCE || attempt >= 3 {
                        return Err(api_error.into());
                    }
                    warn!(
                        "api error {}, retrying attempt {}: {}",
                        ret_code, attempt, message
                    );
                }
                Err(err) => {
                    if attempt >= 3 {
                        return Err(anyhow!("request failed: {}", err));
                    }
                    warn!("request error, retrying attempt {}: {}", attempt, err);
                }
            }

            sleep(Duration::from_millis(200 * attempt as u64)).await;
        }
    }

    fn sign(&self, timestamp: u64, payload: &str) -> Result<String> {
        let pre_sign = format!(
            "{}{}{}{}",
            timestamp, self.api_key, self.recv_window, payload
        );
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|_| anyhow!("invalid api secret"))?;
        mac.update(pre_sign.as_bytes());
        let signature = mac.finalize().into_bytes();
        Ok(hex::encode(signature))
    }
}

fn format_decimal(value: f64) -> String {
    let mut text = format!("{:.8}", value);
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text.is_empty() {
        "0".to_string()
    } else {
        text
    }
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some(num) = value.as_f64() {
        return Some(num);
    }
    if let Some(text) = value.as_str() {
        return text.parse::<f64>().ok();
    }
    None
}
