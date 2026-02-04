use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum InstrumentType {
    Spot,
    Perp,
    Option,
}

impl InstrumentType {
    pub fn category(&self) -> &'static str {
        match self {
            InstrumentType::Spot => "spot",
            InstrumentType::Perp => "linear",
            InstrumentType::Option => "option",
        }
    }

    pub fn ws_public_path(&self) -> &'static str {
        match self {
            InstrumentType::Spot => "spot",
            InstrumentType::Perp => "linear",
            InstrumentType::Option => "option",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        }
    }

    pub fn sign(&self) -> f64 {
        match self {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }

    pub fn opposite(&self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    pub fn from_str(value: &str) -> Result<Side, String> {
        match value.to_lowercase().as_str() {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            _ => Err(format!("invalid side: {}", value)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderBookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Clone, Debug)]
pub struct OrderBookSnapshot {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: u64,
}

impl OrderBookSnapshot {
    pub fn best_bid(&self) -> Option<&OrderBookLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&OrderBookLevel> {
        self.asks.first()
    }

    pub fn mid_price(&self) -> Option<f64> {
        let bid = self.best_bid()?.price;
        let ask = self.best_ask()?.price;
        Some((bid + ask) / 2.0)
    }

    pub fn spread(&self) -> Option<f64> {
        let bid = self.best_bid()?.price;
        let ask = self.best_ask()?.price;
        Some(ask - bid)
    }
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub timestamp: u64,
}

#[derive(Clone, Debug)]
pub struct MarketDataEvent {
    pub instrument: InstrumentType,
    pub symbol: String,
    pub kind: MarketDataKind,
}

#[derive(Clone, Debug)]
pub enum MarketDataKind {
    OrderBook(OrderBookSnapshot),
    Trade(Trade),
}

#[derive(Clone, Debug)]
pub struct LiveOrder {
    pub order_id: String,
    pub client_id: String,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub created_at: u64,
}

#[derive(Clone, Debug)]
pub struct ExecutionFill {
    pub order_id: String,
    pub client_id: Option<String>,
    pub instrument: InstrumentType,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub timestamp: u64,
}
