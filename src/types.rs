use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    Spot,
    Linear,
    Option,
}

impl Category {
    pub fn as_str(&self) -> &'static str {
        match self {
            Category::Spot => "spot",
            Category::Linear => "linear",
            Category::Option => "option",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Instrument {
    pub symbol: String,
    pub category: Category,
}

#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OrderBookSnapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Trade {
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    OrderBook {
        instrument: Instrument,
        snapshot: OrderBookSnapshot,
    },
    Trade {
        instrument: Instrument,
        trade: Trade,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub instrument: Instrument,
    pub side: Side,
    pub qty: f64,
    pub price: Option<f64>,
    pub order_type: OrderType,
    pub post_only: bool,
    pub reduce_only: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OrderResponse {
    pub order_id: String,
    pub status: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Fill {
    pub exec_id: Option<String>,
    pub order_id: String,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionTarget {
    pub instrument: Instrument,
    pub side: Side,
    pub total_qty: f64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ActiveOrder {
    pub order_id: String,
    pub instrument: Instrument,
    pub side: Side,
    pub qty: f64,
    pub price: f64,
    pub post_only: bool,
    pub placed_at: u64,
    pub queue_ahead: f64,
    pub last_level_qty: f64,
}
