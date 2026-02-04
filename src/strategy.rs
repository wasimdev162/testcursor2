use crate::config::InstrumentConfig;
use crate::execution::{ExecutionAction, ExecutionUpdate, OrderAck, OrderIntent};
use crate::types::{
    ExecutionFill, InstrumentType, LiveOrder, MarketDataEvent, MarketDataKind, OrderBookSnapshot,
    Side, Trade,
};
use crate::utils::{clamp, generate_client_id, now_millis, RollingWindow};
use std::collections::HashMap;

pub struct StrategyManager {
    strategies: HashMap<(InstrumentType, String), InstrumentStrategy>,
}

impl StrategyManager {
    pub fn new(instruments: Vec<InstrumentConfig>) -> Self {
        let mut strategies = HashMap::new();
        for instrument in instruments {
            strategies.insert(
                (instrument.instrument_type, instrument.symbol.clone()),
                InstrumentStrategy::new(instrument),
            );
        }
        Self { strategies }
    }

    pub fn on_market_event(&mut self, event: MarketDataEvent) -> Vec<ExecutionAction> {
        let key = (event.instrument, event.symbol.clone());
        let Some(strategy) = self.strategies.get_mut(&key) else {
            return Vec::new();
        };
        match event.kind {
            MarketDataKind::OrderBook(book) => strategy.on_orderbook(book),
            MarketDataKind::Trade(trade) => strategy.on_trade(trade),
        }
    }

    pub fn on_execution_update(&mut self, update: &ExecutionUpdate) {
        match update {
            ExecutionUpdate::Order { instrument, symbol, .. } => {
                if let Some(strategy) = self.strategies.get_mut(&(*instrument, symbol.clone())) {
                    strategy.on_order_update(update);
                }
            }
            ExecutionUpdate::Fill(fill) => {
                if let Some(strategy) =
                    self.strategies.get_mut(&(fill.instrument, fill.symbol.clone()))
                {
                    strategy.on_fill(fill);
                }
            }
        }
    }

    pub fn on_order_ack(&mut self, ack: &OrderAck) {
        if let Some(strategy) =
            self.strategies
                .get_mut(&(ack.instrument, ack.symbol.clone()))
        {
            strategy.on_order_ack(ack);
        }
    }

    pub fn on_timer(&mut self) -> Vec<ExecutionAction> {
        let mut actions = Vec::new();
        for strategy in self.strategies.values_mut() {
            actions.extend(strategy.on_timer());
        }
        actions
    }
}

struct OrderTracker {
    order: LiveOrder,
    queue_ahead: f64,
    last_update_ts: u64,
}

struct SignalState {
    spread_window: RollingWindow,
    mid_window: RollingWindow,
    trade_flow_window: RollingWindow,
    trade_qty_window: RollingWindow,
    last_large_trade_side: Option<Side>,
    last_large_trade_ts: u64,
    bid_persistence: u32,
    ask_persistence: u32,
    last_best_bid_size: f64,
    last_best_ask_size: f64,
}

impl SignalState {
    fn new() -> Self {
        Self {
            spread_window: RollingWindow::new(40),
            mid_window: RollingWindow::new(40),
            trade_flow_window: RollingWindow::new(50),
            trade_qty_window: RollingWindow::new(50),
            last_large_trade_side: None,
            last_large_trade_ts: 0,
            bid_persistence: 0,
            ask_persistence: 0,
            last_best_bid_size: 0.0,
            last_best_ask_size: 0.0,
        }
    }

    fn update_orderbook(&mut self, book: &OrderBookSnapshot) {
        if let (Some(spread), Some(mid)) = (book.spread(), book.mid_price()) {
            self.spread_window.push(spread);
            self.mid_window.push(mid);
        }

        let avg_size = average_top_size(book, 3).unwrap_or(0.0);
        if let Some(best_bid) = book.best_bid() {
            let is_large = avg_size > 0.0 && best_bid.size > avg_size * 3.0;
            let stable = (best_bid.size - self.last_best_bid_size).abs()
                <= self.last_best_bid_size * 0.05;
            if is_large && stable {
                self.bid_persistence += 1;
            } else {
                self.bid_persistence = 0;
            }
            self.last_best_bid_size = best_bid.size;
        }

        if let Some(best_ask) = book.best_ask() {
            let is_large = avg_size > 0.0 && best_ask.size > avg_size * 3.0;
            let stable = (best_ask.size - self.last_best_ask_size).abs()
                <= self.last_best_ask_size * 0.05;
            if is_large && stable {
                self.ask_persistence += 1;
            } else {
                self.ask_persistence = 0;
            }
            self.last_best_ask_size = best_ask.size;
        }
    }

    fn update_trade(&mut self, trade: &Trade) {
        let signed_qty = trade.qty * trade.side.sign();
        self.trade_flow_window.push(signed_qty);
        self.trade_qty_window.push(trade.qty);

        let avg_qty = self.trade_qty_window.mean().unwrap_or(0.0);
        if avg_qty > 0.0 && trade.qty > avg_qty * 3.0 {
            self.last_large_trade_side = Some(trade.side);
            self.last_large_trade_ts = trade.timestamp;
        }
    }

    fn trade_flow_imbalance(&self) -> f64 {
        let signed = self.trade_flow_window.sum();
        let total = self.trade_flow_window.abs_sum();
        if total == 0.0 {
            0.0
        } else {
            signed / total
        }
    }

    fn momentum(&self) -> f64 {
        self.mid_window.slope().unwrap_or(0.0)
    }

    fn spread_widen_score(&self) -> f64 {
        let current = self.spread_window.last().unwrap_or(0.0);
        let mean = self.spread_window.mean().unwrap_or(0.0);
        if mean == 0.0 {
            0.0
        } else {
            (current - mean) / mean
        }
    }

    fn mean_reversion_signal(&self, now: u64) -> f64 {
        if let Some(side) = self.last_large_trade_side {
            if now - self.last_large_trade_ts < 1200 {
                return -side.sign();
            }
        }
        0.0
    }

    fn iceberg_bias(&self) -> f64 {
        let bid_iceberg = self.bid_persistence >= 3;
        let ask_iceberg = self.ask_persistence >= 3;
        match (bid_iceberg, ask_iceberg) {
            (true, false) => 0.6,
            (false, true) => -0.6,
            _ => 0.0,
        }
    }

    fn toxicity_score(&self) -> f64 {
        let flow = self.trade_flow_imbalance();
        let momentum = self.momentum();
        if flow.signum() == momentum.signum() && flow.abs() > 0.15 {
            flow.abs()
        } else {
            0.0
        }
    }
}

pub struct InstrumentStrategy {
    config: InstrumentConfig,
    remaining_qty: f64,
    start_ts: u64,
    last_decision_ts: u64,
    orderbook: Option<OrderBookSnapshot>,
    tracker: Option<OrderTracker>,
    signals: SignalState,
}

impl InstrumentStrategy {
    pub fn new(config: InstrumentConfig) -> Self {
        Self {
            remaining_qty: config.target_qty,
            start_ts: now_millis(),
            last_decision_ts: 0,
            orderbook: None,
            tracker: None,
            signals: SignalState::new(),
            config,
        }
    }

    pub fn on_orderbook(&mut self, book: OrderBookSnapshot) -> Vec<ExecutionAction> {
        self.signals.update_orderbook(&book);
        self.orderbook = Some(book);
        self.maybe_act()
    }

    pub fn on_trade(&mut self, trade: Trade) -> Vec<ExecutionAction> {
        self.signals.update_trade(&trade);
        self.update_queue_position(&trade);
        self.maybe_act()
    }

    pub fn on_timer(&mut self) -> Vec<ExecutionAction> {
        self.maybe_act()
    }

    pub fn on_order_ack(&mut self, ack: &OrderAck) {
        self.tracker = Some(OrderTracker {
            order: LiveOrder::from_ack(ack),
            queue_ahead: self.estimate_queue_ahead(ack.side, ack.price),
            last_update_ts: now_millis(),
        });
    }

    pub fn on_order_update(&mut self, update: &ExecutionUpdate) {
        if let ExecutionUpdate::Order {
            status,
            filled_qty,
            ..
        } = update
        {
            if status.eq_ignore_ascii_case("Cancelled")
                || status.eq_ignore_ascii_case("Filled")
                || status.eq_ignore_ascii_case("Rejected")
            {
                self.tracker = None;
            } else if let Some(tracker) = self.tracker.as_mut() {
                if *filled_qty >= tracker.order.qty {
                    self.tracker = None;
                }
            }
        }
    }

    pub fn on_fill(&mut self, fill: &ExecutionFill) {
        self.remaining_qty = (self.remaining_qty - fill.qty).max(0.0);
        if let Some(tracker) = self.tracker.as_mut() {
            if tracker.order.order_id == fill.order_id {
                tracker.order.qty = (tracker.order.qty - fill.qty).max(0.0);
                if tracker.order.qty <= 0.0 {
                    self.tracker = None;
                }
            }
        }
    }

    fn update_queue_position(&mut self, trade: &Trade) {
        let Some(tracker) = self.tracker.as_mut() else {
            return;
        };
        if tracker.order.side == Side::Buy && trade.side == Side::Sell {
            if trade.price <= tracker.order.price {
                tracker.queue_ahead = (tracker.queue_ahead - trade.qty).max(0.0);
            }
        } else if tracker.order.side == Side::Sell && trade.side == Side::Buy {
            if trade.price >= tracker.order.price {
                tracker.queue_ahead = (tracker.queue_ahead - trade.qty).max(0.0);
            }
        }
        tracker.last_update_ts = trade.timestamp;
    }

    fn maybe_act(&mut self) -> Vec<ExecutionAction> {
        let now = now_millis();
        if self.remaining_qty <= 0.0 {
            if let Some(tracker) = self.tracker.take() {
                return vec![ExecutionAction::Cancel {
                    instrument: self.config.instrument_type,
                    symbol: self.config.symbol.clone(),
                    order_id: Some(tracker.order.order_id),
                    client_id: Some(tracker.order.client_id),
                }];
            }
            return Vec::new();
        }

        if self.last_decision_ts != 0 && now - self.last_decision_ts < 150 {
            return Vec::new();
        }

        let book = match self.orderbook.as_ref() {
            Some(book) => book,
            None => return Vec::new(),
        };

        let spread = book.spread().unwrap_or(0.0);
        let mid = book.mid_price().unwrap_or(0.0);
        let imbalance = orderbook_imbalance(book, 5);
        let flow = self.signals.trade_flow_imbalance();
        let momentum = self.signals.momentum();
        let mean_rev = self.signals.mean_reversion_signal(now);
        let iceberg = self.signals.iceberg_bias();
        let spread_widen = self.signals.spread_widen_score();
        let toxicity = self.signals.toxicity_score();

        let raw_alpha =
            0.45 * imbalance + 0.25 * flow + 0.15 * momentum + 0.1 * mean_rev + 0.05 * iceberg;
        let side_alpha = raw_alpha * self.config.side.sign();

        let urgency = clamp(
            (now - self.start_ts) as f64 / self.config.execution_horizon_ms as f64,
            0.0,
            1.0,
        );

        let queue_pressure = self
            .tracker
            .as_ref()
            .map(|tracker| tracker.queue_ahead)
            .unwrap_or(0.0);
        let flow_pressure = self.signals.trade_flow_window.abs_sum();
        let fill_prob = if queue_pressure + flow_pressure > 0.0 {
            1.0 - queue_pressure / (queue_pressure + flow_pressure)
        } else {
            0.5
        };

        let should_cancel = self.tracker.as_ref().map_or(false, |tracker| {
            let age = now - tracker.order.created_at;
            let price_moved = best_price(book, tracker.order.side)
                .map(|best| (best - tracker.order.price).abs() > spread * 0.1)
                .unwrap_or(false);
            age > self.config.max_order_age_ms
                || toxicity > 0.35
                || spread_widen > 0.5
                || (side_alpha < -0.1 && price_moved)
        });

        let mut actions = Vec::new();
        if should_cancel {
            if let Some(tracker) = self.tracker.take() {
                actions.push(ExecutionAction::Cancel {
                    instrument: self.config.instrument_type,
                    symbol: self.config.symbol.clone(),
                    order_id: Some(tracker.order.order_id),
                    client_id: Some(tracker.order.client_id),
                });
            }
        }

        let wants_aggressive = urgency > 0.7 || side_alpha < -0.08 || fill_prob < 0.2;
        let wants_passive = side_alpha > 0.05 && toxicity < 0.2 && spread_widen < 0.6;

        if self.tracker.is_none() && (wants_aggressive || wants_passive) {
            let use_aggressive = wants_aggressive && (!wants_passive || urgency > 0.85);
            let (price, post_only) = if use_aggressive {
                (best_price(book, self.config.side.opposite()), false)
            } else {
                (best_price(book, self.config.side), true)
            };

            if let Some(price) = price {
                let child_qty = self.child_qty(book, post_only);
                if child_qty > 0.0 {
                    let client_id = generate_client_id(&self.config.name);
                    let decision_price = mid;
                    actions.push(ExecutionAction::Place(OrderIntent {
                        instrument: self.config.instrument_type,
                        symbol: self.config.symbol.clone(),
                        side: self.config.side,
                        qty: child_qty,
                        price: if post_only { Some(price) } else { Some(price) },
                        post_only,
                        reduce_only: false,
                        client_id,
                        decision_price,
                    }));
                }
            }
        }

        if !actions.is_empty() {
            self.last_decision_ts = now;
            tracing::info!(
                instrument = ?self.config.instrument_type,
                symbol = %self.config.symbol,
                side = ?self.config.side,
                remaining = self.remaining_qty,
                alpha = side_alpha,
                urgency = urgency,
                spread = spread,
                "strategy decision"
            );
        }

        actions
    }

    fn child_qty(&self, book: &OrderBookSnapshot, post_only: bool) -> f64 {
        let visible_liquidity = if post_only {
            depth_liquidity(book, self.config.side, 3)
        } else {
            depth_liquidity(book, self.config.side.opposite(), 3)
        };
        if visible_liquidity <= 0.0 {
            return 0.0;
        }
        let target = visible_liquidity * self.config.max_child_pct;
        let qty = target.max(self.config.min_child_qty).min(self.remaining_qty);
        qty
    }

    fn estimate_queue_ahead(&self, side: Side, price: f64) -> f64 {
        let Some(book) = self.orderbook.as_ref() else {
            return 0.0;
        };
        let levels = if side == Side::Buy {
            &book.bids
        } else {
            &book.asks
        };
        levels
            .iter()
            .find(|level| level.price == price)
            .map(|level| level.size)
            .unwrap_or(0.0)
    }
}

fn orderbook_imbalance(book: &OrderBookSnapshot, depth: usize) -> f64 {
    let bid_vol: f64 = book.bids.iter().take(depth).map(|level| level.size).sum();
    let ask_vol: f64 = book.asks.iter().take(depth).map(|level| level.size).sum();
    if bid_vol + ask_vol == 0.0 {
        0.0
    } else {
        (bid_vol - ask_vol) / (bid_vol + ask_vol)
    }
}

fn average_top_size(book: &OrderBookSnapshot, depth: usize) -> Option<f64> {
    let mut sizes: Vec<f64> = book
        .bids
        .iter()
        .take(depth)
        .chain(book.asks.iter().take(depth))
        .map(|level| level.size)
        .collect();
    if sizes.is_empty() {
        return None;
    }
    sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(sizes.iter().sum::<f64>() / sizes.len() as f64)
}

fn depth_liquidity(book: &OrderBookSnapshot, side: Side, depth: usize) -> f64 {
    let levels = if side == Side::Buy {
        &book.bids
    } else {
        &book.asks
    };
    levels.iter().take(depth).map(|level| level.size).sum()
}

fn best_price(book: &OrderBookSnapshot, side: Side) -> Option<f64> {
    match side {
        Side::Buy => book.best_bid().map(|level| level.price),
        Side::Sell => book.best_ask().map(|level| level.price),
    }
}
