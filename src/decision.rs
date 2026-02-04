use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::config::AlgoConfig;
use crate::execution::ExecutionEngine;
use crate::performance::PerformanceTracker;
use crate::types::{
    ActiveOrder, ExecutionTarget, Instrument, MarketEvent, OrderRequest, OrderType,
    OrderBookSnapshot, Side, Trade,
};
use crate::utils::{now_ms, price_key};

#[derive(Debug)]
struct IcebergInfo {
    last_qty: f64,
    replenishments: u32,
}

#[derive(Debug)]
struct RollingStats {
    window: usize,
    values: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
}

impl RollingStats {
    fn new(window: usize) -> Self {
        Self {
            window,
            values: VecDeque::with_capacity(window),
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    fn push(&mut self, value: f64) {
        self.values.push_back(value);
        self.sum += value;
        self.sum_sq += value * value;
        if self.values.len() > self.window {
            if let Some(old) = self.values.pop_front() {
                self.sum -= old;
                self.sum_sq -= old * old;
            }
        }
    }

    fn mean(&self) -> Option<f64> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.sum / self.values.len() as f64)
        }
    }

    fn std_dev(&self) -> Option<f64> {
        let mean = self.mean()?;
        let n = self.values.len() as f64;
        if n < 2.0 {
            return Some(0.0);
        }
        let variance = (self.sum_sq / n) - mean * mean;
        Some(variance.max(0.0).sqrt())
    }

    fn zscore(&self, value: f64) -> Option<f64> {
        let mean = self.mean()?;
        let std = self.std_dev()?;
        if std > 0.0 {
            Some((value - mean) / std)
        } else {
            Some(0.0)
        }
    }
}

#[derive(Debug)]
struct InstrumentState {
    instrument: Instrument,
    side: Side,
    target_qty: f64,
    remaining_qty: f64,
    order_book: Option<OrderBookSnapshot>,
    trades: VecDeque<Trade>,
    spread_stats: RollingStats,
    mid_stats: RollingStats,
    iceberg_map: HashMap<u64, IcebergInfo>,
    active_orders: HashMap<String, ActiveOrder>,
    quote_window_start: u64,
    quote_updates: u64,
}

impl InstrumentState {
    fn new(target: &ExecutionTarget, spread_window: usize, trade_window: usize) -> Self {
        Self {
            instrument: target.instrument.clone(),
            side: target.side,
            target_qty: target.total_qty,
            remaining_qty: target.total_qty,
            order_book: None,
            trades: VecDeque::with_capacity(trade_window),
            spread_stats: RollingStats::new(spread_window),
            mid_stats: RollingStats::new(spread_window),
            iceberg_map: HashMap::new(),
            active_orders: HashMap::new(),
            quote_window_start: now_ms(),
            quote_updates: 0,
        }
    }
}

#[derive(Debug)]
struct MicrostructureSignals {
    mid: f64,
    spread: f64,
    imbalance: f64,
    trade_flow_imbalance: f64,
    momentum: f64,
    mean_reversion: f64,
    toxicity: f64,
    spread_z: f64,
    iceberg_score: f64,
    volatility: f64,
    quote_stuffing: bool,
}

pub struct DecisionEngine {
    config: AlgoConfig,
    execution: ExecutionEngine,
    performance: PerformanceTracker,
    states: HashMap<String, InstrumentState>,
    start_time: u64,
    seen_fills: HashSet<String>,
    poll_counter: u64,
}

impl DecisionEngine {
    pub fn new(config: AlgoConfig, execution: ExecutionEngine) -> Self {
        let performance = PerformanceTracker::new(&config.targets);
        let mut states = HashMap::new();
        for target in &config.targets {
            states.insert(
                instrument_key(&target.instrument),
                InstrumentState::new(target, config.spread_window, config.trade_window),
            );
        }
        Self {
            config,
            execution,
            performance,
            states,
            start_time: now_ms(),
            seen_fills: HashSet::new(),
            poll_counter: 0,
        }
    }

    pub async fn run(&mut self, mut receiver: Receiver<MarketEvent>) -> Result<()> {
        let mut ticker = interval(Duration::from_millis(self.config.decision_interval_ms));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.on_tick().await?;
                    if self.is_complete() {
                        info!("execution complete for all instruments");
                        break;
                    }
                }
                maybe_event = receiver.recv() => {
                    if let Some(event) = maybe_event {
                        self.on_event(event).await?;
                    } else {
                        warn!("market-data channel closed");
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_event(&mut self, event: MarketEvent) -> Result<()> {
        match event {
            MarketEvent::OrderBook { instrument, snapshot } => {
                if let Some(state) = self.states.get_mut(&instrument_key(&instrument)) {
                    update_order_book(state, snapshot);
                }
            }
            MarketEvent::Trade { instrument, trade } => {
                if let Some(state) = self.states.get_mut(&instrument_key(&instrument)) {
                    update_trades(state, trade.clone(), self.config.trade_window);
                    self.performance.record_trade(&instrument, &trade);
                }
            }
        }
        Ok(())
    }

    async fn on_tick(&mut self) -> Result<()> {
        self.poll_counter += 1;
        let now = now_ms();
        let elapsed = now.saturating_sub(self.start_time);
        let urgency = (elapsed as f64 / self.config.horizon_ms as f64).min(1.0);
        let config = &self.config;
        let execution = self.execution.clone();
        let performance = &mut self.performance;
        let states = &mut self.states;
        let seen_fills = &mut self.seen_fills;
        let poll_counter = self.poll_counter;

        for state in states.values_mut() {
            cancel_stale_orders(&execution, config, state, now).await?;
            if poll_counter % 2 == 0 {
                poll_fills(&execution, performance, seen_fills, state).await?;
            }
            if state.remaining_qty <= 0.0 {
                continue;
            }
            let Some(signals) = compute_signals(state) else {
                continue;
            };
            if !state.active_orders.is_empty() {
                continue;
            }

            if signals.quote_stuffing && urgency < 0.7 {
                debug!("quote stuffing detected, waiting: {}", state.instrument.symbol);
                continue;
            }

            let top_qty = best_level_qty(&signals, state);
            let child_qty = (state.remaining_qty)
                .min(config.max_participation * top_qty)
                .max((state.remaining_qty * 0.1).min(top_qty));

            if child_qty <= 0.0 {
                continue;
            }

            let action = decide_action(config, state, &signals, urgency);
            if let Some((order_type, price, post_only)) = action {
                place_child_order(
                    &execution,
                    performance,
                    state,
                    order_type,
                    price,
                    child_qty,
                    post_only,
                    signals.mid,
                )
                .await?;
            }
        }

        if elapsed > config.horizon_ms {
            force_finish(&execution, performance, states).await?;
        }

        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.states
            .values()
            .all(|state| state.remaining_qty <= 0.0)
    }

    pub fn metrics_summary(&self) -> Vec<(String, crate::performance::InstrumentMetrics)> {
        self.performance.metrics_summary()
    }
}

fn update_order_book(state: &mut InstrumentState, snapshot: OrderBookSnapshot) {
    let now = now_ms();
    if now.saturating_sub(state.quote_window_start) > 1000 {
        state.quote_window_start = now;
        state.quote_updates = 0;
    }
    state.quote_updates += 1;

    if let (Some(bid), Some(ask)) = (snapshot.bids.first(), snapshot.asks.first()) {
        let spread = (ask.price - bid.price).max(0.0);
        let mid = (ask.price + bid.price) * 0.5;
        state.spread_stats.push(spread);
        state.mid_stats.push(mid);
    }

    update_queue_positions(state, &snapshot);
    update_icebergs(state, &snapshot);
    state.order_book = Some(snapshot);
}

fn update_trades(state: &mut InstrumentState, trade: Trade, trade_window: usize) {
    state.trades.push_back(trade);
    while state.trades.len() > trade_window {
        state.trades.pop_front();
    }
}

fn update_queue_positions(state: &mut InstrumentState, snapshot: &OrderBookSnapshot) {
    for order in state.active_orders.values_mut() {
        let level_qty = find_level_qty(snapshot, order.price, order.side);
        if let Some(level_qty) = level_qty {
            if level_qty < order.last_level_qty {
                let filled_ahead = order.last_level_qty - level_qty;
                order.queue_ahead = (order.queue_ahead - filled_ahead).max(0.0);
            }
            order.last_level_qty = level_qty;
        }
    }
}

fn update_icebergs(state: &mut InstrumentState, snapshot: &OrderBookSnapshot) {
    if let Some(level) = snapshot.bids.first() {
        update_iceberg_entry(state, level.price, level.qty);
    }
    if let Some(level) = snapshot.asks.first() {
        update_iceberg_entry(state, level.price, level.qty);
    }
}

fn compute_signals(state: &InstrumentState) -> Option<MicrostructureSignals> {
    let book = state.order_book.as_ref()?;
    let best_bid = book.bids.first()?;
    let best_ask = book.asks.first()?;
    let mid = (best_bid.price + best_ask.price) * 0.5;
    let spread = (best_ask.price - best_bid.price).max(0.0);
    let imbalance = compute_imbalance(book);
    let (trade_flow, momentum, mean_reversion) = compute_trade_signals(&state.trades);

    let mut toxicity = trade_flow.abs();
    if momentum.abs() > 0.0005 {
        toxicity += momentum.abs();
    }
    toxicity = toxicity.min(1.5);

    let spread_z = state.spread_stats.zscore(spread).unwrap_or(0.0);
    let iceberg_score = compute_iceberg_score(state);
    let volatility = state.mid_stats.std_dev().unwrap_or(0.0);
    let quote_rate = if now_ms().saturating_sub(state.quote_window_start) > 0 {
        state.quote_updates as f64
    } else {
        0.0
    };

    let quote_stuffing = quote_rate > 50.0 && spread_z > 0.5;

    Some(MicrostructureSignals {
        mid,
        spread,
        imbalance,
        trade_flow_imbalance: trade_flow,
        momentum,
        mean_reversion,
        toxicity,
        spread_z,
        iceberg_score,
        volatility,
        quote_stuffing,
    })
}

fn decide_action(
    config: &AlgoConfig,
    state: &InstrumentState,
    signals: &MicrostructureSignals,
    urgency: f64,
) -> Option<(OrderType, f64, bool)> {
    let side = state.side;
    let alpha = compute_alpha(signals);
    let spread_wide = signals.spread_z > 1.2;
    let toxic = signals.toxicity > config.toxicity_threshold;
    let cross_bias = urgency > config.max_cross_urgency;

    let expect_up = alpha > config.min_alpha;
    let expect_down = alpha < -config.min_alpha;

    let (cross_now, prefer_passive) = match side {
        Side::Buy => (expect_up || cross_bias || toxic, expect_down && !spread_wide),
        Side::Sell => (expect_down || cross_bias || toxic, expect_up && !spread_wide),
    };

    if cross_now && !signals.quote_stuffing {
        return Some((OrderType::Market, signals.mid, false));
    }

    if prefer_passive && signals.iceberg_score < 0.6 {
        let price = match side {
            Side::Buy => signals.mid - signals.spread * 0.5,
            Side::Sell => signals.mid + signals.spread * 0.5,
        };
        return Some((OrderType::Limit, price, true));
    }

    if !spread_wide {
        let price = match side {
            Side::Buy => signals.mid - signals.spread * 0.25,
            Side::Sell => signals.mid + signals.spread * 0.25,
        };
        return Some((OrderType::Limit, price, true));
    }

    None
}

async fn place_child_order(
    execution: &ExecutionEngine,
    performance: &mut PerformanceTracker,
    state: &mut InstrumentState,
    order_type: OrderType,
    price: f64,
    qty: f64,
    post_only: bool,
    decision_price: f64,
) -> Result<()> {
    let request = OrderRequest {
        instrument: state.instrument.clone(),
        side: state.side,
        qty,
        price: if let OrderType::Limit = order_type {
            Some(price)
        } else {
            None
        },
        order_type,
        post_only,
        reduce_only: false,
    };

    let response = execution.place_order(request).await?;
    let placed_at = now_ms();
    if let Some(book) = &state.order_book {
        let level_qty = find_level_qty(book, price, state.side).unwrap_or(0.0);
        state.active_orders.insert(
            response.order_id.clone(),
            ActiveOrder {
                order_id: response.order_id.clone(),
                instrument: state.instrument.clone(),
                side: state.side,
                qty,
                price,
                post_only,
                placed_at,
                queue_ahead: level_qty,
                last_level_qty: level_qty,
            },
        );
    }
    performance.record_decision(
        response.order_id,
        &state.instrument,
        decision_price,
        qty,
        state.side,
    );

    Ok(())
}

async fn cancel_stale_orders(
    execution: &ExecutionEngine,
    config: &AlgoConfig,
    state: &mut InstrumentState,
    now: u64,
) -> Result<()> {
    let mut to_cancel = Vec::new();
    for order in state.active_orders.values() {
        let age = now.saturating_sub(order.placed_at);
        if age > config.order_stale_ms {
            to_cancel.push(order.order_id.clone());
        }
        if order.queue_ahead > order.qty * 5.0 {
            to_cancel.push(order.order_id.clone());
        }
    }
    for order_id in to_cancel {
        execution
            .cancel_order(&state.instrument, &order_id)
            .await?;
        state.active_orders.remove(&order_id);
    }
    Ok(())
}

async fn poll_fills(
    execution: &ExecutionEngine,
    performance: &mut PerformanceTracker,
    seen_fills: &mut HashSet<String>,
    state: &mut InstrumentState,
) -> Result<()> {
    let fills = execution.fetch_executions(&state.instrument, 50).await?;
    for fill in fills {
        let key = fill
            .exec_id
            .clone()
            .unwrap_or_else(|| format!("{}:{}:{}", fill.order_id, fill.price, fill.timestamp));
        if seen_fills.contains(&key) {
            continue;
        }
        seen_fills.insert(key);
        performance.record_fill(&state.instrument, &fill);
        state.remaining_qty = (state.remaining_qty - fill.qty).max(0.0);
    }
    Ok(())
}

async fn force_finish(
    execution: &ExecutionEngine,
    performance: &mut PerformanceTracker,
    states: &mut HashMap<String, InstrumentState>,
) -> Result<()> {
    for state in states.values_mut() {
        if state.remaining_qty <= 0.0 {
            continue;
        }
        if state.active_orders.is_empty() {
            let Some(book) = &state.order_book else {
                continue;
            };
            let best_price = match state.side {
                Side::Buy => book.asks.first().map(|l| l.price),
                Side::Sell => book.bids.first().map(|l| l.price),
            };
            if let Some(price) = best_price {
                place_child_order(
                    execution,
                    performance,
                    state,
                    OrderType::Market,
                    price,
                    state.remaining_qty,
                    false,
                    price,
                )
                .await?;
            }
        }
    }
    Ok(())
}

fn best_level_qty(signals: &MicrostructureSignals, state: &InstrumentState) -> f64 {
    let Some(book) = &state.order_book else {
        return 0.0;
    };
    let level = match state.side {
        Side::Buy => book.bids.first(),
        Side::Sell => book.asks.first(),
    };
    let qty = level.map(|l| l.qty).unwrap_or(0.0);
    if signals.volatility > 0.0 {
        qty * (1.0 - signals.volatility.min(0.5))
    } else {
        qty
    }
}

fn compute_imbalance(book: &OrderBookSnapshot) -> f64 {
    let bid_vol: f64 = book.bids.iter().map(|l| l.qty).sum();
    let ask_vol: f64 = book.asks.iter().map(|l| l.qty).sum();
    let total = bid_vol + ask_vol;
    if total > 0.0 {
        (bid_vol - ask_vol) / total
    } else {
        0.0
    }
}

fn compute_trade_signals(trades: &VecDeque<Trade>) -> (f64, f64, f64) {
    if trades.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut buy_vol = 0.0;
    let mut sell_vol = 0.0;
    let first = trades.front().unwrap();
    let last = trades.back().unwrap();
    for trade in trades {
        match trade.side {
            Side::Buy => buy_vol += trade.qty,
            Side::Sell => sell_vol += trade.qty,
        }
    }
    let total = buy_vol + sell_vol;
    let flow = if total > 0.0 { (buy_vol - sell_vol) / total } else { 0.0 };
    let momentum = if first.price > 0.0 {
        (last.price - first.price) / first.price
    } else {
        0.0
    };

    let avg_size = total / trades.len() as f64;
    let last_size = last.qty;
    let mean_reversion = if last_size > avg_size * 2.0 {
        -momentum * 0.5
    } else {
        0.0
    };

    (flow, momentum, mean_reversion)
}

fn compute_alpha(signals: &MicrostructureSignals) -> f64 {
    0.35 * signals.imbalance
        + 0.25 * signals.trade_flow_imbalance
        + 0.25 * signals.momentum
        + 0.15 * signals.mean_reversion
}

fn compute_iceberg_score(state: &InstrumentState) -> f64 {
    let mut score = 0.0;
    for info in state.iceberg_map.values() {
        if info.replenishments > 3 {
            score += 0.1 * info.replenishments as f64;
        }
    }
    score.min(1.0)
}

fn update_iceberg_entry(state: &mut InstrumentState, price: f64, qty: f64) {
    let key = price_key(price);
    let entry = state.iceberg_map.entry(key).or_insert(IcebergInfo {
        last_qty: qty,
        replenishments: 0,
    });
    if qty >= entry.last_qty * 0.95 && entry.last_qty > 0.0 {
        entry.replenishments = entry.replenishments.saturating_add(1);
    }
    entry.last_qty = qty;
}

fn find_level_qty(snapshot: &OrderBookSnapshot, price: f64, side: Side) -> Option<f64> {
    let levels = match side {
        Side::Buy => &snapshot.bids,
        Side::Sell => &snapshot.asks,
    };
    for level in levels {
        if (level.price - price).abs() < 1e-8 {
            return Some(level.qty);
        }
    }
    None
}

fn instrument_key(instrument: &Instrument) -> String {
    format!("{}:{}", instrument.category.as_str(), instrument.symbol)
}
