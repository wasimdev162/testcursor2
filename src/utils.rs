use chrono::Utc;

pub fn now_ms() -> u64 {
    Utc::now()
        .timestamp_millis()
        .try_into()
        .unwrap_or_default()
}

pub fn price_key(price: f64) -> u64 {
    price.to_bits()
}
