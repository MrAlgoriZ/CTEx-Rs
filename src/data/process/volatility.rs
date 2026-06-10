use crate::data::data_interfaces::Candle;

pub fn get_volatility(candles: &[Candle]) -> f64 {
    if candles.is_empty() {
        return 0.0;
    }

    let sum: f64 = candles.iter().map(|c| (c.high - c.low) / c.open).sum();

    sum / candles.len() as f64
}
