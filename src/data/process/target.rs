use crate::data::data_interfaces::{CandlesTarget, FlattenedData};

pub fn process_target(
    candles1: &CandlesTarget,
    candles2: &CandlesTarget,
) -> (Option<f64>, Option<bool>) {
    // target = (futurePrice - currentPrice) / (dayHigh - dayLow)
    let target =
        (candles2.close - candles1.close) / (candles2.day_price.high - candles2.day_price.low);
    let is_significant = target >= 0.60 || target <= 0.40;
    (Some(target), Some(is_significant))
}

pub fn restore_price(candles: &CandlesTarget, target: f64) -> f64 {
    candles.close + target * (candles.day_price.high - candles.day_price.low)
}

pub fn add_target(flattened: FlattenedData, target: f64, is_significant: bool) -> FlattenedData {
    let mut new_features = flattened.features;
    new_features.push(target);
    let significant = if is_significant { 1.0 } else { 0.0 };
    new_features.push(significant);

    FlattenedData::new(flattened.token, new_features, true)
}
