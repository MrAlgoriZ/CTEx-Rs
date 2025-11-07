use crate::data::data_interfaces::CandlesTarget;

pub fn process_target(candles1: &CandlesTarget, candles2: &CandlesTarget) -> Option<f64> {
    // target = (futurePrice - currentPrice) / (dayHigh - dayLow)
    let target =
        (candles2.close - candles1.close) / (candles2.day_price.high - candles2.day_price.low);
    Some(target)
}

pub fn restore_price(candles: &CandlesTarget, target: f64) -> f64 {
    candles.close + target * (candles.day_price.high - candles.day_price.low)
}
