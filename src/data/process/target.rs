pub fn process_target(close1: f64, close2: f64) -> Option<f64> {
    // target = (futurePrice - currentPrice) / currentPrice
    let target = (close2 - close1) / close1;
    Some(target)
}

pub fn restore_price(close: f64, target: f64) -> f64 {
    close * (1.0 + target)
}
