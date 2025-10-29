const EPSILON: f64 = 1e-8;

// pub fn close_return(close: f64, close_prev: f64) -> f64 {
//     (close - close_prev) / close_prev
// }

pub fn spread_rel(ask: f64, bid: f64, mid: f64) -> f64 {
    ask - bid / mid
}

pub fn mid_price(ask: f64, bid: f64) -> f64 {
    (ask + bid) / 2.0
}

pub fn pressure_side(close: f64, mid: f64) -> f64 {
    close - mid
}

pub fn bid_ask_ratio(ask: f64, bid: f64) -> f64 {
    bid / ask
}

pub fn mid_distance_day_highlow(mid: f64, day_high: f64, day_low: f64) -> f64 {
    (mid - day_low) / (day_high - day_low)
}

pub fn body(open: f64, close: f64) -> f64 {
    close - open
}

pub fn body_strength(open: f64, high: f64, low: f64, close: f64) -> f64 {
    let body: f64 = body(open, close);
    let range: f64 = high - low;
    return body.signum() * (body.abs() / (range + EPSILON));
}
