use crate::data::data_interfaces::Candle;

pub fn corr(x: &[f64], y: &[f64]) -> f64 {
    let n = x.len();
    if n == 0 || y.len() != n {
        return 0.0;
    }

    let mean_x = x.iter().sum::<f64>() / n as f64;
    let mean_y = y.iter().sum::<f64>() / n as f64;

    let mut num = 0.0;
    let mut den_x = 0.0;
    let mut den_y = 0.0;

    for i in 0..n {
        let dx = x[i] - mean_x;
        let dy = y[i] - mean_y;
        num += dx * dy;
        den_x += dx * dx;
        den_y += dy * dy;
    }

    let den = (den_x * den_y).sqrt();
    if den == 0.0 { 0.0 } else { safed(num / den) }
}

pub fn skew(data: &[f64]) -> f64 {
    let n = data.len();
    if n < 2 {
        return 0.0;
    }

    let mean = data.iter().sum::<f64>() / n as f64;
    let mut m2 = 0.0;
    let mut m3 = 0.0;

    for &x in data {
        let d = x - mean;
        m2 += d.powi(2);
        m3 += d.powi(3);
    }

    m2 /= n as f64;
    m3 /= n as f64;

    if m2 == 0.0 { 0.0 } else { m3 / m2.powf(1.5) }
}

pub fn kurtosis(data: &[f64]) -> f64 {
    let n = data.len();
    if n < 2 {
        return 0.0;
    }

    let mean = data.iter().sum::<f64>() / n as f64;
    let mut m2 = 0.0;
    let mut m4 = 0.0;

    for &x in data {
        let d = x - mean;
        m2 += d.powi(2);
        m4 += d.powi(4);
    }

    m2 /= n as f64;
    m4 /= n as f64;

    if m2 == 0.0 { 0.0 } else { m4 / (m2 * m2) - 3.0 }
}

pub fn safed(value: f64) -> f64 {
    if value.abs() < 1e-12 || value.is_nan() || value.is_infinite() {
        return 0.0;
    }
    value
}

pub fn vwap(candle: &Candle) -> f64 {
    let typical_price = (candle.high + candle.low + candle.close) / 3.0;
    let vwap = (typical_price * candle.volume) / (candle.volume);
    safed(vwap)
}

pub fn process_return(close_1: f64, close_2: f64) -> f64 {
    let return_ = (close_2 - close_1) / close_1;
    return_ * 100.0
}

pub fn restore_price(close: f64, target: f64) -> f64 {
    close * (1.0 + (target / 100.0))
}
