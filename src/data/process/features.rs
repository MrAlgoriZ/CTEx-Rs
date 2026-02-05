use crate::data::data_interfaces::Candle;

pub fn return_k(candles: &[Candle], k: usize) -> f64 {
    let n = candles.len();
    let close_t = candles[n - 1].close;
    let close_k = candles[n - 1 - k].close;
    (close_t - close_k) / close_k
}

pub fn log_return_k(candles: &[Candle], k: usize) -> f64 {
    let n = candles.len();
    let close_t = candles[n - 1].close;
    let close_k = candles[n - 1 - k].close;
    (close_t / close_k).ln()
}

pub fn vol_rolling_k(candles: &[Candle], k: usize) -> f64 {
    let n = candles.len();
    let mut returns = Vec::with_capacity(k);

    for i in n - k..n {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        returns.push(r);
    }

    let mean: f64 = returns.iter().sum::<f64>() / k as f64;
    let var: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / k as f64;

    var.sqrt()
}

pub fn volume_change_k(candles: &[Candle], k: usize) -> f64 {
    let n = candles.len();
    let volume_t = candles[n - 1].volume;
    let volume_k = candles[n - 1 - k].volume;
    (volume_t - volume_k) / volume_k
}

pub fn mid(ask: f64, bid: f64) -> f64 {
    (ask + bid) / 2.0
}

pub fn spread(ask: f64, bid: f64) -> f64 {
    (ask - bid) / mid(ask, bid)
}

pub fn sma(candles: &[Candle], period: usize) -> f64 {
    let slice = &candles[candles.len() - period..];
    slice.iter().map(|c| c.close).sum::<f64>() / period as f64
}

pub fn ema(candles: &[Candle], period: usize) -> f64 {
    let alpha = 2.0 / (period as f64 + 1.0);

    let mut ema = candles[..period].iter().map(|c| c.close).sum::<f64>() / period as f64;

    for candle in &candles[period..] {
        ema = candle.close * alpha + ema * (1.0 - alpha);
    }

    ema
}

pub fn rsi(candles: &[Candle], period: usize) -> f64 {
    let mut gain = 0.0;
    let mut loss = 0.0;

    let slice = &candles[candles.len() - period - 1..];

    for i in 1..slice.len() {
        let diff = slice[i].close - slice[i - 1].close;
        if diff > 0.0 {
            gain += diff;
        } else {
            loss -= diff;
        }
    }

    if loss == 0.0 {
        return 1.0;
    }

    let rs = gain / loss;
    rs / (1.0 + rs)
}

pub fn macd_diff_percent(candles: &[Candle], ema_fast: f64, ema_slow: f64) -> f64 {
    let macd_line = ema_fast - ema_slow;

    if ema_slow.abs() < 1e-12 {
        return 0.0;
    }

    let mut macd_series = Vec::new();
    for i in 20..candles.len() {
        let fast = ema(&candles[..=i], 5);
        let slow = ema(&candles[..=i], 20);
        macd_series.push(fast - slow);
    }

    let signal = {
        let alpha = 2.0 / (9.0 + 1.0);
        let mut ema = macd_series[..9].iter().sum::<f64>() / 9.0;
        for &v in &macd_series[9..] {
            ema = v * alpha + ema * (1.0 - alpha);
        }
        ema
    };

    (macd_line - signal) / ema_slow
}

pub fn bb_percent(candles: &[Candle], period: usize, num_std: f64) -> f64 {
    let n = candles.len();

    let slice = &candles[n - period..];
    let closes: Vec<f64> = slice.iter().map(|c| c.close).collect();

    let mean = closes.iter().sum::<f64>() / period as f64;

    let var = closes.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / period as f64;
    let std = var.sqrt();

    let upper_band = mean + num_std * std;
    let lower_band = mean - num_std * std;

    if (upper_band - lower_band).abs() < 1e-12 {
        return 0.5;
    }

    (candles[n - 1].close - lower_band) / (upper_band - lower_band)
}

pub fn zscore_price(candles: &[Candle], period: usize) -> f64 {
    if candles.len() < period {
        return 0.0;
    }

    let slice = &candles[candles.len() - period..];
    let close = slice.last().unwrap().close;

    let sma_val = sma(slice, period);
    let ema_long = ema(slice, period);

    if ema_long.abs() < 1e-12 {
        return 0.0;
    }

    (close - sma_val) / ema_long
}

pub fn mean_reversion(candles: &[Candle]) -> f64 {
    let close = candles.last().unwrap().close;
    let ema_slow = ema(candles, 20);

    if ema_slow == 0.0 {
        return 0.0;
    }

    (close - ema_slow) / ema_slow
}

pub fn breakout_high(candles: &[Candle], period: usize) -> f64 {
    let slice = &candles[candles.len() - period..];

    let close = match slice.last() {
        Some(candle) => {
            if candle.close.is_nan() || candle.close.is_infinite() {
                return 0.0;
            }
            candle.close
        }
        None => return 0.0,
    };

    let rolling_high = slice
        .iter()
        .filter(|c| !c.high.is_nan() && !c.high.is_infinite())
        .map(|c| c.high)
        .fold(f64::NEG_INFINITY, f64::max);

    if rolling_high == f64::NEG_INFINITY {
        return 0.0;
    }

    if rolling_high.abs() < 1e-12 {
        return 0.0;
    }

    let result = (close - rolling_high) / rolling_high;

    if result.is_nan() || result.is_infinite() {
        return 0.0;
    }

    result
}

pub fn breakout_low(candles: &[Candle], period: usize) -> f64 {
    let slice = &candles[candles.len() - period..];

    let close = match slice.last() {
        Some(candle) => {
            if candle.close.is_nan() || candle.close.is_infinite() {
                return 0.0;
            }
            candle.close
        }
        None => return 0.0,
    };

    let rolling_low = slice
        .iter()
        .filter(|c| !c.low.is_nan() && !c.low.is_infinite())
        .map(|c| c.low)
        .fold(f64::INFINITY, f64::min);

    if rolling_low == f64::INFINITY {
        return 0.0;
    }

    if rolling_low.abs() < 1e-12 {
        return 0.0;
    }

    let result = (close - rolling_low) / rolling_low;

    if result.is_nan() || result.is_infinite() {
        return 0.0;
    }

    result
}
