use crate::data::data_interfaces::Candle;
use crate::data::process::features::auxiliary::*;

pub fn return_k(candles: &[Candle], k: usize) -> f64 {
    let n = candles.len();
    let close_t = candles[n - 1].close;
    let close_k = candles[n - 1 - k].close;
    safed((close_t - close_k) / close_k)
}

pub fn log_return_k(candles: &[Candle], k: usize) -> f64 {
    let n = candles.len();
    let close_t = candles[n - 1].close;
    let close_k = candles[n - 1 - k].close;
    safed((close_t / close_k).ln())
}

pub fn vol_rolling_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut returns = Vec::with_capacity(n);

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        returns.push(r);
    }

    let mean: f64 = returns.iter().sum::<f64>() / n as f64;
    let var: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n as f64;

    safed(var.sqrt())
}

pub fn volume_change_k(candles: &[Candle], k: usize) -> f64 {
    let len = candles.len();
    let volume_t = candles[len - 1].volume;
    let volume_k = candles[len - 1 - k].volume;
    safed((volume_t - volume_k) / volume_k)
}

pub fn sma(candles: &[Candle], n: usize) -> f64 {
    let slice = &candles[candles.len() - n..];
    safed(slice.iter().map(|c| c.close).sum::<f64>() / n as f64)
}

pub fn ema(candles: &[Candle], n: usize) -> f64 {
    let alpha = 2.0 / (n as f64 + 1.0);

    let mut ema = candles[..n].iter().map(|c| c.close).sum::<f64>() / n as f64;

    for candle in &candles[n..] {
        ema = candle.close * alpha + ema * (1.0 - alpha);
    }

    safed(ema)
}

pub fn rsi(candles: &[Candle], n: usize) -> f64 {
    let mut gain = 0.0;
    let mut loss = 0.0;

    let slice = &candles[candles.len() - n - 1..];

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
    safed(rs / (1.0 + rs))
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

    safed((macd_line - signal) / ema_slow)
}

pub fn bb_percent_n(candles: &[Candle], n: usize, num_std: f64) -> f64 {
    let len = candles.len();

    let slice = &candles[len - n..];
    let closes: Vec<f64> = slice.iter().map(|c| c.close).collect();

    let mean = closes.iter().sum::<f64>() / n as f64;

    let var = closes.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / n as f64;
    let std = var.sqrt();

    let upper_band = mean + num_std * std;
    let lower_band = mean - num_std * std;

    if (upper_band - lower_band).abs() < 1e-12 {
        return 0.5;
    }

    safed((candles[n - 1].close - lower_band) / (upper_band - lower_band))
}

pub fn zscore_price_n(candles: &[Candle], n: usize) -> f64 {
    if candles.len() < n {
        return 0.0;
    }

    let slice = &candles[candles.len() - n..];
    let close = slice.last().unwrap().close;

    let sma_val = sma(slice, n);
    let ema_long = ema(slice, n);

    safed((close - sma_val) / ema_long)
}

pub fn mean_reversion(candles: &[Candle]) -> f64 {
    let close = candles.last().unwrap().close;
    let ema_slow = ema(candles, 20);

    safed((close - ema_slow) / ema_slow)
}

pub fn breakout_high_n(candles: &[Candle], n: usize) -> f64 {
    let slice = &candles[candles.len() - n..];

    let close = match slice.last() {
        Some(candle) => candle.close,
        None => return 0.0,
    };

    let rolling_high = slice
        .iter()
        .filter(|c| !c.high.is_nan() && !c.high.is_infinite())
        .map(|c| c.high)
        .fold(f64::NEG_INFINITY, f64::max);

    let result = (close - rolling_high) / rolling_high;

    safed(result)
}

pub fn breakout_low_n(candles: &[Candle], n: usize) -> f64 {
    let slice = &candles[candles.len() - n..];

    let close = match slice.last() {
        Some(candle) => candle.close,
        None => return 0.0,
    };

    let rolling_low = slice
        .iter()
        .filter(|c| !c.low.is_nan() && !c.low.is_infinite())
        .map(|c| c.low)
        .fold(f64::INFINITY, f64::min);

    let result = (close - rolling_low) / rolling_low;

    safed(result)
}

pub fn volume_volatility_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut volumes = Vec::with_capacity(n);

    for i in len - n..len {
        let v = (candles[i].volume - candles[i - 1].volume) / candles[i - 1].volume;
        volumes.push(v);
    }

    let mean: f64 = volumes.iter().sum::<f64>() / n as f64;
    let var: f64 = volumes.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;

    safed(var.sqrt())
}

pub fn trend_persistence_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();

    let mut returns = Vec::with_capacity(n);
    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        returns.push(r);
    }

    let mut time = Vec::with_capacity(n);
    for i in 0..n {
        time.push(i as f64);
    }

    corr(&returns, &time)
}

pub fn compression_ratio_n(candles: &[Candle], n: usize, vol_rolling_n: f64) -> f64 {
    let len = candles.len();
    let window = &candles[len - n..len];

    let mean: f64 = window.iter().map(|c| c.close).sum::<f64>() / n as f64;
    let var: f64 = window.iter().map(|c| (c.close - mean).powi(2)).sum::<f64>() / n as f64;
    let std = var.sqrt();

    let upper_band = mean + 2.0 * std;
    let lower_band = mean - 2.0 * std;
    let middle_band = mean;

    safed(((upper_band - lower_band) / middle_band) / vol_rolling_n)
}

pub fn return_autocorr_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();

    let mut r1 = Vec::with_capacity(n);
    let mut r2 = Vec::with_capacity(n);

    for i in len - n..len {
        let r_t = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        let r_prev = (candles[i - 1].close - candles[i - 2].close) / candles[i - 2].close;

        r1.push(r_t);
        r2.push(r_prev);
    }

    corr(&r1, &r2)
}

pub fn vol_autocorr_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();

    let mut v1 = Vec::with_capacity(n);
    let mut v2 = Vec::with_capacity(n);

    for i in len - n..len {
        let v_t = (candles[i].volume - candles[i - 1].volume) / candles[i - 1].volume;
        let v_prev = (candles[i - 1].volume - candles[i - 2].volume) / candles[i - 2].volume;

        v1.push(v_t);
        v2.push(v_prev);
    }

    corr(&v1, &v2)
}

pub fn trend_memory_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut sum = 0.0;

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        sum += r.signum();
    }

    safed(sum / n as f64)
}

pub fn downside_vol_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut returns = Vec::with_capacity(n);

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        if r < 0.0 {
            returns.push(r);
        }
    }

    let k = returns.len();
    if k == 0 {
        return 0.0;
    }

    let mean: f64 = returns.iter().sum::<f64>() / k as f64;
    let var: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / k as f64;

    safed(var.sqrt())
}

pub fn upside_vol_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut returns = Vec::with_capacity(n);

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        if r > 0.0 {
            returns.push(r);
        }
    }

    let k = returns.len();
    if k == 0 {
        return 0.0;
    }

    let mean: f64 = returns.iter().sum::<f64>() / k as f64;
    let var: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / k as f64;

    safed(var.sqrt())
}

pub fn returns_skew_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut returns = Vec::with_capacity(n);

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        returns.push(r);
    }

    safed(skew(&returns))
}

pub fn returns_kurtosis_n(candles: &[Candle], n: usize) -> f64 {
    let len = candles.len();
    let mut returns = Vec::with_capacity(n);

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        returns.push(r);
    }

    safed(kurtosis(&returns))
}

pub fn tail_risk_proxy_n(candles: &[Candle], n: usize, vol_rolling_n: f64) -> f64 {
    let len = candles.len();
    let mut count = 0;

    for i in len - n..len {
        let r = (candles[i].close - candles[i - 1].close) / candles[i - 1].close;
        if r > 0.05 {
            count += 1;
        };
    }

    safed((count as f64 / len as f64) / vol_rolling_n)
}
