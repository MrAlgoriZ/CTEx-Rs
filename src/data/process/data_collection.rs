use std::collections::BTreeMap;

use crate::data::data_interfaces::*;
use crate::data::process::features::auxiliary::{safed, vwap};
use crate::data::process::features::basic::*;

pub const OHLCV_LEN: usize = 48;
pub const OHLCV_FETCH_LEN: usize = 49;

pub fn collect_features(ohlcv: [Candle; OHLCV_LEN]) -> BTreeMap<String, f64> {
    let return_1 = return_k(&ohlcv, 1);
    let return_3 = return_k(&ohlcv, 3);
    let return_6 = return_k(&ohlcv, 6);
    let return_12 = return_k(&ohlcv, 12);

    let log_return_1 = log_return_k(&ohlcv, 1);
    let log_return_3 = log_return_k(&ohlcv, 3);
    let log_return_6 = log_return_k(&ohlcv, 6);
    let log_return_12 = log_return_k(&ohlcv, 12);

    let vol_rolling_3 = vol_rolling_n(&ohlcv, 3);
    let vol_rolling_6 = vol_rolling_n(&ohlcv, 6);
    let vol_rolling_12 = vol_rolling_n(&ohlcv, 12);

    let volume_change_1 = volume_change_k(&ohlcv, 1);
    let volume_change_3 = volume_change_k(&ohlcv, 3);
    let volume_change_6 = volume_change_k(&ohlcv, 6);

    let ema_fast = ema(&ohlcv, 6);
    let ema_slow = ema(&ohlcv, 24);
    let ema_long = ema(&ohlcv, 48);

    let rsi_6 = rsi(&ohlcv, 6);
    let rsi_12 = rsi(&ohlcv, 12);

    let macd_diff = macd_diff_percent(&ohlcv, ema_fast, ema_slow);
    let bb_percent = bb_percent_n(&ohlcv, 24, 2.0);
    let zscore = zscore_price_n(&ohlcv, 48);
    let mean_reversion = mean_reversion(&ohlcv);

    let breakout_high_12 = breakout_high_n(&ohlcv, 12);
    let breakout_low_12 = breakout_low_n(&ohlcv, 12);
    let breakout_high_24 = breakout_high_n(&ohlcv, 24);
    let breakout_low_24 = breakout_low_n(&ohlcv, 24);

    let return_1_over_vol = safed(return_1 / vol_rolling_3);
    let return_6_over_vol = safed(return_6 / vol_rolling_12);

    let ema_fast_percent = safed((ema_fast - ema_slow) / ema_slow);
    let ema_slow_percent = safed((ema_slow - ema_long) / ema_long);

    let trend_strength = ema_fast_percent.abs();

    let trend_persistence_3 = trend_persistence_n(&ohlcv, 3);
    let trend_persistence_6 = trend_persistence_n(&ohlcv, 6);
    let trend_persistence_12 = trend_persistence_n(&ohlcv, 12);

    let volatility_regime = safed(vol_rolling_3 / vol_rolling_12);

    let compression_ratio_6 = compression_ratio_n(&ohlcv, 6, vol_rolling_6);
    let compression_ratio_12 = compression_ratio_n(&ohlcv, 12, vol_rolling_12);

    let range_ratio_6 = {
        let candle = &ohlcv[&ohlcv.len() - 6];
        safed((candle.high - candle.low) / candle.close / vol_rolling_6)
    };
    let range_ratio_12 = {
        let candle = &ohlcv[&ohlcv.len() - 12];
        safed((candle.high - candle.low) / candle.close / vol_rolling_12)
    };

    let volume_acceleration = volume_change_1 - volume_change_3;

    let volume_volatility_3 = volume_volatility_n(&ohlcv, 3);
    let volume_volatility_6 = volume_volatility_n(&ohlcv, 6);
    let volume_volatility_12 = volume_volatility_n(&ohlcv, 12);

    let return_autocorr_3 = return_autocorr_n(&ohlcv, 3);
    let return_autocorr_6 = return_autocorr_n(&ohlcv, 6);
    let return_autocorr_12 = return_autocorr_n(&ohlcv, 12);

    let vol_autocorr_3 = vol_autocorr_n(&ohlcv, 3);
    let vol_autocorr_6 = vol_autocorr_n(&ohlcv, 6);
    let vol_autocorr_12 = vol_autocorr_n(&ohlcv, 12);

    let momentum_decay = safed(return_1 / return_6);

    let trend_memory_3 = trend_memory_n(&ohlcv, 3);
    let trend_memory_6 = trend_memory_n(&ohlcv, 6);
    let trend_memory_12 = trend_memory_n(&ohlcv, 12);

    let downside_vol_3 = downside_vol_n(&ohlcv, 3);
    let upside_vol_3 = upside_vol_n(&ohlcv, 3);
    let downside_vol_6 = downside_vol_n(&ohlcv, 6);
    let upside_vol_6 = upside_vol_n(&ohlcv, 6);

    let skewness_returns_3 = returns_skew_n(&ohlcv, 3);
    let kurtosis_returns_3 = returns_kurtosis_n(&ohlcv, 3);
    let skewness_returns_6 = returns_skew_n(&ohlcv, 6);
    let kurtosis_returns_6 = returns_kurtosis_n(&ohlcv, 6);

    let tail_risk_proxy_3 = tail_risk_proxy_n(&ohlcv, 3, vol_rolling_3);
    let tail_risk_proxy_6 = tail_risk_proxy_n(&ohlcv, 6, vol_rolling_6);
    let tail_risk_proxy_12 = tail_risk_proxy_n(&ohlcv, 12, vol_rolling_12);

    let distance_to_vwap = {
        let candle = ohlcv.last().unwrap();
        let vwap = vwap(candle);
        safed((candle.close - vwap) / vwap)
    };

    let features = BTreeMap::from([
        ("return_1".to_string(), return_1),
        ("return_3".to_string(), return_3),
        ("return_6".to_string(), return_6),
        ("return_12".to_string(), return_12),
        ("log_return_1".to_string(), log_return_1),
        ("log_return_3".to_string(), log_return_3),
        ("log_return_6".to_string(), log_return_6),
        ("log_return_12".to_string(), log_return_12),
        ("vol_rolling_3".to_string(), vol_rolling_3),
        ("vol_rolling_6".to_string(), vol_rolling_6),
        ("vol_rolling_12".to_string(), vol_rolling_12),
        ("volume_change_1".to_string(), volume_change_1),
        ("volume_change_3".to_string(), volume_change_3),
        ("volume_change_6".to_string(), volume_change_6),
        ("ema_fast".to_string(), ema_fast_percent),
        ("ema_slow".to_string(), ema_slow_percent),
        ("rsi_6".to_string(), rsi_6),
        ("rsi_12".to_string(), rsi_12),
        ("macd_diff".to_string(), macd_diff),
        ("bb_percent".to_string(), bb_percent),
        ("zscore".to_string(), zscore),
        ("mean_reversion".to_string(), mean_reversion),
        ("breakout_high_12".to_string(), breakout_high_12),
        ("breakout_low_12".to_string(), breakout_low_12),
        ("breakout_high_24".to_string(), breakout_high_24),
        ("breakout_low_24".to_string(), breakout_low_24),
        ("return_1_over_vol".to_string(), return_1_over_vol),
        ("return_6_over_vol".to_string(), return_6_over_vol),
        ("trend_strength".to_string(), trend_strength),
        ("trend_persistence_3".to_string(), trend_persistence_3),
        ("trend_persistence_6".to_string(), trend_persistence_6),
        ("trend_persistence_12".to_string(), trend_persistence_12),
        ("volatility_regime".to_string(), volatility_regime),
        ("compression_ratio_6".to_string(), compression_ratio_6),
        ("compression_ratio_12".to_string(), compression_ratio_12),
        ("range_ratio_6".to_string(), range_ratio_6),
        ("range_ratio_12".to_string(), range_ratio_12),
        ("volume_acceleration".to_string(), volume_acceleration),
        ("volume_volatility_3".to_string(), volume_volatility_3),
        ("volume_volatility_6".to_string(), volume_volatility_6),
        ("volume_volatility_12".to_string(), volume_volatility_12),
        ("return_autocorr_3".to_string(), return_autocorr_3),
        ("return_autocorr_6".to_string(), return_autocorr_6),
        ("return_autocorr_12".to_string(), return_autocorr_12),
        ("vol_autocorr_3".to_string(), vol_autocorr_3),
        ("vol_autocorr_6".to_string(), vol_autocorr_6),
        ("vol_autocorr_12".to_string(), vol_autocorr_12),
        ("momentum_decay".to_string(), momentum_decay),
        ("trend_memory_3".to_string(), trend_memory_3),
        ("trend_memory_6".to_string(), trend_memory_6),
        ("trend_memory_12".to_string(), trend_memory_12),
        ("downside_vol_3".to_string(), downside_vol_3),
        ("upside_vol_3".to_string(), upside_vol_3),
        ("downside_vol_6".to_string(), downside_vol_6),
        ("upside_vol_6".to_string(), upside_vol_6),
        ("skewness_returns_3".to_string(), skewness_returns_3),
        ("kurtosis_returns_3".to_string(), kurtosis_returns_3),
        ("skewness_returns_6".to_string(), skewness_returns_6),
        ("kurtosis_returns_6".to_string(), kurtosis_returns_6),
        ("tail_risk_proxy_3".to_string(), tail_risk_proxy_3),
        ("tail_risk_proxy_6".to_string(), tail_risk_proxy_6),
        ("tail_risk_proxy_12".to_string(), tail_risk_proxy_12),
        ("distance_to_vwap".to_string(), distance_to_vwap),
    ]);

    features
}

pub fn collect_targets(ohlcv: [Candle; OHLCV_LEN]) -> BTreeMap<String, f64> {
    let future_close = ohlcv[OHLCV_LEN - 1].close;
    let future_high = ohlcv[OHLCV_LEN - 1].high;
    let future_low = ohlcv[OHLCV_LEN - 1].low;
    let future_volume = ohlcv[OHLCV_LEN - 1].volume;

    let ema_fast = ema(&ohlcv, 6);
    let ema_slow = ema(&ohlcv, 24);

    let future_volatility = vol_rolling_n(&ohlcv, 3);
    let future_trend_strength = safed((ema_fast - ema_slow) / ema_slow).abs();
    let future_range = safed((future_high - future_low) / future_close);
    let future_return_mean = returns_mean_n(&ohlcv, 3);
    let future_return_std = returns_std_n(&ohlcv, 3);
    let future_return_skew = returns_skew_n(&ohlcv, 3);
    let future_return_kurtosis = returns_kurtosis_n(&ohlcv, 3);

    let tail_risk_proxy = tail_risk_proxy_n(&ohlcv, 6, future_volatility);
    let risk_score = future_volatility * tail_risk_proxy;
    let drawdown_probability = drawdown_probability_n(&ohlcv, 6, 0.02);
    let tail_event_probability = tail_event_probability_n(&ohlcv, 6, 2.0);
    let volatility_spike_probability = volatility_spike_probability_n(&ohlcv, 6, 1.5);
    let liquidity_drop_probability = liquidity_drop_probability_n(&ohlcv, 6, 0.5);

    let future_return = return_k(&ohlcv, 1);
    let action_type = calculate_action_type(future_return, risk_score, 0.1);
    let position_size = calculate_position_size(future_return, risk_score, 1.0);

    BTreeMap::from([
        ("future_volatility".to_string(), future_volatility),
        ("future_volume".to_string(), future_volume),
        ("future_trend_strength".to_string(), future_trend_strength),
        ("future_range".to_string(), future_range),
        ("future_return_mean".to_string(), future_return_mean),
        ("future_return_std".to_string(), future_return_std),
        ("future_return_skewness".to_string(), future_return_skew),
        ("future_return_kurtosis".to_string(), future_return_kurtosis),
        ("risk_score".to_string(), risk_score),
        ("drawdown_probability".to_string(), drawdown_probability),
        ("tail_event_probability".to_string(), tail_event_probability),
        (
            "volatility_spike_probability".to_string(),
            volatility_spike_probability,
        ),
        (
            "liquidity_drop_probability".to_string(),
            liquidity_drop_probability,
        ),
        ("future_return".to_string(), future_return),
        ("action_type".to_string(), action_type),
        ("position_size".to_string(), position_size),
    ])
}
