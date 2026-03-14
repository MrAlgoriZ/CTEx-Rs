use std::collections::BTreeMap;

use crate::data::data_interfaces::*;
use crate::data::process::features::auxiliary::{safed, vwap};
use crate::data::process::features::basic::*;

pub const OHLCV_LEN: usize = 50;
pub const OHLCV_FETCH_LEN: usize = 51;

pub struct AddFeatures {
    ohlcv: [Candle; OHLCV_LEN],
}

impl AddFeatures {
    pub fn new(ohlcv: [Candle; OHLCV_LEN]) -> Self {
        AddFeatures { ohlcv }
    }

    pub fn apply_features(&self) -> BTreeMap<String, f64> {
        let return_1 = return_k(&self.ohlcv, 1);
        let return_2 = return_k(&self.ohlcv, 2);
        let return_3 = return_k(&self.ohlcv, 3);
        let return_5 = return_k(&self.ohlcv, 5);
        let return_10 = return_k(&self.ohlcv, 10);
        let log_return_1 = log_return_k(&self.ohlcv, 1);

        let vol_rolling_3 = vol_rolling_k(&self.ohlcv, 3);
        let vol_rolling_5 = vol_rolling_k(&self.ohlcv, 5);
        let vol_rolling_10 = vol_rolling_k(&self.ohlcv, 10);

        let volume_change_1 = volume_change_k(&self.ohlcv, 1);
        let volume_change_3 = volume_change_k(&self.ohlcv, 3);

        let ema_fast = ema(&self.ohlcv, 5);
        let ema_slow = ema(&self.ohlcv, 20);
        let ema_long = ema(&self.ohlcv, 50);

        let rsi_7 = rsi(&self.ohlcv, 7);
        let rsi_14 = rsi(&self.ohlcv, 14);

        let macd_diff = macd_diff_percent(&self.ohlcv, ema_fast, ema_slow);
        let bb_percent = bb_percent(&self.ohlcv, 20, 2.0);
        let zscore = zscore_price(&self.ohlcv, 50);
        let mean_reversion = mean_reversion(&self.ohlcv);

        let breakout_high = breakout_high(&self.ohlcv, 20);
        let breakout_low = breakout_low(&self.ohlcv, 20);

        let return_1_over_vol = safed(return_1 / vol_rolling_3);
        let return_5_over_vol = safed(return_5 / vol_rolling_10);

        let ema_fast_percent = safed((ema_fast - ema_slow) / ema_slow);
        let ema_slow_percent = safed((ema_slow - ema_long) / ema_long);

        // TODO: Доделать все значения
        let trend_strength = ema_fast_percent.abs();
        let trend_persistence = f64::default();
        let volatility_regime = safed(vol_rolling_3 / vol_rolling_10);
        let compression_ratio = f64::default();
        let range_ratio = {
            let candle = &self.ohlcv[&self.ohlcv.len() - 6];
            safed((candle.high - candle.low) / candle.close / vol_rolling_5)
        };
        let volume_acceleration = volume_change_1 - volume_change_3;
        let volume_volatility = f64::default();
        let return_autocorr_n = f64::default();
        let vol_autocorr_10 = f64::default();
        let momentum_decay = safed(return_1 / return_5);
        let trend_memory = f64::default();
        let downside_vol = f64::default();
        let upside_vol = f64::default();
        let skewness_returns = f64::default();
        let kurtosis_returns = f64::default();
        let tail_risk_proxy = f64::default();
        let distance_to_vwap = {
            let candle = self.ohlcv.last().unwrap();
            let vwap = vwap(candle);
            safed((candle.close - vwap) / vwap)
        };

        let features = BTreeMap::from([
            ("return_1".to_string(), return_1),
            ("return_2".to_string(), return_2),
            ("return_3".to_string(), return_3),
            ("return_5".to_string(), return_5),
            ("return_10".to_string(), return_10),
            ("log_return_1".to_string(), log_return_1),
            ("vol_rolling_3".to_string(), vol_rolling_3),
            ("vol_rolling_5".to_string(), vol_rolling_5),
            ("vol_rolling_10".to_string(), vol_rolling_10),
            ("volume_change_1".to_string(), volume_change_1),
            ("volume_change_3".to_string(), volume_change_3),
            ("ema_fast_percent".to_string(), ema_fast_percent),
            ("ema_slow_percent".to_string(), ema_slow_percent),
            ("rsi_7".to_string(), rsi_7),
            ("rsi_14".to_string(), rsi_14),
            ("macd_diff".to_string(), macd_diff),
            ("bb_percent".to_string(), bb_percent),
            ("zscore".to_string(), zscore),
            ("mean_reversion".to_string(), mean_reversion),
            ("breakout_high".to_string(), breakout_high),
            ("breakout_low".to_string(), breakout_low),
            ("return_1_over_vol".to_string(), return_1_over_vol),
            ("return_5_over_vol".to_string(), return_5_over_vol),
            ("trend_strength".to_string(), trend_strength),
            ("trend_persistence".to_string(), trend_persistence),
            ("volatility_regime".to_string(), volatility_regime),
            ("compression_ratio".to_string(), compression_ratio),
            ("range_ratio".to_string(), range_ratio),
            ("volume_acceleration".to_string(), volume_acceleration),
            ("volume_volatility".to_string(), volume_volatility),
            ("return_autocorr_n".to_string(), return_autocorr_n),
            ("vol_autocorr_10".to_string(), vol_autocorr_10),
            ("momentum_decay".to_string(), momentum_decay),
            ("trend_memory".to_string(), trend_memory),
            ("downside_vol".to_string(), downside_vol),
            ("upside_vol".to_string(), upside_vol),
            ("skewness_returns".to_string(), skewness_returns),
            ("kurtosis_returns".to_string(), kurtosis_returns),
            ("tail_risk_proxy".to_string(), tail_risk_proxy),
            ("distance_to_vwap".to_string(), distance_to_vwap),
        ]);

        features
    }
}
