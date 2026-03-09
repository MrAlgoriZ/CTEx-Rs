use std::collections::BTreeMap;

use crate::data::data_interfaces::*;
use crate::data::process::features::*;
use crate::data::requests::time::TimeRequest;

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

        let spread_val: f64 = {
            let last = self.ohlcv.last().unwrap();
            (last.high - last.low) / last.close
        };

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

        let return_1_over_vol = {
            if vol_rolling_3.abs() < 1e-12 {
                0.0
            } else {
                return_1 / vol_rolling_3
            }
        };
        let return_5_over_vol = {
            if vol_rolling_10.abs() < 1e-12 {
                0.0
            } else {
                return_5 / vol_rolling_10
            }
        };

        let ema_fast_percent = (ema_fast - ema_slow) / ema_slow;
        let ema_slow_percent = (ema_slow - ema_long) / ema_long;

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
            ("spread_val".to_string(), spread_val),
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
        ]);

        features
    }
}

#[derive(Debug, Clone)]
pub struct CollectedData {
    pub symbol: String,
    pub timeframe: f64,
    pub time: CircleTime,
    pub features: BTreeMap<String, f64>,
}

impl CollectedData {
    pub fn new(symbol: &str, ohlcv: Vec<Candle>, timeframe: &str) -> Self {
        let ohlcv_wrapped = ohlcv[..OHLCV_LEN].try_into().unwrap();
        let timeframe = Timeframe::from_str(timeframe).unwrap().seconds().unwrap();

        CollectedData {
            symbol: symbol.to_string(),
            timeframe,
            time: TimeRequest::new().get_time(),
            features: AddFeatures::new(ohlcv_wrapped).apply_features(),
        }
    }

    pub fn with_time(mut self, time: CircleTime) -> Self {
        self.time = time;
        self
    }

    pub fn from_slice(
        symbol: &str,
        timeframe: &str,
        candles: &[CandleWithTimestamp],
    ) -> Option<Self> {
        let ohlcv: Vec<Candle> = candles.iter().map(|candle| candle.to_candle()).collect();
        let time =
            TimeRequest::from_timestamp(candles[(candles.len() - 1) - 1].timestamp).get_time();

        Some(Self::new(symbol, ohlcv, timeframe).with_time(time))
    }
}
