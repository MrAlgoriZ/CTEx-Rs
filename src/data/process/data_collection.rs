use crate::data::data_interfaces::*;
use crate::data::process::features::*;
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::time::TimeRequest;
use std::sync::Arc;

pub const OHLCV_LEN: usize = 50;
pub const OHLCV_FETCH_LEN: usize = 51;
const FEATURES_LEN: usize = 24;

pub struct AddFeatures {
    ohlcv: [Candle; OHLCV_LEN],
    ticker: Ticker,
}

impl AddFeatures {
    pub fn new(ticker: Ticker, ohlcv: [Candle; OHLCV_LEN]) -> Self {
        AddFeatures { ohlcv, ticker }
    }

    pub fn apply_features(&self, fake_ticker: bool) -> Vec<f64> {
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
            if fake_ticker {
                let last = self.ohlcv.last().unwrap();
                (last.high - last.low) / last.close
            } else {
                spread(self.ticker.ask, self.ticker.bid)
            }
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

        let features = vec![
            return_1,
            return_2,
            return_3,
            return_5,
            return_10,
            log_return_1,
            vol_rolling_3,
            vol_rolling_5,
            vol_rolling_10,
            volume_change_1,
            volume_change_3,
            spread_val,
            ema_fast_percent,
            ema_slow_percent,
            rsi_7,
            rsi_14,
            macd_diff,
            bb_percent,
            zscore,
            mean_reversion,
            breakout_high,
            breakout_low,
            return_1_over_vol,
            return_5_over_vol,
        ];

        features
    }
}

#[derive(Debug, Clone)]
pub struct CollectedData {
    pub symbol: String,
    pub timeframe: f64,
    pub time: CircleTime,
    pub features: [f64; FEATURES_LEN],
}

impl CollectedData {
    pub fn new(
        symbol: &str,
        ohlcv: Vec<Candle>,
        ticker: Ticker,
        timeframe: &str,
        fake_ticker: bool,
    ) -> Self {
        let ohlcv_wrapped = ohlcv[..OHLCV_LEN].try_into().unwrap();
        let timeframe = Timeframe::from_str(timeframe).unwrap().seconds().unwrap();

        CollectedData {
            symbol: symbol.to_string(),
            timeframe,
            time: TimeRequest::new().get_time(),
            features: AddFeatures::new(ticker, ohlcv_wrapped)
                .apply_features(fake_ticker)
                .try_into()
                .unwrap(),
        }
    }

    pub fn with_time(mut self, time: CircleTime) -> Self {
        self.time = time;
        self
    }
}

pub async fn collect_all(
    symbol: &str,
    timeframe: &str,
    client: &CCXTClient,
) -> Result<CollectedData, anyhow::Error> {
    let (ohlcv_res, ticker_res) = tokio::join!(
        client.fetch_ohlcv(symbol, timeframe, OHLCV_FETCH_LEN),
        client.fetch_ticker(symbol),
    );

    let ohlcv = ohlcv_res?;
    let ticker = ticker_res?;

    Ok(CollectedData::new(symbol, ohlcv, ticker, timeframe, false))
}

pub fn collect_from_slice(
    symbol: &str,
    timeframe: &str,
    candles: &[CandleWithTimestamp],
) -> Option<CollectedData> {
    let ticker = Ticker { bid: 0.0, ask: 0.0 };

    let ohlcv: Vec<Candle> = candles.iter().map(|candle| candle.to_candle()).collect();
    let time = TimeRequest::from_timestamp(candles[(candles.len() - 1) - 1].timestamp).get_time();

    Some(CollectedData::new(symbol, ohlcv, ticker, timeframe, true).with_time(time))
}

pub fn flat_all(collected_data: Arc<CollectedData>, target: Option<f64>) -> FlattenedData {
    let mut features = Vec::with_capacity(1 + 4 + FEATURES_LEN + 1);

    features.push(collected_data.timeframe);
    features.push(collected_data.time.hour_sin);
    features.push(collected_data.time.hour_cos);
    features.push(collected_data.time.min_sin);
    features.push(collected_data.time.min_cos);

    features.extend_from_slice(&collected_data.features);

    if let Some(t) = target {
        features.push(t);
        FlattenedData::new(collected_data.symbol.clone(), features, true)
    } else {
        FlattenedData::new(collected_data.symbol.clone(), features, false)
    }
}
