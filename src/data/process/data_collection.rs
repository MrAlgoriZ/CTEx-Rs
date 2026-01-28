use crate::CONFIG_PATH;
use crate::data::data_interfaces::*;
use crate::data::process::features::*;
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::time::TimeRequest;
use crate::engine::utils::{config::load_config::load_config, processor::*};

use rayon::prelude::*;
use std::sync::Arc;

const OHLCV_LEN: usize = 10;
const OHLCV_FETCH_LEN: usize = 11;
const FEATURES_LEN: usize = 70;

pub struct AddFeatures {
    ohlcv: [Candle; OHLCV_LEN],
    ohlcv1h: [Candle; OHLCV_LEN],
    ohlcv1d: [Candle; OHLCV_LEN],
    ticker: Ticker,
}

impl AddFeatures {
    pub fn new(
        ticker: Ticker,
        ohlcv: [Candle; OHLCV_LEN],
        ohlcv1h: [Candle; OHLCV_LEN],
        ohlcv1d: [Candle; OHLCV_LEN],
    ) -> Self {
        AddFeatures {
            ohlcv,
            ohlcv1h,
            ohlcv1d,
            ticker,
        }
    }

    pub fn apply_features(&self) -> Vec<f64> {
        let mut features: Vec<f64> = Vec::new();

        let mid: f64 = mid_price(self.ticker.ask, self.ticker.bid);

        features.push(spread_rel(self.ticker.ask, self.ticker.bid, mid));
        features.push(mid);
        features.push(pressure_side(self.ohlcv[9].close, mid));
        features.push(pressure_side(self.ohlcv1h[9].close, mid));
        features.push(pressure_side(self.ohlcv1d[9].close, mid));
        features.push(bid_ask_ratio(self.ticker.ask, self.ticker.bid));
        features.push(mid_distance_day_highlow(
            mid,
            self.ticker.high,
            self.ticker.low,
        ));

        let ohlcv_var_list: [[Candle; OHLCV_LEN]; 3] = [self.ohlcv, self.ohlcv1h, self.ohlcv1d];

        for ohlcv in ohlcv_var_list {
            let candle_features: Vec<f64> = ohlcv
                .par_iter()
                .flat_map(|candle| {
                    vec![
                        body(candle.open, candle.close),
                        body_strength(candle.open, candle.high, candle.low, candle.close),
                    ]
                })
                .collect();
            features.extend(candle_features);
            features.push(get_volatility(&ohlcv));
        }

        features
    }
}

#[derive(Debug, Clone)]
pub struct CollectedData {
    pub token: String,
    pub time: CircleTime,
    pub ohlcv: [Candle; OHLCV_LEN],
    pub ohlcv1h: [Candle; OHLCV_LEN],
    pub ohlcv1d: [Candle; OHLCV_LEN],
    pub ticker: Ticker,
    pub features: [f64; FEATURES_LEN],
}

impl CollectedData {
    pub fn new(
        token: &str,
        ohlcv: [Candle; OHLCV_FETCH_LEN],
        ohlcv1h: [Candle; OHLCV_FETCH_LEN],
        ohlcv1d: [Candle; OHLCV_FETCH_LEN],
        ticker: Ticker,
    ) -> Self {
        let ohlcv10 = ohlcv[..OHLCV_LEN].try_into().unwrap();
        let ohlcv1h10 = ohlcv1h[..OHLCV_LEN].try_into().unwrap();
        let ohlcv1d10 = ohlcv1d[..OHLCV_LEN].try_into().unwrap();

        CollectedData {
            token: token.to_string(),
            time: TimeRequest::new().get_time(),
            ohlcv: ohlcv10,
            ohlcv1h: ohlcv1h10,
            ohlcv1d: ohlcv1d10,
            ticker: ticker.clone(),
            features: AddFeatures::new(ticker, ohlcv10, ohlcv1h10, ohlcv1d10)
                .apply_features()
                .try_into()
                .unwrap(),
        }
    }
}

struct ProcessAll {
    ohlcv: [Candle; OHLCV_FETCH_LEN],
    ohlcv1h: [Candle; OHLCV_FETCH_LEN],
    ohlcv1d: [Candle; OHLCV_FETCH_LEN],
    ticker: Ticker,
}

impl ProcessAll {
    pub fn new(
        ohlcv: [Candle; OHLCV_FETCH_LEN],
        ohlcv1h: [Candle; OHLCV_FETCH_LEN],
        ohlcv1d: [Candle; OHLCV_FETCH_LEN],
        ticker: Ticker,
    ) -> Self {
        ProcessAll {
            ohlcv,
            ohlcv1h,
            ohlcv1d,
            ticker,
        }
    }

    pub fn ohlcv(&self) -> [Candle; OHLCV_FETCH_LEN] {
        process_ohlcv(&self.ohlcv, self.ohlcv[0].open)
            .try_into()
            .unwrap()
    }

    pub fn ohlcv1h(&self) -> [Candle; OHLCV_FETCH_LEN] {
        process_ohlcv(&self.ohlcv1h, self.ohlcv[0].open)
            .try_into()
            .unwrap()
    }

    pub fn ohlcv1d(&self) -> [Candle; OHLCV_FETCH_LEN] {
        process_ohlcv(&self.ohlcv1d, self.ohlcv[0].open)
            .try_into()
            .unwrap()
    }

    pub fn ticker(&self) -> Ticker {
        process_ticker(&self.ticker, self.ohlcv[0].open)
    }
}

pub async fn collect_all(token: &str) -> Result<CollectedData, anyhow::Error> {
    let client = CCXTClient::new(&load_config(CONFIG_PATH).main_exchange);

    let (ohlcv_res, ohlcv1h_res, ohlcv1d_res, ticker_res) = tokio::join!(
        client.fetch_ohlcv(token, "15m", OHLCV_FETCH_LEN),
        client.fetch_ohlcv(token, "1h", OHLCV_FETCH_LEN),
        client.fetch_ohlcv(token, "1d", OHLCV_FETCH_LEN),
        client.fetch_ticker(token),
    );

    let ohlcv: [Candle; OHLCV_FETCH_LEN] = ohlcv_res?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert ohlcv"))?;
    let ohlcv1h: [Candle; OHLCV_FETCH_LEN] = ohlcv1h_res?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert ohlcv1h"))?;
    let ohlcv1d: [Candle; OHLCV_FETCH_LEN] = ohlcv1d_res?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert ohlcv1d"))?;
    let ticker = ticker_res?;

    let process_value = ProcessAll::new(ohlcv, ohlcv1h, ohlcv1d, ticker);

    Ok(CollectedData::new(
        token,
        process_value.ohlcv(),
        process_value.ohlcv1h(),
        process_value.ohlcv1d(),
        process_value.ticker(),
    ))
}

pub fn flat_all(collected_data: Arc<CollectedData>, target: Option<f64>) -> FlattenedData {
    let mut features = Vec::with_capacity(4 + 50 + 50 + 50 + 6 + 70 + 1);

    features.push(collected_data.time.hour_sin);
    features.push(collected_data.time.hour_cos);
    features.push(collected_data.time.min_sin);
    features.push(collected_data.time.min_cos);

    let ohlcv_features: Vec<f64> = collected_data
        .ohlcv
        .par_iter()
        .flat_map(|candle| {
            vec![
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            ]
        })
        .collect();
    features.extend(ohlcv_features);

    let ohlcv1h_features: Vec<f64> = collected_data
        .ohlcv1h
        .par_iter()
        .flat_map(|candle| {
            vec![
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            ]
        })
        .collect();
    features.extend(ohlcv1h_features);

    let ohlcv1d_features: Vec<f64> = collected_data
        .ohlcv1d
        .par_iter()
        .flat_map(|candle| {
            vec![
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            ]
        })
        .collect();
    features.extend(ohlcv1d_features);

    features.push(collected_data.ticker.bid);
    features.push(collected_data.ticker.ask);
    features.push(collected_data.ticker.open);
    features.push(collected_data.ticker.high);
    features.push(collected_data.ticker.low);
    features.push(collected_data.ticker.average);

    features.extend_from_slice(&collected_data.features);

    if let Some(t) = target {
        features.push(t);
        FlattenedData::new(collected_data.token.clone(), features, true)
    } else {
        FlattenedData::new(collected_data.token.clone(), features, false)
    }
}
