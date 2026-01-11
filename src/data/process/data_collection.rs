use crate::data::data_interfaces::*;
use crate::data::process::features::*;
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::data::requests::time_req::TimeRequest;
use crate::engine::utils::processor::*;

use std::sync::Arc;
use tokio::task::spawn_blocking;

const OHLCV_LEN: usize = 10;
const OHLCV_FETCH_LEN: usize = 11;
const FEATURES_LEN: usize = 70;

pub struct AddFeatures {
    ohlcv: [ICandle; OHLCV_LEN],
    ohlcv1h: [ICandle; OHLCV_LEN],
    ohlcv1d: [ICandle; OHLCV_LEN],
    ticker: ITicker,
    day_price: IDayPrice,
}

impl AddFeatures {
    pub fn new(
        ticker: ITicker,
        ohlcv: [ICandle; OHLCV_LEN],
        day_price: IDayPrice,
        ohlcv1h: [ICandle; OHLCV_LEN],
        ohlcv1d: [ICandle; OHLCV_LEN],
    ) -> Self {
        AddFeatures {
            ohlcv,
            ohlcv1h,
            ohlcv1d,
            ticker,
            day_price,
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
            self.day_price.high,
            self.day_price.low,
        ));

        let ohlcv_var_list: [[ICandle; OHLCV_LEN]; 3] = [self.ohlcv, self.ohlcv1h, self.ohlcv1d];

        for ohlcv in ohlcv_var_list {
            for candle in ohlcv.iter() {
                features.push(body(candle.open, candle.close));
                features.push(body_strength(
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                ));
            }
            features.push(get_volatility(&ohlcv));
        }

        features
    }
}

#[derive(Debug, Clone)]
pub struct CollectedData {
    pub token: String,
    pub time: ITime,
    pub ohlcv: [ICandle; OHLCV_LEN],
    pub ohlcv1h: [ICandle; OHLCV_LEN],
    pub ohlcv1d: [ICandle; OHLCV_LEN],
    pub ticker: ITicker,
    pub day_price: IDayPrice,
    pub mean_price: f64,
    pub features: [f64; FEATURES_LEN],
}

impl CollectedData {
    pub fn new(
        token: &str,
        ohlcv: [ICandle; OHLCV_FETCH_LEN],
        ohlcv1h: [ICandle; OHLCV_FETCH_LEN],
        ohlcv1d: [ICandle; OHLCV_FETCH_LEN],
        ticker: ITicker,
        day_price: IDayPrice,
        mean_price: f64,
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
            day_price: day_price.clone(),
            mean_price,
            features: AddFeatures::new(ticker, ohlcv10, day_price, ohlcv1h10, ohlcv1d10)
                .apply_features()
                .try_into()
                .unwrap(),
        }
    }
}

struct ProcessAll {
    ohlcv: [ICandle; OHLCV_FETCH_LEN],
    ohlcv1h: [ICandle; OHLCV_FETCH_LEN],
    ohlcv1d: [ICandle; OHLCV_FETCH_LEN],
    ticker: ITicker,
    day_price: IDayPrice,
    mean_price: f64,
}

impl ProcessAll {
    pub fn new(
        ohlcv: [ICandle; OHLCV_FETCH_LEN],
        ohlcv1h: [ICandle; OHLCV_FETCH_LEN],
        ohlcv1d: [ICandle; OHLCV_FETCH_LEN],
        ticker: ITicker,
        day_price: IDayPrice,
        mean_price: f64,
    ) -> Self {
        ProcessAll {
            ohlcv,
            ohlcv1h,
            ohlcv1d,
            ticker,
            day_price,
            mean_price,
        }
    }

    pub fn ohlcv(&self) -> [ICandle; OHLCV_FETCH_LEN] {
        process_ohlcv(&self.ohlcv).try_into().unwrap()
    }

    pub fn ohlcv1h(&self) -> [ICandle; OHLCV_FETCH_LEN] {
        process_ohlcv(&self.ohlcv1h).try_into().unwrap()
    }

    pub fn ohlcv1d(&self) -> [ICandle; OHLCV_FETCH_LEN] {
        process_ohlcv(&self.ohlcv1d).try_into().unwrap()
    }

    pub async fn ticker(&self) -> ITicker {
        process_ticker(&self.ticker)
    }

    pub async fn day_price(&self) -> IDayPrice {
        process_day_price(&self.day_price, self.ohlcv[0].open)
    }

    pub async fn mean_price(&self) -> f64 {
        let percent = DynamicPercent::with_base(self.ohlcv[0].open, 100.0);
        percent.one_value(self.mean_price)
    }
}

pub async fn collect_all(token: &str) -> CollectedData {
    let client = BinanceClient::new().await;
    let ohlcv = client
        .fetch_ohlcv(token, "15m", OHLCV_FETCH_LEN)
        .await
        .try_into()
        .unwrap();
    let ohlcv1h = client
        .fetch_ohlcv(token, "1h", OHLCV_FETCH_LEN)
        .await
        .try_into()
        .unwrap();
    let ohlcv1d = client
        .fetch_ohlcv(token, "1d", OHLCV_FETCH_LEN)
        .await
        .try_into()
        .unwrap();
    let ticker = client.fetch_ticker(token).await;
    let day_price = client.fetch_day_price(token).await;
    let mean_price = client.fetch_average_price(token).await;

    let process_value = spawn_blocking(move || {
        ProcessAll::new(ohlcv, ohlcv1h, ohlcv1d, ticker, day_price, mean_price)
    })
    .await
    .unwrap();

    CollectedData::new(
        token,
        process_value.ohlcv(),
        process_value.ohlcv1h(),
        process_value.ohlcv1d(),
        process_value.ticker().await,
        process_value.day_price().await,
        process_value.mean_price().await,
    )
}

pub fn flat_all(collected_data: Arc<CollectedData>, target: Option<f64>) -> FlattenedData {
    let mut features = Vec::new();

    features.push(collected_data.time.hour_sin);
    features.push(collected_data.time.hour_cos);
    features.push(collected_data.time.min_sin);
    features.push(collected_data.time.min_cos);

    for candle in &collected_data.ohlcv {
        features.push(candle.open);
        features.push(candle.high);
        features.push(candle.low);
        features.push(candle.close);
        features.push(candle.volume);
    }

    for candle in &collected_data.ohlcv1h {
        features.push(candle.open);
        features.push(candle.high);
        features.push(candle.low);
        features.push(candle.close);
        features.push(candle.volume);
    }

    for candle in &collected_data.ohlcv1d {
        features.push(candle.open);
        features.push(candle.high);
        features.push(candle.low);
        features.push(candle.close);
        features.push(candle.volume);
    }

    features.push(collected_data.ticker.bid);
    features.push(collected_data.ticker.ask);
    features.push(collected_data.day_price.open);
    features.push(collected_data.day_price.high);
    features.push(collected_data.day_price.low);
    features.push(collected_data.mean_price);

    features.extend_from_slice(&collected_data.features);

    if target != None {
        features.push(target.unwrap());
        FlattenedData::new(collected_data.token.clone(), features, true)
    } else {
        FlattenedData::new(collected_data.token.clone(), features, false)
    }
}
