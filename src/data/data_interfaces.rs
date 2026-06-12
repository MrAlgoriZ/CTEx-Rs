use crate::data::process::data_collection::{OHLCV_LEN, collect_features};
use crate::data::requests::database::standart::{
    COLUMNS_FIRST_LAYER, COLUMNS_SECOND_LAYER, COLUMNS_THIRD_LAYER, SQLStandart,
    TARGETS_SINGLE_MODEL,
};
use crate::data::requests::database::standart::{
    TARGETS_FIRST_LAYER, TARGETS_SECOND_LAYER, TARGETS_THIRD_LAYER,
};
use crate::data::requests::time::TimeRequest;

use std::collections::{BTreeMap, HashMap};
use std::ops::Add;

#[derive(Clone, Copy)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Copy)]
pub struct CandleWithTimestamp {
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl CandleWithTimestamp {
    pub fn to_candle(self) -> Candle {
        Candle {
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct Ticker {
    pub bid: f64,
    pub ask: f64,
}

#[derive(Debug, Clone)]
pub struct CircleTime {
    pub hour_sin: f64,
    pub hour_cos: f64,
    pub min_sin: f64,
    pub min_cos: f64,
}

impl CircleTime {
    pub fn to_tuple(&self) -> (f64, f64, f64, f64) {
        (self.hour_sin, self.hour_cos, self.min_sin, self.min_cos)
    }
}

#[derive(Debug, Clone)]
pub struct DataMap {
    pub symbol: Option<String>,
    data: BTreeMap<String, f64>,
}

impl DataMap {
    pub fn new(symbol: Option<String>, data: BTreeMap<String, f64>) -> Self {
        Self { symbol, data }
    }

    pub fn get_only_features(&self) -> BTreeMap<String, f64> {
        let mut map = BTreeMap::new();
        let targets = TARGETS_FIRST_LAYER
            .iter()
            .map(|s| s.to_string())
            .chain(TARGETS_SECOND_LAYER.iter().map(|s| s.to_string()))
            .chain(TARGETS_THIRD_LAYER.iter().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        for (key, value) in self.data.iter() {
            if !targets.contains(key) {
                map.insert(key.to_string(), *value);
            }
        }

        map
    }

    pub fn get_only_targets(&self) -> BTreeMap<String, f64> {
        let mut map = BTreeMap::new();
        let targets = TARGETS_FIRST_LAYER
            .iter()
            .map(|s| s.to_string())
            .chain(TARGETS_SECOND_LAYER.iter().map(|s| s.to_string()))
            .chain(TARGETS_THIRD_LAYER.iter().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        for (key, value) in self.data.iter() {
            if targets.contains(key) {
                map.insert(key.to_string(), *value);
            }
        }

        map
    }

    pub fn has_target(&self) -> bool {
        let targets = TARGETS_FIRST_LAYER
            .iter()
            .map(|s| s.to_string())
            .chain(TARGETS_SECOND_LAYER.iter().map(|s| s.to_string()))
            .chain(TARGETS_THIRD_LAYER.iter().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        for (key, _) in self.data.iter() {
            if targets.contains(key) {
                return true;
            }
        }

        false
    }

    pub fn to_standart(&self, standart: &SQLStandart) -> DataMap {
        let mut map = BTreeMap::new();
        let columns = {
            let targets = match standart {
                SQLStandart::FirstLayer => TARGETS_FIRST_LAYER,
                SQLStandart::SecondLayer => TARGETS_SECOND_LAYER,
                SQLStandart::ThirdLayer => TARGETS_THIRD_LAYER,
                SQLStandart::SingleModel => TARGETS_SINGLE_MODEL,
                SQLStandart::Dummy => &[],
            };
            let features = match standart {
                SQLStandart::FirstLayer => COLUMNS_FIRST_LAYER,
                SQLStandart::SecondLayer => COLUMNS_SECOND_LAYER,
                SQLStandart::ThirdLayer => COLUMNS_THIRD_LAYER,
                SQLStandart::SingleModel => COLUMNS_FIRST_LAYER,
                SQLStandart::Dummy => &[],
            };
            targets
                .iter()
                .chain(features.iter())
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        for (key, value) in self.data.iter() {
            if columns.contains(key) {
                map.insert(key.to_string(), *value);
            }
        }

        DataMap {
            symbol: self.symbol.clone(),
            data: map,
        }
    }

    pub fn init(symbol: Option<&str>, ohlcv: Vec<Candle>, timeframe: &str) -> Self {
        let ohlcv_wrapped = ohlcv[..OHLCV_LEN].try_into().unwrap();
        let timeframe = Timeframe::from_str(timeframe).unwrap().seconds().unwrap();
        let (hour_sin, hour_cos, minute_sin, minute_cos) = TimeRequest::new().get_time().to_tuple();

        let mut features = collect_features(ohlcv_wrapped);

        features.insert("timeframe".to_string(), timeframe);
        features.insert("hour_sin".to_string(), hour_sin);
        features.insert("hour_cos".to_string(), hour_cos);
        features.insert("minute_sin".to_string(), minute_sin);
        features.insert("minute_cos".to_string(), minute_cos);

        DataMap {
            symbol: symbol.map(|s| s.to_string()),
            data: features,
        }
    }

    pub fn with_time(mut self, time: CircleTime) -> Self {
        let (hour_sin, hour_cos, minute_sin, minute_cos) = time.to_tuple();

        self.insert("hour_sin".to_string(), hour_sin);
        self.insert("hour_cos".to_string(), hour_cos);
        self.insert("minute_sin".to_string(), minute_sin);
        self.insert("minute_cos".to_string(), minute_cos);

        self
    }

    pub fn from_slice(
        symbol: Option<&str>,
        timeframe: &str,
        candles: &[CandleWithTimestamp],
    ) -> Self {
        let ohlcv: Vec<Candle> = candles.iter().map(|candle| candle.to_candle()).collect();
        let time =
            TimeRequest::from_timestamp(candles[(candles.len() - 1) - 1].timestamp).get_time();

        Self::init(symbol, ohlcv, timeframe).with_time(time)
    }

    pub fn generate_accuracy() -> Self {
        Self {
            symbol: None,
            data: BTreeMap::from([
                ("future_volatility_confidence".to_string(), 100.0),
                ("future_volume_confidence".to_string(), 100.0),
                ("future_trend_strength_confidence".to_string(), 100.0),
                ("future_range_confidence".to_string(), 100.0),
                ("future_return_mean_confidence".to_string(), 100.0),
                ("future_return_std_confidence".to_string(), 100.0),
                ("future_return_skewness_confidence".to_string(), 100.0),
                ("future_return_kurtosis_confidence".to_string(), 100.0),
                ("risk_score_confidence".to_string(), 100.0),
                ("drawdown_probability_confidence".to_string(), 100.0),
                ("tail_event_probability_confidence".to_string(), 100.0),
                ("volatility_spike_probability_confidence".to_string(), 100.0),
                ("liquidity_drop_probability_confidence".to_string(), 100.0),
            ]),
        }
    }

    pub fn generate_predictions(targets: DataMap) -> Self {
        Self {
            symbol: None,
            data: BTreeMap::from([
                (
                    "future_volatility_pred".to_string(),
                    *targets.get("future_volatility").unwrap(),
                ),
                (
                    "future_volume_pred".to_string(),
                    *targets.get("future_volume").unwrap(),
                ),
                (
                    "future_trend_strength_pred".to_string(),
                    *targets.get("future_trend_strength").unwrap(),
                ),
                (
                    "future_range_pred".to_string(),
                    *targets.get("future_range").unwrap(),
                ),
                (
                    "future_return_mean_pred".to_string(),
                    *targets.get("future_return_mean").unwrap(),
                ),
                (
                    "future_return_std_pred".to_string(),
                    *targets.get("future_return_std").unwrap(),
                ),
                (
                    "future_return_skewness_pred".to_string(),
                    *targets.get("future_return_skewness").unwrap(),
                ),
                (
                    "future_return_kurtosis_pred".to_string(),
                    *targets.get("future_return_kurtosis").unwrap(),
                ),
                (
                    "risk_score_pred".to_string(),
                    *targets.get("risk_score").unwrap(),
                ),
                (
                    "drawdown_probability_pred".to_string(),
                    *targets.get("drawdown_probability").unwrap(),
                ),
                (
                    "tail_event_probability_pred".to_string(),
                    *targets.get("tail_event_probability").unwrap(),
                ),
                (
                    "volatility_spike_probability_pred".to_string(),
                    *targets.get("volatility_spike_probability").unwrap(),
                ),
                (
                    "liquidity_drop_probability_pred".to_string(),
                    *targets.get("liquidity_drop_probability").unwrap(),
                ),
            ]),
        }
    }

    pub fn to_hashmap(&self) -> HashMap<String, f64> {
        let mut map = HashMap::new();
        for (key, value) in self.data.iter() {
            map.insert(key.clone(), *value);
        }
        map
    }
}

impl DataMap {
    pub fn get(&self, key: &str) -> Option<&f64> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: String, value: f64) {
        self.data.insert(key, value);
    }

    pub fn entry(&mut self, key: String) -> std::collections::btree_map::Entry<'_, String, f64> {
        self.data.entry(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect::<Vec<_>>()
    }

    pub fn to_vec(&self) -> Vec<f64> {
        self.data.values().copied().collect()
    }

    pub fn get_data(&self) -> &BTreeMap<String, f64> {
        &self.data
    }
}

impl Add for DataMap {
    type Output = DataMap;

    fn add(mut self, rhs: DataMap) -> Self::Output {
        self.data.extend(rhs.data);
        self.symbol = self.symbol.or(rhs.symbol);

        self
    }
}

pub enum Timeframe {
    M1,
    M3,
    M5,
    M15,
    M30,
    H1,
    H2,
    H4,
    H6,
    H8,
    H12,
    D1,
    D3,
    W1,
}

impl Timeframe {
    pub fn seconds(self) -> Option<f64> {
        match self {
            Timeframe::M1 => Some(60.0),
            Timeframe::M3 => Some(180.0),
            Timeframe::M5 => Some(300.0),
            Timeframe::M15 => Some(900.0),
            Timeframe::M30 => Some(1800.0),

            Timeframe::H1 => Some(3600.0),
            Timeframe::H2 => Some(7200.0),
            Timeframe::H4 => Some(14400.0),
            Timeframe::H6 => Some(21600.0),
            Timeframe::H8 => Some(28800.0),
            Timeframe::H12 => Some(43200.0),

            Timeframe::D1 => Some(86400.0),
            Timeframe::D3 => Some(259200.0),
            Timeframe::W1 => Some(604800.0),
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "1m" => Some(Timeframe::M1),
            "3m" => Some(Timeframe::M3),
            "5m" => Some(Timeframe::M5),
            "15m" => Some(Timeframe::M15),
            "30m" => Some(Timeframe::M30),

            "1h" => Some(Timeframe::H1),
            "2h" => Some(Timeframe::H2),
            "4h" => Some(Timeframe::H4),
            "6h" => Some(Timeframe::H6),
            "8h" => Some(Timeframe::H8),
            "12h" => Some(Timeframe::H12),

            "1d" => Some(Timeframe::D1),
            "3d" => Some(Timeframe::D3),
            "1w" => Some(Timeframe::W1),
            _ => None,
        }
    }
}
