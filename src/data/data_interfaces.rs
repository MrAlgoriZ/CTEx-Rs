use crate::data::process::data_collection::{AddFeatures, OHLCV_LEN};
use crate::data::requests::database::standart::{
    COLUMNS_FIRST_LAYER, COLUMNS_SECOND_LAYER, COLUMNS_THIRD_LAYER, SQLStandart,
    TARGETS_SINGLE_MODEL,
};
use crate::data::requests::database::standart::{
    TARGETS_FIRST_LAYER, TARGETS_SECOND_LAYER, TARGETS_THIRD_LAYER,
};
use crate::data::requests::time::TimeRequest;

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Copy)]
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
    pub fn as_tuple(self) -> (f64, f64, f64, f64) {
        (self.hour_sin, self.hour_cos, self.min_sin, self.min_cos)
    }
}

#[derive(Debug, Clone)]
pub struct DataMap {
    pub symbol: String,
    pub data: BTreeMap<String, f64>,
}

impl DataMap {
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

    pub fn init(symbol: &str, ohlcv: Vec<Candle>, timeframe: &str) -> Self {
        let ohlcv_wrapped = ohlcv[..OHLCV_LEN].try_into().unwrap();
        let timeframe = Timeframe::from_str(timeframe).unwrap().seconds().unwrap();
        let (hour_sin, hour_cos, minute_sin, minute_cos) = TimeRequest::new().get_time().as_tuple();

        let mut features = AddFeatures::new(ohlcv_wrapped).apply_features();

        features.insert("timeframe".to_string(), timeframe);
        features.insert("hour_sin".to_string(), hour_sin);
        features.insert("hour_cos".to_string(), hour_cos);
        features.insert("minute_sin".to_string(), minute_sin);
        features.insert("minute_cos".to_string(), minute_cos);

        DataMap {
            symbol: symbol.to_string(),
            data: features,
        }
    }

    pub fn with_time(mut self, time: CircleTime) -> Self {
        let (hour_sin, hour_cos, minute_sin, minute_cos) = time.as_tuple();

        self.data.insert("hour_sin".to_string(), hour_sin);
        self.data.insert("hour_cos".to_string(), hour_cos);
        self.data.insert("minute_sin".to_string(), minute_sin);
        self.data.insert("minute_cos".to_string(), minute_cos);

        self
    }

    pub fn from_slice(symbol: &str, timeframe: &str, candles: &[CandleWithTimestamp]) -> Self {
        let ohlcv: Vec<Candle> = candles.iter().map(|candle| candle.to_candle()).collect();
        let time =
            TimeRequest::from_timestamp(candles[(candles.len() - 1) - 1].timestamp).get_time();

        Self::init(symbol, ohlcv, timeframe).with_time(time)
    }
}

#[derive(Debug, Clone, Copy)]
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
