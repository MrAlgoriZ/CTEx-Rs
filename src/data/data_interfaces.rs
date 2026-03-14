use crate::data::process::data_collection::CollectedData;
use crate::data::requests::database::standart::{
    COLUMNS_FIRST_LAYER, COLUMNS_SECOND_LAYER, COLUMNS_THIRD_LAYER, SQLStandart,
    TARGETS_SINGLE_MODEL,
};
use crate::data::requests::database::standart::{
    TARGETS_FIRST_LAYER, TARGETS_SECOND_LAYER, TARGETS_THIRD_LAYER,
};

use std::{collections::BTreeMap, sync::Arc};

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

#[derive(Debug, Clone)]
pub struct DataMap {
    pub symbol: String,
    pub data: BTreeMap<String, f64>,
}

impl DataMap {
    pub fn from_collected(
        collected: Arc<CollectedData>,
        target: Option<f64>,
        target_name: Option<&str>,
    ) -> Self {
        let mut map = BTreeMap::new();

        if let Some(target) = target
            && let Some(target_name) = target_name
        {
            map.insert(target_name.to_string(), target);
        }
        map.insert("timeframe".to_string(), collected.timeframe);
        map.insert("hour_sin".to_string(), collected.time.hour_sin);
        map.insert("hour_cos".to_string(), collected.time.hour_cos);
        map.insert("minute_sin".to_string(), collected.time.min_sin);
        map.insert("minute_cos".to_string(), collected.time.min_cos);

        for (key, value) in collected.features.iter() {
            map.insert(key.to_string(), *value);
        }

        Self {
            symbol: collected.symbol.to_string(),
            data: map,
        }
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
            };
            let features = match standart {
                SQLStandart::FirstLayer => COLUMNS_FIRST_LAYER,
                SQLStandart::SecondLayer => COLUMNS_SECOND_LAYER,
                SQLStandart::ThirdLayer => COLUMNS_THIRD_LAYER,
                SQLStandart::SingleModel => COLUMNS_FIRST_LAYER,
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
