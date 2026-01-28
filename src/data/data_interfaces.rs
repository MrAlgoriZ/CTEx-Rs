#[derive(Debug, Clone, Copy)]
pub struct ICandle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl ICandle {
    pub fn new(open: f64, high: f64, low: f64, close: f64, volume: f64) -> Self {
        ICandle {
            open,
            high,
            low,
            close,
            volume,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ITicker {
    pub bid: f64,
    pub ask: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub average: f64,
}

impl ITicker {
    pub fn new(bid: f64, ask: f64, open: f64, high: f64, low: f64, average: f64) -> Self {
        ITicker {
            bid,
            ask,
            open,
            high,
            low,
            average,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ITime {
    pub hour_sin: f64,
    pub hour_cos: f64,
    pub min_sin: f64,
    pub min_cos: f64,
}

impl ITime {
    pub fn new(hour_sin: f64, hour_cos: f64, min_sin: f64, min_cos: f64) -> Self {
        ITime {
            hour_sin,
            hour_cos,
            min_sin,
            min_cos,
        }
    }
}

#[derive(Debug)]
pub struct FlattenedData {
    pub token: String,
    pub features: Vec<f64>,
    with_target: bool,
}

impl FlattenedData {
    pub fn new(token: String, features: Vec<f64>, with_target: bool) -> Self {
        FlattenedData {
            token,
            features,
            with_target,
        }
    }
    pub fn is_there_a_target(&self) -> bool {
        self.with_target
    }
}
