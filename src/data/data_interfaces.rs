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

#[derive(Debug)]
pub struct FlattenedData {
    pub symbol: String,
    pub features: Vec<f64>,
    with_target: bool,
}

impl FlattenedData {
    pub fn new(symbol: String, features: Vec<f64>, with_target: bool) -> Self {
        FlattenedData {
            symbol,
            features,
            with_target,
        }
    }
    pub fn is_there_a_target(&self) -> bool {
        self.with_target
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
