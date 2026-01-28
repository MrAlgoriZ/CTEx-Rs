use crate::data::data_interfaces::{ICandle, ITicker};

// TODO Зарефакторить, из-за того, что нормализация невалидная (проверить на логику)
fn ohlcv_f64(ohlcv: &[ICandle]) -> Vec<[f64; 5]> {
    let mut new_ohlcv: Vec<[f64; 5]> = Vec::with_capacity(ohlcv.len());

    for candle in ohlcv {
        new_ohlcv.push([
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
        ])
    }
    new_ohlcv
}

fn flatten_ohlcv(values: &Vec<[f64; 5]>) -> Vec<f64> {
    values
        .iter()
        .flat_map(|arr: &[f64; 5]| arr.iter().copied())
        .collect()
}

fn unflatten_ohlcv(values: &[f64]) -> Vec<ICandle> {
    values
        .chunks_exact(5)
        .map(|chunk| ICandle::new(chunk[0], chunk[1], chunk[2], chunk[3], chunk[4]))
        .collect()
}

pub struct DynamicPercent {
    base: f64,
    x: f64,
}

impl DynamicPercent {
    pub fn new(base: f64, x: f64) -> Self {
        DynamicPercent { base, x }
    }

    pub fn all_values(&self, values: Vec<f64>, skip_fifth: bool) -> Vec<f64> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                if skip_fifth && (i + 1) % 5 == 0 {
                    v
                } else {
                    self.x * (v / self.base)
                }
            })
            .collect()
    }

    pub fn one_value(&self, value: f64) -> f64 {
        self.x * (value / self.base)
    }
}

pub fn process_ohlcv(ohlcv: &[ICandle], base: f64) -> Vec<ICandle> {
    let ohlcv_vec: Vec<ICandle> = ohlcv.to_vec();

    let flat: Vec<f64> = flatten_ohlcv(&ohlcv_f64(&ohlcv_vec));
    let normalized: Vec<f64> = DynamicPercent::new(base, 100.0).all_values(flat, true);
    unflatten_ohlcv(&normalized)
}

pub fn process_ticker(ticker: &ITicker, base: f64) -> ITicker {
    let ticker_percent: DynamicPercent = DynamicPercent::new(base, 100.0);
    ITicker::new(
        ticker_percent.one_value(ticker.bid),
        ticker_percent.one_value(ticker.ask),
        ticker_percent.one_value(ticker.open),
        ticker_percent.one_value(ticker.high),
        ticker_percent.one_value(ticker.low),
        ticker_percent.one_value(ticker.average),
    )
}
