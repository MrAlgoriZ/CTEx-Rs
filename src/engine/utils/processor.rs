use crate::data::data_interfaces::ICandle;
use tokio::task;

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
    values.iter().flat_map(|arr: &[f64; 5]| arr.iter().copied()).collect()
}

fn unflatten_ohlcv(values: &[f64]) -> Vec<ICandle> {
    values
        .chunks_exact(5)
        .map(|chunk| ICandle::new(chunk[0], chunk[1], chunk[2], chunk[3], chunk[4]))
        .collect()
}

pub fn dynamic_percent(values: &[f64], x: f64, skip_fifth: bool) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }

    let base = values[0];
    values
        .iter()
        .enumerate()
        .map(|(i, &v)| if skip_fifth && (i + 1) % 5 == 0 { v } else { x * (v / base) })
        .collect()
}

pub async fn process_ohlcv(ohlcv: &[ICandle]) -> Vec<ICandle> {
    let ohlcv_vec: Vec<ICandle> = ohlcv.to_vec();

    task::spawn_blocking(move || {
        let flat: Vec<f64> = flatten_ohlcv(&ohlcv_f64(&ohlcv_vec));
        let normalized: Vec<f64> = dynamic_percent(&flat, 100.0, true);
        unflatten_ohlcv(&normalized)
    })
    .await
    .expect("blocking task panicked")
}