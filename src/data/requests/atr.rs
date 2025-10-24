use super::ccxt::binance::BinanceClient;
use crate::data::data_interfaces::*;
use std::collections::HashMap;

pub async fn get_volatility(client: &BinanceClient, token: &str) -> f64 {
    let candles = match client.fetch_ohlcv(token, "1d", 10).await {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error fetching candles: {}", e);
            return 0.0;
        }
    };

    let mut volatilities = Vec::new();

    for candle in candles {
        let high = candle.high;
        let low = candle.low;
        let open = candle.open;
        let volatility = (high - low) / open;
        volatilities.push(volatility);
    }

    let sum = volatilities.iter().sum::<f64>();
    let avg = sum / volatilities.len() as f64;

    avg
}

fn calculate_atr(ohlcv: &[ICandle], period: usize) -> Vec<Option<f64>> {
    let mut true_ranges: Vec<f64> = Vec::with_capacity(ohlcv.len());
    let mut atr_values: Vec<Option<f64>> = Vec::with_capacity(ohlcv.len());

    for i in 0..ohlcv.len() {
        let high = ohlcv[i].high;
        let low = ohlcv[i].low;
        let close = ohlcv[i].close;

        let tr = if i == 0 {
            high - low
        } else {
            let prev_close = ohlcv[i - 1].close;
            let tr1 = high - low;
            let tr2 = (high - prev_close).abs();
            let tr3 = (low - prev_close).abs();
            tr1.max(tr2).max(tr3)
        };

        true_ranges.push(tr);

        if i + 1 >= period {
            let start = i + 1 - period;
            let atr: f64 = true_ranges[start..=i].iter().sum::<f64>() / period as f64;
            let atr_percent = (atr / close) * 100.0;
            atr_values.push(Some(atr_percent));
        } else {
            atr_values.push(None);
        }
    }

    atr_values
}

pub async fn get_atr(client: &BinanceClient, symbol: &str) -> HashMap<String, Vec<Option<f64>>> {
    let period = 14;
    let limit = 24;

    let mut results = HashMap::new();
    let timeframes = vec!["15m", "30m", "1h", "1d"];

    for tf in timeframes {
        let mut ohlcv = match client.fetch_ohlcv(symbol, tf, limit).await {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error fetching ohlcv for {}: {}", tf, e);
                Vec::new()
            }
        };

        if !ohlcv.is_empty() {
            ohlcv.pop(); // убираем последнюю незакрытую свечу
        }

        let atr = calculate_atr(&ohlcv, period);
        results.insert(tf.to_string(), atr);
    }

    results
}

pub async fn get_all_atr(client: &BinanceClient, symbol: &str) -> Vec<f64> {
    let atr_results = get_atr(client, symbol).await;
    atr_results
        .values()
        .flat_map(|values| {
            let slice = if values.len() > 10 {
                &values[values.len() - 10..]
            } else {
                &values[..]
            };
            slice.iter().filter_map(|&v| v)
        })
        .collect()
}
