use super::ccxt::binance::BinanceClient;
use std::collections::HashMap;

pub fn get_volatility(client: &BinanceClient, token: &str) -> f64 {
    let candles: Vec<Vec<f64>> = client.fetch_ohlcv(token, "1d", 10);

    let mut volatilities = vec![];

    for candle in candles {
        let high = candle[1];
        let low = candle[2];
        let open = candle[0];
        let volatility = (high - low) / open;
        volatilities.push(volatility);
    }

    let sum = volatilities.iter().sum::<f64>();
    let avg = sum / volatilities.len() as f64;

    avg
}

fn calculate_atr(ohlcv: &Vec<Vec<f64>>, period: usize) -> Vec<Option<f64>> {
    let mut true_ranges: Vec<f64> = Vec::with_capacity(ohlcv.len());
    let mut atr_values: Vec<Option<f64>> = Vec::with_capacity(ohlcv.len());

    for i in 0..ohlcv.len() {
        let high = ohlcv[i][1];
        let low = ohlcv[i][2];
        let close = ohlcv[i][3];

        let tr = if i == 0 {
            high - low
        } else {
            let prev_close = ohlcv[i - 1][3];
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

fn get_atr(client: &BinanceClient, symbol: &str) -> HashMap<String, Vec<Option<f64>>> {
    let period = 14;
    let limit = 24;

    let mut results: HashMap<String, Vec<Option<f64>>> = HashMap::new();

    let timeframes = vec!["15m", "30m", "1h", "1d"];

    for tf in timeframes {
        let mut ohlcv = client.fetch_ohlcv(symbol, tf, limit);

        if !ohlcv.is_empty() {
            ohlcv.pop();
        }

        let atr = calculate_atr(&ohlcv, period);

        results.insert(tf.to_string(), atr);
    }

    results
}

pub fn get_all_atr(client: &BinanceClient, symbol: &str) -> Vec<f64> {
    let atr_results = get_atr(client, symbol);
    let atr_results = atr_results.values().flat_map(|values| {
        let slice = if values.len() > 10 {
            &values[values.len() - 10..]
        } else {
            &values[..]
        };
        slice.iter().filter_map(|&v| v)
    });
    atr_results.collect()
}
