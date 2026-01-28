use crate::data::data_interfaces::Candle;

pub fn get_volatility(candles: &[Candle]) -> f64 {
    let mut volatilities = Vec::with_capacity(candles.len());

    for candle in candles.iter() {
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
