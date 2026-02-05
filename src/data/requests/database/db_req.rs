use crate::{data::data_interfaces::FlattenedData, engine::utils::colors::Fore};
use sqlx::{Error, PgPool, Row, query};

const COLUMNS: &'static [&str] = &[
    "timeframe",
    "hour_sin",
    "hour_cos",
    "min_sin",
    "min_cos",
    "return_1",
    "return_2",
    "return_3",
    "return_5",
    "return_10",
    "log_return_1",
    "vol_rolling_3",
    "vol_rolling_5",
    "vol_rolling_10",
    "volume_change_1",
    "volume_change_3",
    "spread",
    "ema_fast",
    "ema_slow",
    "rsi_7",
    "rsi_14",
    "macd_diff",
    "bb_percent",
    "zscore_price",
    "mean_reversion",
    "breakout_high",
    "breakout_low",
    "return_1_over_vol",
    "return_5_over_vol",
    "target",
];

pub async fn insert_candle(
    pool: &PgPool,
    symbol: &str,
    values: &[f64],
) -> Result<(), anyhow::Error> {
    if values.len() != COLUMNS.len() {
        return Err(anyhow::anyhow!(format!(
            "Неправильная длина. Ожидалось {}, получено {}",
            COLUMNS.len(),
            values.len()
        )));
    }

    let placeholders = (2..=COLUMNS.len() + 1)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT INTO new_candles (symbol, {}) VALUES ($1, {})",
        COLUMNS.join(", "),
        placeholders
    );

    let mut q = query(&sql).bind(symbol);
    for v in values {
        q = q.bind(v);
    }

    q.execute(pool).await.map_err(|e| {
        eprintln!("{}Данные не загрузились в бд: {:?}", Fore::RED.as_str(), e);
        anyhow::anyhow!(format!("{e}"))
    })?;

    Ok(())
}

pub async fn select_all_candles(pool: &PgPool) -> Result<Vec<FlattenedData>, Error> {
    let rows = sqlx::query(&format!(
        "SELECT symbol, {} FROM new_candles",
        COLUMNS.join(", ")
    ))
    .fetch_all(pool)
    .await?;

    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        let symbol: String = row.try_get("symbol")?;

        let mut values = Vec::new();
        for i in 2..row.columns().len() {
            let value: Option<f64> = row.try_get(i)?;
            values.push(value.unwrap_or(f64::NAN));
        }

        result.push(FlattenedData::new(symbol, values, true));
    }

    Ok(result)
}
