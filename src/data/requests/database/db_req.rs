use crate::{data::data_interfaces::FlattenedData, engine::utils::colors::Fore};
use sqlx::{Error, PgPool, Row, query};

const COLUMNS: &'static [&str] = &[
    "hour_sin",
    "hour_cos",
    "min_sin",
    "min_cos",
    "open_1",
    "high_1",
    "low_1",
    "close_1",
    "volume_1",
    "open_2",
    "high_2",
    "low_2",
    "close_2",
    "volume_2",
    "open_3",
    "high_3",
    "low_3",
    "close_3",
    "volume_3",
    "open_4",
    "high_4",
    "low_4",
    "close_4",
    "volume_4",
    "open_5",
    "high_5",
    "low_5",
    "close_5",
    "volume_5",
    "open_6",
    "high_6",
    "low_6",
    "close_6",
    "volume_6",
    "open_7",
    "high_7",
    "low_7",
    "close_7",
    "volume_7",
    "open_8",
    "high_8",
    "low_8",
    "close_8",
    "volume_8",
    "open_9",
    "high_9",
    "low_9",
    "close_9",
    "volume_9",
    "open_10",
    "high_10",
    "low_10",
    "close_10",
    "volume_10",
    "bid",
    "ask",
    "day_open",
    "day_high",
    "day_low",
    "mean_price",
    "spread_rel",
    "mid_price",
    "pressure_side",
    "bid_ask_ratio",
    "mid_distance_day_highlow",
    "body_1",
    "body_strength_1",
    "body_2",
    "body_strength_2",
    "body_3",
    "body_strength_3",
    "body_4",
    "body_strength_4",
    "body_5",
    "body_strength_5",
    "body_6",
    "body_strength_6",
    "body_7",
    "body_strength_7",
    "body_8",
    "body_strength_8",
    "body_9",
    "body_strength_9",
    "body_10",
    "body_strength_10",
    "volatility",
    "target",
];

pub async fn insert_candle(
    pool: &PgPool,
    token: &str,
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
        "INSERT INTO candles (token, {}) VALUES ($1, {})",
        COLUMNS.join(", "),
        placeholders
    );

    let mut q = query(&sql).bind(token);
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
        "SELECT token, {} FROM candles",
        COLUMNS.join(", ")
    ))
    .fetch_all(pool)
    .await?;

    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        let token: String = row.try_get("token")?;

        let mut values = Vec::new();
        for i in 2..row.columns().len() {
            let value: Option<f64> = row.try_get(i)?;
            values.push(value.unwrap_or(f64::NAN));
        }

        result.push(FlattenedData::new(token, values, true));
    }

    Ok(result)
}
