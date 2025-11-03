use crate::data::data_interfaces::FlattenedData;
use sqlx::{Error, PgPool, Row, query};

pub async fn insert_candle(pool: &PgPool, token: &str, values: Vec<f64>) -> Result<(), Error> {
    if values.len() != 230 {
        return Err(Error::RowNotFound);
    }

    let placeholders: Vec<String> = (2..=231).map(|i| format!("${}", i)).collect();

    let columns = "
        hour_sin, hour_cos, min_sin, min_cos,
        open_1, high_1, low_1, close_1, volume_1,
        open_2, high_2, low_2, close_2, volume_2,
        open_3, high_3, low_3, close_3, volume_3,
        open_4, high_4, low_4, close_4, volume_4,
        open_5, high_5, low_5, close_5, volume_5,
        open_6, high_6, low_6, close_6, volume_6,
        open_7, high_7, low_7, close_7, volume_7,
        open_8, high_8, low_8, close_8, volume_8,
        open_9, high_9, low_9, close_9, volume_9,
        open_10, high_10, low_10, close_10, volume_10,
        open_1h, high_1h, low_1h, close_1h, volume_1h,
        open_2h, high_2h, low_2h, close_2h, volume_2h,
        open_3h, high_3h, low_3h, close_3h, volume_3h,
        open_4h, high_4h, low_4h, close_4h, volume_4h,
        open_5h, high_5h, low_5h, close_5h, volume_5h,
        open_6h, high_6h, low_6h, close_6h, volume_6h,
        open_7h, high_7h, low_7h, close_7h, volume_7h,
        open_8h, high_8h, low_8h, close_8h, volume_8h,
        open_9h, high_9h, low_9h, close_9h, volume_9h,
        open_10h, high_10h, low_10h, close_10h, volume_10h,
        open_1d, high_1d, low_1d, close_1d, volume_1d,
        open_2d, high_2d, low_2d, close_2d, volume_2d,
        open_3d, high_3d, low_3d, close_3d, volume_3d,
        open_4d, high_4d, low_4d, close_4d, volume_4d,
        open_5d, high_5d, low_5d, close_5d, volume_5d,
        open_6d, high_6d, low_6d, close_6d, volume_6d,
        open_7d, high_7d, low_7d, close_7d, volume_7d,
        open_8d, high_8d, low_8d, close_8d, volume_8d,
        open_9d, high_9d, low_9d, close_9d, volume_9d,
        open_10d, high_10d, low_10d, close_10d, volume_10d,
        bid, ask, day_open, day_high, day_low, mean_price, spread_rel, mid_price,
        pressure_side, pressure_side1h, pressure_side1d, bid_ask_ratio,
        mid_distance_day_highlow, body_1, body_strength_1, body_2, body_strength_2,
        body_3, body_strength_3, body_4, body_strength_4, body_5, body_strength_5,
        body_6, body_strength_6, body_7, body_strength_7, body_8, body_strength_8,
        body_9, body_strength_9, body_10, body_strength_10, volatility,
        body_1h, body_strength_1h, body_2h, body_strength_2h, body_3h, body_strength_3h,
        body_4h, body_strength_4h, body_5h, body_strength_5h, body_6h, body_strength_6h,
        body_7h, body_strength_7h, body_8h, body_strength_8h, body_9h, body_strength_9h,
        body_10h, body_strength_10h, volatility_1h,
        body_1d, body_strength_1d, body_2d, body_strength_2d, body_3d, body_strength_3d,
        body_4d, body_strength_4d, body_5d, body_strength_5d, body_6d, body_strength_6d,
        body_7d, body_strength_7d, body_8d, body_strength_8d, body_9d, body_strength_9d,
        body_10d, body_strength_10d, volatility_1d
    ";

    let sql = format!(
        "INSERT INTO candles (token, {}) VALUES ($1, {})",
        columns,
        placeholders.join(", ")
    );

    let mut q = query(&sql).bind(token);

    for v in values {
        q = q.bind(v);
    }

    q.execute(pool).await?;

    return Ok(());
}

pub async fn select_all_candles(pool: &PgPool) -> Result<Vec<FlattenedData>, Error> {
    let rows = sqlx::query("SELECT * FROM candles").fetch_all(pool).await?;

    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        let token: String = row.try_get("token")?;

        let mut values = Vec::new();
        for i in 2..row.columns().len() {
            let value: Option<f64> = row.try_get(i)?;
            values.push(value.unwrap_or(f64::NAN));
        }

        result.push(FlattenedData {
            token,
            features: values,
        });
    }

    Ok(result)
}
