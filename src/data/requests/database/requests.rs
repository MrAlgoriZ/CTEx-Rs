use anyhow::{Result, anyhow};
use log::error;
use sqlx::{Column, Error, PgPool, Row, query};
use std::collections::BTreeMap;

use crate::data::data_interfaces::DataMap;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::utils::colors::Fore;

impl SQLStandart {
    pub async fn select_all(&self, pool: &PgPool) -> Result<Vec<DataMap>, Error> {
        let columns = self.get_columns();
        let targets = self.get_target_list();

        let all_columns = columns
            .iter()
            .map(|c| c.to_string())
            .chain(targets.iter().map(|c| c.to_string()))
            .collect::<Vec<_>>();

        let rows = sqlx::query(&format!(
            "SELECT symbol, {} FROM dataset",
            all_columns.join(", ")
        ))
        .fetch_all(pool)
        .await?;

        let mut result: Vec<DataMap> = Vec::with_capacity(rows.len());

        for row in rows {
            let symbol: String = row.try_get("symbol")?;

            let mut values = BTreeMap::new();
            for column in row.columns().iter().skip(1) {
                let name = column.name();
                let value: Option<f64> = row.try_get(name)?;
                values.insert(name.to_string(), value.unwrap_or(f64::NAN));
            }

            result.push(DataMap::new(Some(symbol), values));
        }

        Ok(result)
    }

    pub async fn insert_row(&self, pool: &PgPool, values: DataMap) -> Result<()> {
        let columns = values.get_keys();
        let columns_str = columns.join(", ");

        let mut placeholder_index: u128 = 1;
        let placeholders = columns
            .iter()
            .map(|_| {
                placeholder_index += 1;
                format!("${}", placeholder_index)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO dataset (symbol, {}) VALUES ($1, {})",
            columns_str, placeholders
        );

        let mut q = query(&sql).bind(values.symbol.clone());

        for k in columns.iter() {
            q = q.bind(values.get(k).copied().unwrap_or(f64::NAN));
        }

        q.execute(pool).await.map_err(|e| {
            error!("{}Data not saved to database: {:?}", Fore::RED.as_str(), e);
            anyhow!(format!("{e}"))
        })?;

        Ok(())
    }
}
