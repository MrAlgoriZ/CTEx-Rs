use anyhow::anyhow;
use chrono::Utc;
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use sqlx::PgPool;

use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::{mean_absolute_error, mean_squared_error, r2};
use smartcore::model_selection::train_test_split;
use std::collections::HashMap;

use crate::data::data_interfaces::FlattenedData;
use crate::data::requests::database::db_req::select_all_candles;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::{colors::Fore, config::load_config::load_config};

pub struct RFInterface {
    model: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    name: String,
    x_train: Option<DenseMatrix<f64>>,
    x_val: Option<DenseMatrix<f64>>,
    y_train: Option<Vec<f64>>,
    y_val: Option<Vec<f64>>,
    symbol_columns: Option<Vec<String>>,
    config: Config,
}

impl RFInterface {
    pub fn new() -> Self {
        let config = load_config("config/config.yaml");
        Self {
            model: None,
            name: config.model.name.clone(),
            x_train: None,
            x_val: None,
            y_train: None,
            y_val: None,
            symbol_columns: None,
            config,
        }
    }

    fn load_data(
        &mut self,
        data: Vec<FlattenedData>,
    ) -> Result<(DenseMatrix<f64>, Vec<f64>), anyhow::Error> {
        let n_samples = data.len();
        if n_samples == 0 {
            return Err(anyhow!("No data provided"));
        }

        let total_len = data[0].features.len();
        if total_len < 2 {
            return Err(anyhow!(
                "Each row must have at least one feature and one target"
            ));
        }

        if total_len != 30 {
            return Err(anyhow!(
                "Expected 30 columns (29 features + 1 target), got {}",
                total_len
            ));
        }

        let feature_len = total_len - 1;
        let target_idx = feature_len;

        if data.iter().any(|d| d.features.len() != total_len) {
            return Err(anyhow!(
                "All rows must have the same number of features + target"
            ));
        }

        let symbols: Vec<&str> = data.iter().map(|d| d.symbol.as_str()).collect();
        let unique_symbols: Vec<&str> = {
            let mut set = std::collections::HashSet::new();
            symbols.iter().for_each(|s| {
                set.insert(*s);
            });
            set.into_iter().collect()
        };
        let n_symbols = unique_symbols.len();

        let symbol_to_idx: HashMap<&str, usize> = unique_symbols
            .iter()
            .enumerate()
            .map(|(i, &s)| (s, i))
            .collect();

        self.symbol_columns = Some(
            unique_symbols
                .iter()
                .map(|s| format!("symbol_name_{}", s))
                .collect(),
        );

        let mut x_rows: Vec<Vec<f64>> = Vec::with_capacity(n_samples);
        let mut y_target: Vec<f64> = Vec::with_capacity(n_samples);
        let mut skipped_nan_features = 0;
        let mut skipped_nan_target = 0;

        for row in data.iter() {
            let target = row.features[target_idx];

            if target.is_nan() {
                skipped_nan_target += 1;
                continue;
            }

            let has_nan_features = row.features[..feature_len].iter().any(|&v| v.is_nan());
            if has_nan_features {
                skipped_nan_features += 1;
                continue;
            }

            let mut full_row = vec![0.0; n_symbols + feature_len];

            if let Some(&idx) = symbol_to_idx.get(row.symbol.as_str()) {
                full_row[idx] = 1.0;
            }

            for (i, &val) in row.features[..feature_len].iter().enumerate() {
                full_row[n_symbols + i] = val;
            }

            x_rows.push(full_row);
            y_target.push(target);
        }

        if self.config.prints.model.metrics {
            println!(
                "{}[{}] Пропущено строк: {} (NaN в target), {} (NaN в признаках)",
                Fore::YELLOW.as_str(),
                Utc::now().format("%H:%M:%S"),
                skipped_nan_target,
                skipped_nan_features
            );
            println!(
                "{}[{}] Осталось {} валидных строк из {}",
                Fore::GREEN.as_str(),
                Utc::now().format("%H:%M:%S"),
                x_rows.len(),
                n_samples
            );
        }

        if x_rows.is_empty() {
            return Err(anyhow!("No valid data after removing NaN values"));
        }

        assert!(
            x_rows.len() == y_target.len(),
            "X and y length mismatch: X={}, y={}",
            x_rows.len(),
            y_target.len()
        );

        let n_features = n_symbols + feature_len;
        let mut flat_x = Vec::with_capacity(x_rows.len() * n_features);
        for row in x_rows.iter() {
            flat_x.extend(row);
        }

        let x = DenseMatrix::new(x_rows.len(), n_features, flat_x, false)?;

        Ok((x, y_target))
    }

    fn prepare_data(
        &mut self,
        x: DenseMatrix<f64>,
        y_target: Vec<f64>,
        train_ratio: f32,
    ) -> Result<(DenseMatrix<f64>, DenseMatrix<f64>, Vec<f64>, Vec<f64>), anyhow::Error> {
        let (x_train, x_val, y_train, y_val) = train_test_split(
            &x,
            &y_target,
            train_ratio,
            true,
            Some(self.config.model.seed),
        );

        self.x_train = Some(x_train.clone());
        self.x_val = Some(x_val.clone());
        self.y_train = Some(y_train.clone());
        self.y_val = Some(y_val.clone());

        Ok((x_train, x_val, y_train, y_val))
    }

    fn fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        let params = RandomForestRegressorParameters::default()
            .with_n_trees(self.config.model.n_trees)
            .with_max_depth(self.config.model.max_depth)
            .with_seed(self.config.model.seed);

        self.model = Some(RandomForestRegressor::fit(x_train, y_train, params)?);

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            self.evaluate(xv, yv)?;
        }

        Ok(())
    }

    fn evaluate(&self, x_val: &DenseMatrix<f64>, y_val: &Vec<f64>) -> Result<f64, anyhow::Error> {
        let model = self
            .model
            .as_ref()
            .ok_or(anyhow!("Model not trained yet"))?;

        let proba = model.predict(x_val)?;
        let y_float: Vec<f64> = y_val.to_vec();

        let thr_accuracy = threshold_accuracy(
            &y_float,
            &proba,
            self.config.behaviour.success_threshold.default,
        );
        let dir_accuracy = direction_accuracy(&y_float, &proba);

        if self.config.prints.model.metrics {
            let mae = mean_absolute_error(&y_float, &proba);
            let mse = mean_squared_error(&y_float, &proba);
            let r2_score = r2(&y_float, &proba);

            println!(
                "{}[{}] Ошибка по MAE для {}: {:.3} pp",
                Fore::WHITE.as_str(),
                Utc::now().format("%H:%M:%S"),
                self.name,
                mae
            );
            println!(
                "{}[{}] Ошибка по MSE для {}: {:.3} (pp²)",
                Fore::WHITE.as_str(),
                Utc::now().format("%H:%M:%S"),
                self.name,
                mse
            );
            println!(
                "{}[{}] Ошибка по R2 для {}: {:.3}",
                Fore::WHITE.as_str(),
                Utc::now().format("%H:%M:%S"),
                self.name,
                r2_score
            );
        }

        if self.config.prints.model.evualate {
            println!(
                "{}[{}] Точность по порогу {} для {} составляет {:.3}%",
                Fore::WHITE.as_str(),
                Utc::now().format("%H:%M:%S"),
                self.config.behaviour.success_threshold.default,
                self.name,
                thr_accuracy * 100.0
            );
            println!(
                "{}[{}] Точность по направлению для {} составляет {:.3}%",
                Fore::WHITE.as_str(),
                Utc::now().format("%H:%M:%S"),
                self.name,
                dir_accuracy * 100.0
            );
        }

        Ok(thr_accuracy)
    }

    pub fn predict(&self, x: Vec<f64>, symbol_name: Option<&str>) -> Result<f64, anyhow::Error> {
        let symbol_cols = self
            .symbol_columns
            .as_ref()
            .ok_or(anyhow!("No symbol columns defined"))?;
        let model = self
            .model
            .as_ref()
            .ok_or(anyhow!("Model not trained yet"))?;
        let mut input: Vec<f64> = Vec::with_capacity(symbol_cols.len() + x.len());

        let mut symbol_vec = vec![0.0; symbol_cols.len()];
        if let Some(tn) = symbol_name {
            if let Some(idx) = symbol_cols
                .iter()
                .position(|col| col == &format!("symbol_name_{}", tn))
            {
                symbol_vec[idx] = 1.0;
            }
        }
        input.extend(symbol_vec);

        input.extend(x);

        let input_mat = DenseMatrix::new(1, input.len(), input, false)?;
        let proba = model.predict(&input_mat)?;
        Ok(proba[0])
    }

    pub fn train(&mut self, data: Vec<FlattenedData>) -> Result<(), anyhow::Error> {
        let (x, y_target) = self.load_data(data)?;
        let (x_train, x_val, y_train, y_val) =
            self.prepare_data(x, y_target, self.config.model.train_test_split.train_ratio)?;
        self.fit(&x_train, &y_train, Some(&x_val), Some(&y_val))?;
        Ok(())
    }
}

pub async fn train_model(pool: &PgPool, model: &mut RFInterface) -> Result<(), anyhow::Error> {
    let data = select_all_candles(pool).await?;
    model.train(data)?;
    Ok(())
}

fn threshold_accuracy(y_true: &[f64], y_pred: &[f64], threshold: f64) -> f64 {
    if y_true.is_empty() {
        return 0.0;
    }

    let mut success = 0;

    for (y, p) in y_true.iter().zip(y_pred.iter()) {
        if (y - p).abs() <= threshold {
            success += 1;
        }
    }

    success as f64 / y_true.len() as f64
}

fn direction_accuracy(y_true: &[f64], y_pred: &[f64]) -> f64 {
    if y_true.is_empty() {
        return 0.0;
    }

    let mut success = 0;

    for (y, p) in y_true.iter().zip(y_pred.iter()) {
        if (y > &0.0 && p > &0.0) || (y < &0.0 && p < &0.0) || (y == &0.0 && p == &0.0) {
            success += 1;
        }
    }

    success as f64 / y_true.len() as f64
}

#[tokio::test]
async fn test_training() -> Result<(), anyhow::Error> {
    let pool = PgPool::connect(&crate::engine::utils::config::load_env::load_env().database_url)
        .await
        .map_err(|e| return anyhow::anyhow!(format!("{}", e)))?;
    let mut model = RFInterface::new();

    train_model(&pool, &mut model).await?;

    Ok(())
}
