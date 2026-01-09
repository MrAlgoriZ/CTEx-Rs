use anyhow::{Result, anyhow};
use chrono::Local;
use smartcore::api::{Transformer, UnsupervisedEstimator};
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use sqlx::PgPool;
use std::sync::{Arc, Mutex as StdMutex};

use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::{mean_absolute_error, mean_squared_error, r2};
use smartcore::model_selection::train_test_split;
use smartcore::preprocessing::numerical::{StandardScaler, StandardScalerParameters};

use crate::data::data_interfaces::FlattenedData;
use crate::data::requests::database::db_req::select_all_candles;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::{colors::Fore, config::load_config::load_config};

pub struct RFInterface {
    model: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    name: String,
    scaler: Option<StandardScaler<f64>>,
    x_train: Option<DenseMatrix<f64>>,
    x_val: Option<DenseMatrix<f64>>,
    y_train: Option<Vec<f64>>,
    y_val: Option<Vec<f64>>,
    token_columns: Option<Vec<String>>,
    config: Config,
}

impl RFInterface {
    pub fn new() -> Self {
        let config = load_config("config/config.yaml");
        Self {
            model: None,
            name: config.model.name.clone(),
            scaler: None,
            x_train: None,
            x_val: None,
            y_train: None,
            y_val: None,
            token_columns: None,
            config,
        }
    }

    pub fn load_data(&mut self, data: Vec<FlattenedData>) -> Result<(DenseMatrix<f64>, Vec<f64>)> {
        let n_samples = data.len();
        if n_samples == 0 {
            return Err(anyhow!("No data provided"));
        }

        let feature_len = data[0].features.len();
        if feature_len < 1 {
            return Err(anyhow!("Features must have at least 1 element (target)"));
        }
        if data.iter().any(|d| d.features.len() != feature_len) {
            return Err(anyhow!("All features must have the same length"));
        }

        use std::collections::HashMap;

        let tokens: Vec<&str> = data.iter().map(|d| d.token.as_str()).collect();
        let unique_tokens: Vec<&str> = {
            let mut set = std::collections::HashSet::new();
            tokens.iter().for_each(|t| {
                set.insert(*t);
            });
            set.into_iter().collect()
        };
        let n_tokens = unique_tokens.len();

        let token_to_idx: HashMap<&str, usize> = unique_tokens
            .iter()
            .enumerate()
            .map(|(i, &t)| (t, i))
            .collect();

        self.token_columns = Some(
            unique_tokens
                .iter()
                .map(|t| format!("token_name_{}", t))
                .collect(),
        );

        let target_idx = feature_len - 1;

        let mut x_rows: Vec<Vec<f64>> = Vec::with_capacity(n_samples);
        let mut y_target: Vec<f64> = Vec::with_capacity(n_samples);

        for row in &data {
            let mut full_row = vec![0.0; n_tokens + feature_len];

            if let Some(&idx) = token_to_idx.get(row.token.as_str()) {
                full_row[idx] = 1.0;
            }

            for (i, &val) in row.features.iter().enumerate() {
                full_row[n_tokens + i] = val;
            }

            y_target.push(row.features[target_idx]);

            let x_row = full_row[..n_tokens + feature_len - 1].to_vec();
            x_rows.push(x_row);
        }

        let n_features = n_tokens + feature_len - 1;
        let mut flat_x = Vec::with_capacity(n_samples * n_features);
        for row in x_rows {
            flat_x.extend(row);
        }

        let x = DenseMatrix::new(n_samples, n_features, flat_x, false)?;

        Ok((x, y_target))
    }

    pub fn prepare_data(
        &mut self,
        x: DenseMatrix<f64>,
        y_target: Vec<f64>,
        train_ratio: f32,
    ) -> Result<(DenseMatrix<f64>, DenseMatrix<f64>, Vec<f64>, Vec<f64>)> {
        let (x_train, x_val, y_train, y_val) = train_test_split(
            &x,
            &y_target,
            train_ratio,
            true,
            Some(self.config.model.seed),
        );

        let scaler = StandardScaler::fit(&x_train, StandardScalerParameters::default())?;
        let x_train_scaled = scaler.transform(&x_train)?;
        let x_val_scaled = scaler.transform(&x_val)?;

        self.scaler = Some(scaler);
        self.x_train = Some(x_train_scaled.clone());
        self.x_val = Some(x_val_scaled.clone());
        self.y_train = Some(y_train.clone());
        self.y_val = Some(y_val.clone());

        Ok((x_train_scaled, x_val_scaled, y_train, y_val))
    }

    pub fn fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<()> {
        let params = RandomForestRegressorParameters::default()
            .with_n_trees(self.config.model.n_trees)
            .with_max_depth(self.config.model.max_depth)
            .with_seed(self.config.model.seed);

        self.model = Some(RandomForestRegressor::fit(x_train, y_train, params)?);

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            self.evaluate(xv, yv, self.config.prints.model_evualate)?;
        }

        Ok(())
    }

    pub fn evaluate(
        &self,
        x_val: &DenseMatrix<f64>,
        y_val: &Vec<f64>,
        print_results: bool,
    ) -> Result<f64> {
        let model = self
            .model
            .as_ref()
            .ok_or(anyhow!("Model not trained yet"))?;

        let proba = model.predict(x_val)?;
        let y_float: Vec<f64> = y_val.to_vec();

        let accuracy = threshold_accuracy(
            &y_float,
            &proba,
            self.config.behaviour.success_threshold.default,
        );

        if print_results {
            let mae = 1.0 - mean_absolute_error(&y_float, &proba);
            let mse = 1.0 - mean_squared_error(&y_float, &proba);
            let r2_score = 1.0 - r2(&y_float, &proba);
        
            println!(
                "{}[{}] Ошибка по MAE для {} составляет {:.3}%",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                self.name,
                mae
            );
            println!(
                "{}[{}] Ошибка по MSE для {} составляет {:.3}%",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                self.name,
                mse
            );
            println!(
                "{}[{}] Ошибка по R2 для {} составляет {:.3}%",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                self.name,
                r2_score
            );
            println!(
                "{}[{}] Точность по порогу {} для {} составляет {:.3}%",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                self.config.behaviour.success_threshold.default,
                self.name,
                accuracy * 100.0
            );
        }

        Ok(accuracy)
    }

    pub fn predict(&self, x: Vec<f64>, token_name: Option<&str>) -> Result<f64> {
        let token_cols = self
            .token_columns
            .as_ref()
            .ok_or(anyhow!("No token columns defined"))?;
        let model = self
            .model
            .as_ref()
            .ok_or(anyhow!("Model not trained yet"))?;
        let scaler = self.scaler.as_ref().ok_or(anyhow!("Scaler not fitted"))?;

        let mut input: Vec<f64> = Vec::with_capacity(token_cols.len() + x.len());

        let mut token_vec = vec![0.0; token_cols.len()];
        if let Some(tn) = token_name {
            if let Some(idx) = token_cols
                .iter()
                .position(|col| col == &format!("token_name_{}", tn))
            {
                token_vec[idx] = 1.0;
            }
        }
        input.extend(token_vec);

        input.extend(x);

        let input_mat = DenseMatrix::new(1, input.len(), input, false)?;
        let scaled_input = scaler.transform(&input_mat)?;

        let proba = model.predict(&scaled_input)?;
        Ok(proba[0])
    }

    pub fn train(&mut self, data: Vec<FlattenedData>) -> Result<()> {
        let (x, y_target) = self.load_data(data)?;
        let (x_train, x_val, y_train, y_val) =
            self.prepare_data(x, y_target, self.config.model.train_test_split.train_ratio)?;
        self.fit(&x_train, &y_train, Some(&x_val), Some(&y_val))?;
        Ok(())
    }
}

pub async fn train_model(pool: &PgPool, model: &Arc<StdMutex<RFInterface>>) {
    let data = select_all_candles(pool).await.unwrap();
    let model_clone = Arc::clone(model);

    tokio::task::spawn_blocking(move || {
        let mut model_guard = model_clone.lock().unwrap();
        model_guard
            .train(data)
            .expect("The model faced a problem with learning");
    })
    .await
    .unwrap();
}

fn threshold_accuracy(
    y_true: &[f64],
    y_pred: &[f64],
    threshold: f64,
) -> f64 {
    let mut success = 0;

    for (y, p) in y_true.iter().zip(y_pred.iter()) {
        if (y - p).abs() <= threshold {
            success += 1;
        }
    }

    success as f64 / y_true.len() as f64
}
