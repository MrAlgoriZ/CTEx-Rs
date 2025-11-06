use anyhow::{Result, anyhow};
use smartcore::api::{Transformer, UnsupervisedEstimator};
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};

use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::accuracy;
use smartcore::model_selection::train_test_split;
use smartcore::preprocessing::numerical::{StandardScaler, StandardScalerParameters};

use crate::data::data_interfaces::FlattenedData;
use crate::data::requests::database::db_req::select_all_candles;

pub struct RFInterface {
    model_target: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    model_significant: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    name: String,
    scaler: Option<StandardScaler<f64>>,
    x_train: Option<DenseMatrix<f64>>,
    x_val: Option<DenseMatrix<f64>>,
    y_train_target: Option<Vec<f64>>,
    y_val_target: Option<Vec<f64>>,
    y_train_significant: Option<Vec<f64>>,
    y_val_significant: Option<Vec<f64>>,
    token_columns: Option<Vec<String>>,
}

impl RFInterface {
    pub fn new() -> Self {
        Self {
            model_target: None,
            model_significant: None,
            name: "RandomForestRegressor".to_string(),
            scaler: None,
            x_train: None,
            x_val: None,
            y_train_target: None,
            y_val_target: None,
            y_train_significant: None,
            y_val_significant: None,
            token_columns: None,
        }
    }

    pub fn load_data(
        &mut self,
        data: Vec<FlattenedData>,
    ) -> Result<(DenseMatrix<f64>, Vec<f64>, Vec<f64>)> {
        let n_samples = data.len();
        if n_samples == 0 {
            return Err(anyhow!("No data provided"));
        }

        let feature_len = data[0].features.len();
        if feature_len < 2 {
            return Err(anyhow!(
                "Features must have at least 2 elements (target and is_significant)"
            ));
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

        let target_idx = feature_len - 2;
        let sig_idx = feature_len - 1;

        let mut x_rows: Vec<Vec<f64>> = Vec::with_capacity(n_samples);
        let mut y_target: Vec<f64> = Vec::with_capacity(n_samples);
        let mut y_significant: Vec<f64> = Vec::with_capacity(n_samples);

        for row in &data {
            let mut full_row = vec![0.0; n_tokens + feature_len];

            if let Some(&idx) = token_to_idx.get(row.token.as_str()) {
                full_row[idx] = 1.0;
            }

            for (i, &val) in row.features.iter().enumerate() {
                full_row[n_tokens + i] = val;
            }

            y_target.push(row.features[target_idx]);
            y_significant.push(row.features[sig_idx]);

            let x_row = full_row[..n_tokens + feature_len - 2].to_vec();
            x_rows.push(x_row);
        }

        let n_features = n_tokens + feature_len - 2;
        let mut flat_x = Vec::with_capacity(n_samples * n_features);
        for row in x_rows {
            flat_x.extend(row);
        }

        let x = DenseMatrix::new(n_samples, n_features, flat_x, false)?;

        Ok((x, y_target, y_significant))
    }

    pub fn prepare_data(
        &mut self,
        x: DenseMatrix<f64>,
        y_target: Vec<f64>,
        y_significant: Vec<f64>,
        train_ratio: f32,
    ) -> Result<(
        DenseMatrix<f64>,
        DenseMatrix<f64>,
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
        Vec<f64>,
    )> {
        let (x_train, x_val, y_train_target, y_val_target) =
            train_test_split(&x, &y_target, train_ratio, true, Some(42));

        let (_, _, y_train_significant, y_val_significant) =
            train_test_split(&x, &y_significant, train_ratio, true, Some(42));

        let scaler = StandardScaler::fit(&x_train, StandardScalerParameters::default())?;
        let x_train_scaled = scaler.transform(&x_train)?;
        let x_val_scaled = scaler.transform(&x_val)?;

        // Store
        self.scaler = Some(scaler);
        self.x_train = Some(x_train_scaled.clone());
        self.x_val = Some(x_val_scaled.clone());
        self.y_train_target = Some(y_train_target.clone());
        self.y_val_target = Some(y_val_target.clone());
        self.y_train_significant = Some(y_train_significant.clone());
        self.y_val_significant = Some(y_val_significant.clone());

        Ok((
            x_train_scaled,
            x_val_scaled,
            y_train_target,
            y_val_target,
            y_train_significant,
            y_val_significant,
        ))
    }

    pub fn fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train_target: &Vec<f64>,
        y_train_significant: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val_target: Option<&Vec<f64>>,
        y_val_significant: Option<&Vec<f64>>,
    ) -> Result<()> {
        let params = RandomForestRegressorParameters::default()
            .with_n_trees(100)
            .with_max_depth(5)
            .with_seed(42);

        self.model_target = Some(RandomForestRegressor::fit(
            x_train,
            y_train_target,
            params.clone(),
        )?);
        self.model_significant = Some(RandomForestRegressor::fit(
            x_train,
            y_train_significant,
            params,
        )?);

        if let (Some(xv), Some(yvt), Some(yvs)) = (x_val, y_val_target, y_val_significant) {
            self.evaluate(xv, yvt, yvs, true)?;
        }

        Ok(())
    }

    pub fn evaluate(
        &self,
        x_val: &DenseMatrix<f64>,
        y_val_target: &Vec<f64>,
        _y_val_significant: &Vec<f64>,
        print_results: bool,
    ) -> Result<f64> {
        let model = self
            .model_target
            .as_ref()
            .ok_or(anyhow!("Model not trained yet"))?;

        let proba = model.predict(x_val)?;

        let preds: Vec<i32> = proba
            .iter()
            .map(|&p| if p >= 0.5 { 1 } else { 0 })
            .collect();
        let y_int: Vec<i32> = y_val_target.iter().map(|&y| y.round() as i32).collect();

        let accuracy = accuracy(&y_int, &preds) * 100.0;

        if print_results {
            println!("Точность {} составляет {:.3}%", self.name, accuracy);
        }

        Ok(accuracy)
    }

    pub fn predict(&self, x: Vec<f64>, token_name: Option<&str>) -> Result<f64> {
        let token_cols = self
            .token_columns
            .as_ref()
            .ok_or(anyhow!("No token columns defined"))?;
        let model = self
            .model_target
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
        let (x, y_target, y_significant) = self.load_data(data)?;
        let (x_train, x_val, y_train_target, y_val_target, y_train_significant, y_val_significant) =
            self.prepare_data(x, y_target, y_significant, 0.8)?;
        self.fit(
            &x_train,
            &y_train_target,
            &y_train_significant,
            Some(&x_val),
            Some(&y_val_target),
            Some(&y_val_significant),
        )?;
        Ok(())
    }
}

pub async fn train_model(pool: &PgPool, model: &Arc<Mutex<RFInterface>>) {
    let data = select_all_candles(pool).await.unwrap();
    let model_clone = model.clone();
    tokio::task::spawn_blocking(move || {
        let mut model_guard = model_clone.lock().unwrap();
        model_guard
            .train(data)
            .expect("The model faced a problem with learning");
    })
    .await
    .unwrap();
}
