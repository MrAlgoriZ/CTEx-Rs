use std::fs;
use std::path::Path;

use anyhow::{Result, anyhow};
use flexi_logger::{FileSpec, Logger};
use log::{LevelFilter, info};
use ndarray::{Axis, s};
use polars::prelude::*;
use smartcore::api::UnsupervisedEstimator;
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use smartcore::linalg::basic::arrays::Array2;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::accuracy;
use smartcore::model_selection::train_test_split;
use smartcore::preprocessing::numerical::StandardScaler;

use crate::data::data_interfaces::FlattenedData;

pub struct RFInterface {
    model_target: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    model_significant: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    name: String,
    scaler: Option<StandardScaler<f64>>,
    mean: Option<Vec<f64>>,
    std: Option<Vec<f64>>,
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
        fs::create_dir_all("logs").expect("Failed to create logs directory");

        Logger::try_with_str("debug")
            .unwrap()
            .log_to_file(
                FileSpec::default()
                    .directory("logs")
                    .basename("random_forest"),
            )
            .build()
            .unwrap();

        Self {
            model_target: None,
            model_significant: None,
            name: "RandomForestRegressor".to_string(),
            scaler: None,
            mean: None,
            std: None,
            x_train: None,
            x_val: None,
            y_train_target: None,
            y_val_target: None,
            y_train_significant: None,
            y_val_significant: None,
            token_columns: None,
        }
    }

    pub async fn load_data(
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

        let n_features = feature_len - 2;

        let tokens: Vec<String> = data.iter().map(|d| d.token.clone()).collect();

        let token_series = Series::new(PlSmallStr::from_str("token_name"), tokens);

        let token_df = DataFrame::new(vec![token_series.into_column()])?;
        let dummies = token_df.to_dummies(None, "_", false)?; // ОШИБКА: DataFrame не имеет метода .to_dummies()

        self.token_columns = Some(dummies.get_column_names_owned());

        let mut feature_series: Vec<Series> = (0..feature_len)
            .map(|i| {
                let vals: Vec<f64> = data.iter().map(|d| d.features[i]).collect();
                Series::new(PlSmallStr::from_string(format!("f{}", i)), vals)
            })
            .collect();

        let mut df = dummies;
        for s in feature_series.into_iter() {
            df = df.with_column(s)?;
        }

        let target_col = format!("f{}", feature_len - 2);
        let sig_col = format!("f{}", feature_len - 1);

        let x_df = df.drop()?.drop(sig_col.as_str())?; // ОШИБКА (ide не жалуется, но мне кажется она тут есть. Важное примечание: .drop() не требует параметров (стоит использовать другой метод))

        let y_df = df.select([target_col.as_str(), sig_col.as_str()])?;

        let x_arr = x_df.to_ndarray::<Float64Type>(IndexOrder::C)?.to_owned();
        let x = DenseMatrix::from_vec(
            // ОШИБКА DenseMatrix не имеет метода from_vec()
            x_arr.shape()[0],
            x_arr.shape()[1],
            x_arr.into_iter().collect(),
        );

        let y_arr = y_df.to_ndarray::<Float64Type>(IndexOrder::C)?.to_owned();
        let y_target = y_arr.slice(s![.., 0]).to_vec();
        let y_significant = y_arr.slice(s![.., 1]).to_vec();

        info!(
            "Loaded {} samples with {} features from data",
            n_samples,
            x.shape().1
        );

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

        // Fit scaler
        let scaler = StandardScaler::fit(&x_train)?; // ОШИБКА: нужен еще один параметр, как я понял StandartScalerParameters
        let x_train_scaled = scaler.transform(&x_train); // ОШИБКА: impl<T: Number + RealNumber, M: Array2<T>> Transformer<M> for StandardScaler<T> { fn transform(&self, x: &M) -> Result<M, Failed> }
        let x_val_scaled = scaler.transform(&x_val); // ОШИБКА: impl<T: Number + RealNumber, M: Array2<T>> Transformer<M> for StandardScaler<T> { fn transform(&self, x: &M) -> Result<M, Failed> }

        // Store
        self.scaler = Some(scaler);
        self.mean = Some(self.scaler.as_ref().unwrap().mean().clone()); // ОШИБКА: Метода mean() нет для &StandartScaler<f64>
        self.std = Some(self.scaler.as_ref().unwrap().scale().clone()); // ОШИБКА: Метода scale() нет для &StandartScaler<f64>
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

        info!("Random Forest training finished");

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

        let proba = model.predict(x_val);

        let preds: Vec<i32> = proba
            .iter()
            .map(|&p| if p >= 0.5 { 1 } else { 0 }) // ОШИБКА: mismatched types expected struct Vec<f64, _> found type {float} (rustc E0308)
            .collect();
        let y_int: Vec<i32> = y_val_target.iter().map(|&y| y.round() as i32).collect();

        let accuracy = accuracy(&y_int, &preds) * 100.0;

        info!("Evaluation Accuracy: {:.2}%", accuracy);

        if print_results {
            println!("Точность {} составляет {:.3}%", self.name, accuracy);
        }

        Ok(accuracy)
    }

    pub fn predict(
        &self,
        mut x: Vec<f64>,
        token_name: Option<&str>,
        tf: Option<Vec<f64>>,
    ) -> Result<f64> {
        let token_cols = self
            .token_columns
            .as_ref()
            .ok_or(anyhow!("No token columns defined"))?;
        let mean = self
            .mean
            .as_ref()
            .ok_or(anyhow!("Scaler not trained yet"))?;
        let std = self.std.as_ref().ok_or(anyhow!("Scaler not trained yet"))?;
        let model = self
            .model_target
            .as_ref()
            .ok_or(anyhow!("Model not trained yet"))?;

        let mut input: Vec<f64> =
            Vec::with_capacity(token_cols.len() + x.len() + tf.as_ref().map_or(0, |t| t.len()));
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

        if let Some(t) = tf {
            input.extend(t);
        }

        input.extend(x);

        if input.len() != mean.len() {
            return Err(anyhow!("Input length mismatch with trained features"));
        }
        for (i, val) in input.iter_mut().enumerate() {
            *val = (*val - mean[i]) / (std[i] + 1e-8);
        }

        let input_mat = DenseMatrix::from_vec(1, input.len(), input); // ОШИБКА: DenseMatrix не имеет метода from_vec()
        let proba = model.predict(&input_mat)?;

        Ok(proba[0])
    }

    pub async fn train(&mut self, data: Vec<FlattenedData>) -> Result<()> {
        let (x, y_target, y_significant) = self.load_data(data).await?;
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
