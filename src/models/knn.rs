use anyhow::anyhow;
use smartcore::algorithm::neighbour::KNNAlgorithmName;
use smartcore::api::{Transformer, UnsupervisedEstimator};
use smartcore::linalg::basic::arrays::Array;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::distance::euclidian::Euclidian;
use smartcore::neighbors::KNNWeightFunction;
use smartcore::neighbors::knn_regressor::{KNNRegressor, KNNRegressorParameters};
use smartcore::preprocessing::numerical::StandardScaler;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::data::data_interfaces::DataMap;
use crate::data::process::features::auxiliary::corr;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::manager::PredictionsCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::TargetType;
use crate::models::model::{Model, ModelDependencies};

pub struct KNN {
    model: Option<KNNRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>, Euclidian<f64>>>,
    name: String,
    target_type: TargetType,
    symbol_columns: Option<Vec<String>>,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
    standart: SQLStandart,
    pool: PgPool,
    algorithm: String,
    weight: String,
    k: usize,
}

impl KNN {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        target_type: TargetType,
        standart: SQLStandart,
        pool: PgPool,
        algorithm: String,
        weight: String,
        k: usize,
    ) -> Self {
        Self {
            model: None,
            name: "KNN".to_string(),
            target_type,
            symbol_columns: None,
            config: load_config("config/config.yaml"),
            prediction_tx,
            standart,
            pool,
            algorithm,
            weight,
            k,
        }
    }
}

impl ModelDependencies for KNN {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_config(&self) -> &Config {
        &self.config
    }

    fn change_symbol_columns(&mut self, symbol_columns: Option<Vec<String>>) {
        self.symbol_columns = symbol_columns;
    }

    fn get_prediction_tx(&self) -> &Option<mpsc::Sender<PredictionsCommand>> {
        &self.prediction_tx
    }

    fn check_model_trained(&self) -> bool {
        match self.model.as_ref() {
            Some(_) => return true,
            None => return false,
        }
    }

    fn get_symbol_columns(&self) -> &Option<Vec<String>> {
        &self.symbol_columns
    }

    fn get_target_name(&self) -> &str {
        self.target_type.get_name()
    }

    fn get_standart(&self) -> &SQLStandart {
        &self.standart
    }

    fn get_pool(&self) -> Option<&sqlx::PgPool> {
        Some(&self.pool)
    }
}

#[async_trait::async_trait]
impl Model for KNN {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        let algorithm: &str = &self.algorithm;
        let weight: &str = &self.weight;
        let algorithm_value = match algorithm {
            "CoverTree" => Some(KNNAlgorithmName::CoverTree),
            "LinearSearch" => Some(KNNAlgorithmName::LinearSearch),
            _ => None,
        };
        let weight_value = match weight {
            "Distance" => Some(KNNWeightFunction::Distance),
            "Uniform" => Some(KNNWeightFunction::Uniform),
            _ => None,
        };

        let params = KNNRegressorParameters::default()
            .with_algorithm(algorithm_value.unwrap_or(KNNAlgorithmName::CoverTree))
            .with_weight(weight_value.unwrap_or(KNNWeightFunction::Uniform))
            .with_k(self.k);

        self.model = Some(KNNRegressor::fit(x_train, y_train, params)?);

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            self.evaluate(xv, yv)?;
        }

        Ok(())
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        let model = self
            .model
            .as_ref()
            .ok_or(anyhow!("Model not trained yet!"))?;
        let prediction = model.predict(values)?;
        Ok(prediction)
    }

    fn normalize(
        &self,
        x_train: DenseMatrix<f64>,
        x_val: DenseMatrix<f64>,
    ) -> (DenseMatrix<f64>, DenseMatrix<f64>) {
        let n_symbols = self
            .get_symbol_columns()
            .as_ref()
            .map(|cols| cols.len())
            .unwrap_or(0);

        let (n_rows_train, n_cols) = (x_train.shape().0, x_train.shape().1);
        let (n_rows_val, _) = (x_val.shape().0, x_val.shape().1);

        if n_symbols == 0 || n_symbols >= n_cols {
            let scaler = StandardScaler::fit(&x_train, Default::default())
                .expect("Failed to fit StandardScaler");

            let x_train_scaled = scaler
                .transform(&x_train)
                .expect("Failed to transform training data");

            let x_val_scaled = scaler
                .transform(&x_val)
                .expect("Failed to transform validation data");

            return (x_train_scaled, x_val_scaled);
        }

        let train_raw: Vec<f64> = x_train.iter().map(|v| v.clone()).collect();
        let val_raw: Vec<f64> = x_val.iter().map(|v| v.clone()).collect();

        let mut symbol_train_data = Vec::with_capacity(n_rows_train * n_symbols);
        let mut numeric_train_data = Vec::with_capacity(n_rows_train * (n_cols - n_symbols));

        let mut symbol_val_data = Vec::with_capacity(n_rows_val * n_symbols);
        let mut numeric_val_data = Vec::with_capacity(n_rows_val * (n_cols - n_symbols));

        for i in 0..n_rows_train {
            let row_start = i * n_cols;

            for j in 0..n_symbols {
                symbol_train_data.push(train_raw[row_start + j]);
            }

            for j in n_symbols..n_cols {
                numeric_train_data.push(train_raw[row_start + j]);
            }
        }

        for i in 0..n_rows_val {
            let row_start = i * n_cols;

            for j in 0..n_symbols {
                symbol_val_data.push(val_raw[row_start + j]);
            }

            for j in n_symbols..n_cols {
                numeric_val_data.push(val_raw[row_start + j]);
            }
        }

        let numeric_train =
            DenseMatrix::new(n_rows_train, n_cols - n_symbols, numeric_train_data, false)
                .expect("Failed to create numeric training matrix");

        let numeric_val = DenseMatrix::new(n_rows_val, n_cols - n_symbols, numeric_val_data, false)
            .expect("Failed to create numeric validation matrix");

        let scaler = StandardScaler::fit(&numeric_train, Default::default())
            .expect("Failed to fit StandardScaler");

        let numeric_train_scaled: DenseMatrix<f64> = scaler
            .transform(&numeric_train)
            .expect("Failed to transform training numeric data");

        let numeric_val_scaled: DenseMatrix<f64> = scaler
            .transform(&numeric_val)
            .expect("Failed to transform validation numeric data");

        let numeric_train_scaled_raw: Vec<&f64> = numeric_train_scaled.iter().collect();
        let numeric_val_scaled_raw: Vec<&f64> = numeric_val_scaled.iter().collect();

        let mut train_final_data = Vec::with_capacity(n_rows_train * n_cols);
        let mut val_final_data = Vec::with_capacity(n_rows_val * n_cols);

        for i in 0..n_rows_train {
            for j in 0..n_symbols {
                train_final_data.push((symbol_train_data[i * n_symbols + j]).clone());
            }
            for j in 0..(n_cols - n_symbols) {
                train_final_data
                    .push((numeric_train_scaled_raw[i * (n_cols - n_symbols) + j]).clone());
            }
        }

        for i in 0..n_rows_val {
            for j in 0..n_symbols {
                val_final_data.push((symbol_val_data[i * n_symbols + j]).clone());
            }
            for j in 0..(n_cols - n_symbols) {
                val_final_data.push((numeric_val_scaled_raw[i * (n_cols - n_symbols) + j]).clone());
            }
        }

        let x_train_final = DenseMatrix::new(n_rows_train, n_cols, train_final_data, false)
            .expect("Failed to create final training matrix");

        let x_val_final = DenseMatrix::new(n_rows_val, n_cols, val_final_data, false)
            .expect("Failed to create final validation matrix");

        (x_train_final, x_val_final)
    }

    async fn handle_mistakes(
        &mut self,
        true_data: DataMap,
        predicted_data: DataMap,
    ) -> Result<(), anyhow::Error> {
        let true_data: Vec<f64> = true_data.data.values().map(|v| *v).collect();
        let predicted_data: Vec<f64> = predicted_data.data.values().map(|v| *v).collect();
        let correlation = corr(&true_data, &predicted_data);
        println!("Corr: {}", correlation);

        if correlation > self.config.behaviour.success_threshold {
            self.train().await?;
        }

        Ok(())
    }
}
