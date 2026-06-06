use std::collections::HashMap;

use anyhow::anyhow;
use log::warn;
use smartcore::linalg::basic::arrays::Array;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::xgboost::{XGRegressor, XGRegressorParameters};
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::data::data_interfaces::DataMap;
use crate::data::process::features::auxiliary::corr;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::actors::prediction::PredictionsCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::TargetType;
use crate::models::TaskType;
use crate::models::model::{Model, ModelDependencies};

pub struct XGBoost {
    model: Option<XGRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    task_type: TaskType,
    name: String,
    target_type: TargetType,
    symbol_columns: Option<Vec<String>>,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
    standart: SQLStandart,
    pool: PgPool,
    n_estimators: usize,
    max_depth: u16,
}

impl XGBoost {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        task_type: TaskType,
        target_type: TargetType,
        standart: SQLStandart,
        pool: PgPool,
        n_estimators: usize,
        max_depth: u16,
    ) -> Self {
        Self {
            model: None,
            task_type,
            name: "XGBoost".to_string(),
            target_type,
            symbol_columns: None,
            standart,
            config: load_config(),
            prediction_tx,
            pool,
            n_estimators,
            max_depth,
        }
    }
}

impl ModelDependencies for XGBoost {
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

    fn get_pool(&self) -> Option<&PgPool> {
        Some(&self.pool)
    }
}

#[async_trait::async_trait]
impl Model for XGBoost {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<Option<HashMap<String, f64>>, anyhow::Error> {
        // debug!("tx len: {:?}", x_train);
        // debug!("ty len: {:?}", y_train.len());

        match self.task_type {
            TaskType::Regression => {
                let safe_max_depth = if x_train.shape().0 < 50 {
                    warn!(
                        "Small dataset ({} samples), reducing max_depth from {} to 3",
                        x_train.shape().0,
                        self.max_depth
                    );
                    3
                } else {
                    self.max_depth
                };

                let params = XGRegressorParameters::default()
                    .with_n_estimators(self.n_estimators)
                    .with_max_depth(safe_max_depth)
                    .with_seed(self.get_config().model.seed);

                self.model = Some(match XGRegressor::fit(x_train, y_train, params) {
                    Ok(v) => v,
                    Err(e) => return Err(anyhow!("Failed to fit XGRegressor: {}", e)),
                });
            }
            _ => return Err(anyhow!("XGBoost supports only regression task type!")),
        };

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            match self.evaluate(xv, yv) {
                Ok(result) => return Ok(Some(result)),
                Err(e) => return Err(e),
            }
        }

        Ok(None)
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| anyhow!("XGBoost model not trained yet!"))?;
        let prediction = model
            .predict(values)
            .map_err(|e| anyhow!("Failed to predict with XGRegressor: {}", e))?;
        Ok(prediction)
    }

    async fn handle_mistakes(
        &mut self,
        true_data: DataMap,
        predicted_data: DataMap,
    ) -> Result<(), anyhow::Error> {
        let true_data = true_data.to_vec();
        let predicted_data = predicted_data.to_vec();
        let correlation = corr(&true_data, &predicted_data);

        if correlation < self.config.behaviour.success_threshold {
            self.train()
                .await
                .map_err(|e| anyhow!("Failed to retrain XGBoost model: {}", e))?;
        }

        Ok(())
    }
}
