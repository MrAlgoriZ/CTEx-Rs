use std::collections::HashMap;

use anyhow::anyhow;
use log::error;
use smartcore::ensemble::extra_trees_regressor::{
    ExtraTreesRegressor, ExtraTreesRegressorParameters,
};
use smartcore::linalg::basic::matrix::DenseMatrix;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::data::data_interfaces::DataMap;
use crate::data::process::features::auxiliary::corr;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::actors::prediction::PredictionsCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::model::{Model, ModelDependencies};
use crate::models::{TargetType, TaskType};

pub struct ExtraTrees {
    model: Option<ExtraTreesRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    task_type: TaskType,
    name: String,
    target_type: TargetType,
    symbol_columns: Option<Vec<String>>,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
    standart: SQLStandart,
    pool: PgPool,
    n_trees: usize,
    max_depth: u16,
    min_samples_leaf: usize,
    min_samples_split: usize,
    m: usize,
}

impl ExtraTrees {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        task_type: TaskType,
        target_type: TargetType,
        standart: SQLStandart,
        pool: PgPool,
        n_trees: usize,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
        m: usize,
    ) -> Self {
        Self {
            model: None,
            task_type,
            name: "ExtraTrees".to_string(),
            target_type,
            symbol_columns: None,
            config: load_config(),
            prediction_tx,
            standart,
            pool,
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        }
    }
}

impl ModelDependencies for ExtraTrees {
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
impl Model for ExtraTrees {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<Option<HashMap<String, f64>>, anyhow::Error> {
        match self.task_type {
            TaskType::Regression => {
                let params = ExtraTreesRegressorParameters::default()
                    .with_n_trees(self.n_trees)
                    .with_max_depth(self.max_depth)
                    .with_seed(self.get_config().model.seed)
                    .with_min_samples_leaf(self.min_samples_leaf)
                    .with_min_samples_split(self.min_samples_split)
                    .with_m(self.m);

                self.model = Some(
                    ExtraTreesRegressor::fit(x_train, y_train, params)
                        .map_err(|e| anyhow!("Failed to fit ExtraTreesRegressor: {}", e))?,
                );
            }
            _ => return Err(anyhow!("ExtraTrees supports only regression task type!")),
        };

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            match self.evaluate(xv, yv) {
                Ok(result) => return Ok(Some(result)),
                Err(e) => error!("Failed to evaluate RandomForest model: {}", e),
            }
        }

        Ok(None)
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| anyhow!("ExtraTrees model not trained yet!"))?;
        let prediction = model
            .predict(values)
            .map_err(|e| anyhow!("Failed to predict with ExtraTreesRegressor: {}", e))?;
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
                .map_err(|e| anyhow!("Failed to retrain ExtraTrees model: {}", e))?;
        }

        Ok(())
    }
}
