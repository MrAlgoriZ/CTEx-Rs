use std::collections::HashMap;

use anyhow::{Result, anyhow};
use log::error;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::tree::decision_tree_classifier::{
    DecisionTreeClassifier, DecisionTreeClassifierParameters,
};
use smartcore::tree::decision_tree_regressor::{
    DecisionTreeRegressor, DecisionTreeRegressorParameters,
};
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

pub struct DecisionTree {
    regression_model: Option<DecisionTreeRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    classification_model: Option<DecisionTreeClassifier<f64, i32, DenseMatrix<f64>, Vec<i32>>>,
    task_type: TaskType,
    name: String,
    target_type: TargetType,
    symbol_columns: Option<Vec<String>>,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
    standart: SQLStandart,
    pool: PgPool,
    max_depth: u16,
    min_samples_leaf: usize,
    min_samples_split: usize,
}

impl DecisionTree {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        task_type: TaskType,
        target_type: TargetType,
        standart: SQLStandart,
        pool: PgPool,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
    ) -> Self {
        Self {
            regression_model: None,
            classification_model: None,
            task_type,
            name: "DecisionTree".to_string(),
            target_type,
            symbol_columns: None,
            config: load_config(),
            prediction_tx,
            standart,
            pool,
            max_depth,
            min_samples_leaf,
            min_samples_split,
        }
    }
}

impl ModelDependencies for DecisionTree {
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
        match self.regression_model.as_ref() {
            Some(_) => true,
            None => self.classification_model.as_ref().is_some(),
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
impl Model for DecisionTree {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<Option<HashMap<String, f64>>> {
        match self.task_type {
            TaskType::Regression => {
                let params = DecisionTreeRegressorParameters::default()
                    .with_max_depth(self.max_depth)
                    .with_min_samples_leaf(self.min_samples_leaf)
                    .with_min_samples_split(self.min_samples_split);

                self.regression_model = Some(
                    DecisionTreeRegressor::fit(x_train, y_train, params)
                        .map_err(|e| anyhow!("Failed to fit DecisionTreeRegressor: {}", e))?,
                );
            }
            TaskType::Classification => {
                let params = DecisionTreeClassifierParameters::default()
                    .with_max_depth(self.max_depth)
                    .with_min_samples_leaf(self.min_samples_leaf)
                    .with_min_samples_split(self.min_samples_split);

                self.classification_model = Some(
                    DecisionTreeClassifier::fit(
                        x_train,
                        &y_train.iter().map(|v| *v as i32).collect(),
                        params,
                    )
                    .map_err(|e| anyhow!("Failed to fit DecisionTreeClassifier: {}", e))?,
                );
            }
        }

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            match self.evaluate(xv, yv) {
                Ok(result) => return Ok(Some(result)),
                Err(e) => error!("Failed to evaluate RandomForest model: {}", e),
            }
        }

        Ok(None)
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>> {
        let prediction = match self.task_type {
            TaskType::Regression => {
                let model = self
                    .regression_model
                    .as_ref()
                    .ok_or_else(|| anyhow!("DecisionTree regression model not trained yet!"))?;
                model
                    .predict(values)
                    .map_err(|e| anyhow!("Failed to predict with DecisionTreeRegressor: {}", e))?
            }
            TaskType::Classification => {
                let model = self
                    .classification_model
                    .as_ref()
                    .ok_or_else(|| anyhow!("DecisionTree classification model not trained yet!"))?;
                model
                    .predict(values)
                    .map_err(|e| anyhow!("Failed to predict with DecisionTreeClassifier: {}", e))?
                    .iter()
                    .map(|v| *v as f64)
                    .collect()
            }
        };
        Ok(prediction)
    }

    async fn handle_mistakes(&mut self, true_data: DataMap, predicted_data: DataMap) -> Result<()> {
        let true_data = true_data.to_vec();
        let predicted_data = predicted_data.to_vec();
        let correlation = corr(&true_data, &predicted_data);

        if correlation < self.config.behaviour.success_threshold {
            self.train()
                .await
                .map_err(|e| anyhow!("Failed to retrain DecisionTree model: {}", e))?;
        }

        Ok(())
    }
}
