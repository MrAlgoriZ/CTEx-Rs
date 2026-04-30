use anyhow::anyhow;
use smartcore::ensemble::random_forest_classifier::{
    RandomForestClassifier, RandomForestClassifierParameters,
};
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use smartcore::linalg::basic::matrix::DenseMatrix;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::data::data_interfaces::DataMap;
use crate::data::process::features::auxiliary::corr;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::manager::PredictionsCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::TargetType;
use crate::models::TaskType;
use crate::models::model::{Model, ModelDependencies};

pub struct RandomForest {
    regression_model: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    classification_model: Option<RandomForestClassifier<f64, i32, DenseMatrix<f64>, Vec<i32>>>,
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

impl RandomForest {
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
            regression_model: None,
            classification_model: None,
            task_type,
            name: "RandomForest".to_string(),
            target_type,
            symbol_columns: None,
            config: load_config("config/config.yaml"),
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

impl ModelDependencies for RandomForest {
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
            Some(_) => return true,
            None => match self.classification_model.as_ref() {
                Some(_) => return true,
                None => return false,
            },
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
impl Model for RandomForest {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        match self.task_type {
            TaskType::Regression => {
                let params = RandomForestRegressorParameters::default()
                    .with_n_trees(self.n_trees)
                    .with_max_depth(self.max_depth)
                    .with_seed(self.get_config().model.seed)
                    .with_min_samples_leaf(self.min_samples_leaf)
                    .with_min_samples_split(self.min_samples_split)
                    .with_m(self.m);

                self.regression_model = Some(
                    RandomForestRegressor::fit(x_train, y_train, params)
                        .map_err(|e| anyhow!("Failed to fit RandomForestRegressor: {}", e))?,
                );
            }
            TaskType::Classification => {
                let params = RandomForestClassifierParameters::default()
                    .with_n_trees(self.n_trees as u16)
                    .with_max_depth(self.max_depth)
                    .with_seed(self.get_config().model.seed)
                    .with_min_samples_leaf(self.min_samples_leaf)
                    .with_min_samples_split(self.min_samples_split)
                    .with_m(self.m);

                self.classification_model = Some(
                    RandomForestClassifier::fit(
                        x_train,
                        &y_train.iter().map(|v| *v as i32).collect(),
                        params,
                    )
                    .map_err(|e| anyhow!("Failed to fit RandomForestClassifier: {}", e))?,
                );
            }
        }

        if let (Some(xv), Some(yv)) = (x_val, y_val) {
            match self.evaluate(xv, yv) {
                Ok(_) => {}
                Err(e) => eprintln!("Failed to evaluate RandomForest model: {}", e),
            }
        }

        Ok(())
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        let prediction = match self.task_type {
            TaskType::Regression => {
                let model = self
                    .regression_model
                    .as_ref()
                    .ok_or_else(|| anyhow!("RandomForest regression model not trained yet!"))?;
                model
                    .predict(values)
                    .map_err(|e| anyhow!("Failed to predict with RandomForestRegressor: {}", e))?
            }
            TaskType::Classification => {
                let model = self
                    .classification_model
                    .as_ref()
                    .ok_or_else(|| anyhow!("RandomForest classification model not trained yet!"))?;
                model
                    .predict(values)
                    .map_err(|e| anyhow!("Failed to predict with RandomForestClassifier: {}", e))?
                    .iter()
                    .map(|v| *v as f64)
                    .collect()
            }
        };
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
                .map_err(|e| anyhow!("Failed to retrain RandomForest model: {}", e))?;
        }

        Ok(())
    }
}
