use anyhow::anyhow;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::tree::decision_tree_regressor::{
    DecisionTreeRegressor, DecisionTreeRegressorParameters,
};
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

pub struct DecisionTree {
    model: Option<DecisionTreeRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
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
        target_type: TargetType,
        standart: SQLStandart,
        pool: PgPool,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
    ) -> Self {
        Self {
            model: None,
            name: "DecisionTree".to_string(),
            target_type,
            symbol_columns: None,
            config: load_config("config/config.yaml"),
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
impl Model for DecisionTree {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        let params = DecisionTreeRegressorParameters::default()
            .with_max_depth(self.max_depth)
            .with_min_samples_leaf(self.min_samples_leaf)
            .with_min_samples_split(self.min_samples_split);

        self.model = Some(DecisionTreeRegressor::fit(x_train, y_train, params)?);

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

    async fn handle_mistakes(
        &mut self,
        true_data: DataMap,
        predicted_data: DataMap,
    ) -> Result<(), anyhow::Error> {
        let true_data = true_data.to_vec();
        let predicted_data = predicted_data.to_vec();
        let correlation = corr(&true_data, &predicted_data);
        println!("Corr: {}", correlation);

        if correlation > self.config.behaviour.success_threshold {
            self.train().await?;
        }

        Ok(())
    }
}
