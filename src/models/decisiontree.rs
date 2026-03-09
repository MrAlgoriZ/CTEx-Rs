use anyhow::anyhow;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::tree::decision_tree_regressor::{
    DecisionTreeRegressor, DecisionTreeRegressorParameters,
};
use tokio::sync::mpsc;

use crate::data::requests::database::consts::SQLStandart;
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
    max_depth: u16,
    min_samples_leaf: usize,
    min_samples_split: usize,
}

impl DecisionTree {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        target_type: TargetType,
        standart: SQLStandart,
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
}

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
}
