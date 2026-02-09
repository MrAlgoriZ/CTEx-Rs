use anyhow::anyhow;
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use smartcore::linalg::basic::matrix::DenseMatrix;
use tokio::sync::mpsc;

use crate::engine::cycles::manager::PredictionCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::model::{Model, ModelDependencies};

pub struct RandomForest {
    model: Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    name: String,
    symbol_columns: Option<Vec<String>>,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionCommand>>,
    n_trees: usize,
    max_depth: u16,
}

impl RandomForest {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionCommand>>,
        n_trees: usize,
        max_depth: u16,
    ) -> Self {
        Self {
            model: None,
            name: "RandomForest".to_string(),
            symbol_columns: None,
            config: load_config("config/config.yaml"),
            prediction_tx,
            n_trees,
            max_depth,
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

    fn get_prediction_tx(&self) -> &Option<mpsc::Sender<PredictionCommand>> {
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
}

impl Model for RandomForest {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        let params = RandomForestRegressorParameters::default()
            .with_n_trees(self.n_trees)
            .with_max_depth(self.max_depth)
            .with_seed(self.get_config().model.seed);

        self.model = Some(RandomForestRegressor::fit(x_train, y_train, params)?);

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
