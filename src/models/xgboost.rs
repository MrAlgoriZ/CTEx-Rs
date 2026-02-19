use anyhow::anyhow;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::xgboost::{XGRegressor, XGRegressorParameters};
use tokio::sync::mpsc;

use crate::engine::cycles::manager::PredictionCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::TargetType;
use crate::models::model::{Model, ModelDependencies};

pub struct XGBoost {
    model: Option<XGRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>,
    name: String,
    target_type: TargetType,
    symbol_columns: Option<Vec<String>>,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionCommand>>,
    n_estimators: usize,
    max_depth: u16,
}

impl XGBoost {
    pub fn new(
        prediction_tx: Option<mpsc::Sender<PredictionCommand>>,
        target_type: TargetType,
        n_estimators: usize,
        max_depth: u16,
    ) -> Self {
        Self {
            model: None,
            name: "XGBoost".to_string(),
            target_type,
            symbol_columns: None,
            config: load_config("config/config.yaml"),
            prediction_tx,
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

    fn get_target_index(&self) -> i32 {
        self.target_type.get_index()
    }
}

impl Model for XGBoost {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        let params = XGRegressorParameters::default()
            .with_n_estimators(self.n_estimators)
            .with_max_depth(self.max_depth)
            .with_seed(self.get_config().model.seed);

        self.model = Some(XGRegressor::fit(x_train, y_train, params)?);

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
