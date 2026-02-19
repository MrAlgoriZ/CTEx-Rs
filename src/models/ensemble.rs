use anyhow::anyhow;
use smartcore::linalg::basic::matrix::DenseMatrix;
use tokio::sync::mpsc;

use crate::engine::cycles::manager::PredictionCommand;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::ModelParams;
use crate::models::model::{Model, ModelDependencies, init_single_model};

struct SubModelPrediction {
    name: &'static str,
    value: f64,
}

pub struct Ensemble {
    volatility_model: Option<Box<dyn Model + Send + Sync>>,
    volume_model: Option<Box<dyn Model + Send + Sync>>,
    spread_model: Option<Box<dyn Model + Send + Sync>>,
    trend_strength_model: Option<Box<dyn Model + Send + Sync>>,
    range_model: Option<Box<dyn Model + Send + Sync>>,
    return_model: Option<Box<dyn Model + Send + Sync>>,
    return_mean_model: Option<Box<dyn Model + Send + Sync>>,
    return_std_model: Option<Box<dyn Model + Send + Sync>>,
    return_skew_model: Option<Box<dyn Model + Send + Sync>>,
    return_kurt_model: Option<Box<dyn Model + Send + Sync>>,
    action_model: Option<Box<dyn Model + Send + Sync>>,

    name: String,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionCommand>>,

    symbol_columns: Option<Vec<String>>,
}

impl Ensemble {
    pub fn new(prediction_tx: Option<mpsc::Sender<PredictionCommand>>) -> Self {
        Self {
            volatility_model: None,
            volume_model: None,
            spread_model: None,
            trend_strength_model: None,
            range_model: None,
            return_model: None,
            return_mean_model: None,
            return_std_model: None,
            return_skew_model: None,
            return_kurt_model: None,
            action_model: None,
            name: "Ensemble".to_string(),
            config: load_config("config/config.yaml"),
            prediction_tx,
            symbol_columns: None,
        }
    }

    pub fn init_from_config(&mut self) -> Result<(), anyhow::Error> {
        let params = self.config.model.params.clone();

        match params {
            ModelParams::Ensemble {
                volatility_model_params,
                volume_model_params,
                spread_model_params,
                trend_strength_model_params,
                range_model_params,
                return_model_params,
                return_mean_model_params,
                return_std_model_params,
                return_skew_model_params,
                return_kurt_model_params,
                action_model_params,
            } => {
                let ptx = self.prediction_tx.clone();

                self.volatility_model = Some(init_single_model(
                    volatility_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.volume_model = Some(init_single_model(
                    volume_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.spread_model = Some(init_single_model(
                    spread_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.trend_strength_model = Some(init_single_model(
                    trend_strength_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.range_model = Some(init_single_model(
                    range_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.return_model = Some(init_single_model(
                    return_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.return_mean_model = Some(init_single_model(
                    return_mean_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.return_std_model = Some(init_single_model(
                    return_std_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.return_skew_model = Some(init_single_model(
                    return_skew_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.return_kurt_model = Some(init_single_model(
                    return_kurt_model_params.get_params().clone(),
                    ptx.clone(),
                ));
                self.action_model = Some(init_single_model(
                    action_model_params.get_params().clone(),
                    ptx.clone(),
                ));

                Ok(())
            }
            ModelParams::Single { .. } => Err(anyhow!(
                "Ensemble requires ModelParams::Ensemble in config, got Single"
            )),
        }
    }

    fn sub_models_ref(&self) -> Vec<(&'static str, Option<&(dyn Model + Send + Sync)>)> {
        vec![
            ("volatility", self.volatility_model.as_deref()),
            ("volume", self.volume_model.as_deref()),
            ("spread", self.spread_model.as_deref()),
            ("trend_strength", self.trend_strength_model.as_deref()),
            ("range", self.range_model.as_deref()),
            ("return", self.return_model.as_deref()),
            ("return_mean", self.return_mean_model.as_deref()),
            ("return_std", self.return_std_model.as_deref()),
            ("return_skew", self.return_skew_model.as_deref()),
            ("return_kurt", self.return_kurt_model.as_deref()),
            ("action", self.action_model.as_deref()),
        ]
    }

    fn aggregate_predictions(
        &self,
        sub_predictions: Vec<SubModelPrediction>,
    ) -> Result<Vec<f64>, anyhow::Error> {
        if let Some(action_pred) = sub_predictions.iter().find(|p| p.name == "action") {
            return Ok(vec![action_pred.value]);
        }

        if sub_predictions.is_empty() {
            return Err(anyhow!("No sub-model predictions available"));
        }
        let mean =
            sub_predictions.iter().map(|p| p.value).sum::<f64>() / sub_predictions.len() as f64;
        Ok(vec![mean])
    }
}

impl ModelDependencies for Ensemble {
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
        self.sub_models_ref()
            .iter()
            .all(|(_, m)| m.map(|m| m.check_model_trained()).unwrap_or(false))
    }

    fn get_symbol_columns(&self) -> &Option<Vec<String>> {
        if let Some(ref action) = self.action_model {
            return action.get_symbol_columns();
        }
        &self.symbol_columns
    }

    fn get_target_index(&self) -> i32 {
        0
    }
}

impl Model for Ensemble {
    fn model_fit(
        &mut self,
        _x_train: &DenseMatrix<f64>,
        _y_train: &Vec<f64>,
        _x_val: Option<&DenseMatrix<f64>>,
        _y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        Err(anyhow!(
            "Ensemble.model_fit() should not be called directly. Use train() instead."
        ))
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        if !self.check_model_trained() {
            return Err(anyhow!("Ensemble is not fully trained"));
        }

        let sub_preds: Result<Vec<SubModelPrediction>, anyhow::Error> = self
            .sub_models_ref()
            .into_iter()
            .filter_map(|(name, model_opt)| model_opt.map(|m| (name, m)))
            .map(|(name, model)| {
                let pred = model.model_predict(values)?;
                let value = pred
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("Sub-model '{}' returned empty prediction", name))?;
                Ok(SubModelPrediction { name, value })
            })
            .collect();

        self.aggregate_predictions(sub_preds?)
    }

    fn evaluate(&self, _x_val: &DenseMatrix<f64>, _y_val: &Vec<f64>) -> Result<f64, anyhow::Error> {
        if !self.check_model_trained() {
            return Err(anyhow!("Ensemble is not fully trained"));
        }

        println!("[Ensemble] All sub-models evaluated individually during training.");

        Ok(0.0)
    }

    fn train(
        &mut self,
        data: Vec<crate::data::data_interfaces::FlattenedData>,
    ) -> Result<(), anyhow::Error> {
        if self.volatility_model.is_none() {
            self.init_from_config()?;
        }

        if data.is_empty() {
            return Err(anyhow!("No data provided for Ensemble training"));
        }

        println!("[Ensemble] Starting training of {} sub-models...", 11);

        macro_rules! train_sub_model {
            ($model_field:expr, $name:expr) => {
                if let Some(ref mut model) = $model_field {
                    println!("[Ensemble] Training sub-model: {}", $name);
                    model
                        .train(data.clone())
                        .map_err(|e| anyhow!("Sub-model '{}' training failed: {}", $name, e))?;
                    println!("[Ensemble] Sub-model '{}' trained successfully.", $name);
                } else {
                    return Err(anyhow!(
                        "Sub-model '{}' not initialized. Call init_from_config() first.",
                        $name
                    ));
                }
            };
        }

        train_sub_model!(self.volatility_model, "volatility");
        train_sub_model!(self.volume_model, "volume");
        train_sub_model!(self.spread_model, "spread");
        train_sub_model!(self.trend_strength_model, "trend_strength");
        train_sub_model!(self.range_model, "range");
        train_sub_model!(self.return_model, "return");
        train_sub_model!(self.return_mean_model, "return_mean");
        train_sub_model!(self.return_std_model, "return_std");
        train_sub_model!(self.return_skew_model, "return_skew");
        train_sub_model!(self.return_kurt_model, "return_kurt");
        train_sub_model!(self.action_model, "action");

        if let Some(ref action) = self.action_model {
            self.symbol_columns = action.get_symbol_columns().clone();
        }

        println!("[Ensemble] All sub-models trained successfully.");
        Ok(())
    }
}
