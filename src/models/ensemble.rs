use anyhow::anyhow;
use tokio::sync::mpsc;

use crate::CONFIG_PATH;
use crate::engine::cycles::manager::{ModelActor, ModelCommand, PredictionCommand};
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::SingleModelParams;
use crate::models::model::{Model, ModelDependencies, init_single_model};
use smartcore::linalg::basic::matrix::DenseMatrix;

pub struct Ensemble {
    volatility_model_tx: mpsc::Sender<ModelCommand>,
    volume_model_tx: mpsc::Sender<ModelCommand>,
    spread_model_tx: mpsc::Sender<ModelCommand>,
    trend_strength_model_tx: mpsc::Sender<ModelCommand>,
    range_model_tx: mpsc::Sender<ModelCommand>,
    return_model_tx: mpsc::Sender<ModelCommand>,
    return_mean_model_tx: mpsc::Sender<ModelCommand>,
    return_std_model_tx: mpsc::Sender<ModelCommand>,
    return_skew_model_tx: mpsc::Sender<ModelCommand>,
    return_kurt_model_tx: mpsc::Sender<ModelCommand>,
    action_model_tx: mpsc::Sender<ModelCommand>,
    interpretator_model_tx: mpsc::Sender<ModelCommand>,

    name: String,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionCommand>>,

    symbol_columns: Option<Vec<String>>,
}

impl Ensemble {
    pub fn new(
        volatility_model_tx: mpsc::Sender<ModelCommand>,
        volume_model_tx: mpsc::Sender<ModelCommand>,
        spread_model_tx: mpsc::Sender<ModelCommand>,
        trend_strength_model_tx: mpsc::Sender<ModelCommand>,
        range_model_tx: mpsc::Sender<ModelCommand>,
        return_model_tx: mpsc::Sender<ModelCommand>,
        return_mean_model_tx: mpsc::Sender<ModelCommand>,
        return_std_model_tx: mpsc::Sender<ModelCommand>,
        return_skew_model_tx: mpsc::Sender<ModelCommand>,
        return_kurt_model_tx: mpsc::Sender<ModelCommand>,
        action_model_tx: mpsc::Sender<ModelCommand>,
        interpretator_model_tx: mpsc::Sender<ModelCommand>,
        prediction_tx: Option<mpsc::Sender<PredictionCommand>>,
        config: Config,
    ) -> Self {
        Self {
            volatility_model_tx,
            volume_model_tx,
            spread_model_tx,
            trend_strength_model_tx,
            range_model_tx,
            return_model_tx,
            return_mean_model_tx,
            return_std_model_tx,
            return_skew_model_tx,
            return_kurt_model_tx,
            action_model_tx,
            interpretator_model_tx,
            name: "Ensemble".to_string(),
            config,
            prediction_tx,
            symbol_columns: None,
        }
    }

    pub fn init(
        prediction_tx: Option<mpsc::Sender<PredictionCommand>>,
        volatility_model_params: SingleModelParams,
        volume_model_params: SingleModelParams,
        spread_model_params: SingleModelParams,
        trend_strength_model_params: SingleModelParams,
        range_model_params: SingleModelParams,
        return_model_params: SingleModelParams,
        return_mean_model_params: SingleModelParams,
        return_std_model_params: SingleModelParams,
        return_skew_model_params: SingleModelParams,
        return_kurt_model_params: SingleModelParams,
        action_model_params: SingleModelParams,
        interpretator_model_params: SingleModelParams,
    ) -> Self {
        let config = load_config(CONFIG_PATH);

        let volatility_model = init_single_model(volatility_model_params, None);
        let (volatility_model_actor, volatility_model_tx) = ModelActor::new(volatility_model);
        tokio::spawn(volatility_model_actor.run());

        let volume_model = init_single_model(volume_model_params, None);
        let (volume_model_actor, volume_model_tx) = ModelActor::new(volume_model);
        tokio::spawn(volume_model_actor.run());

        let spread_model = init_single_model(spread_model_params, None);
        let (spread_model_actor, spread_model_tx) = ModelActor::new(spread_model);
        tokio::spawn(spread_model_actor.run());

        let trend_strength_model = init_single_model(trend_strength_model_params, None);
        let (trend_strength_model_actor, trend_strength_model_tx) =
            ModelActor::new(trend_strength_model);
        tokio::spawn(trend_strength_model_actor.run());

        let range_model = init_single_model(range_model_params, None);
        let (range_model_actor, range_model_tx) = ModelActor::new(range_model);
        tokio::spawn(range_model_actor.run());

        let return_model = init_single_model(return_model_params, None);
        let (return_model_actor, return_model_tx) = ModelActor::new(return_model);
        tokio::spawn(return_model_actor.run());

        let return_mean_model = init_single_model(return_mean_model_params, None);
        let (return_mean_model_actor, return_mean_model_tx) = ModelActor::new(return_mean_model);
        tokio::spawn(return_mean_model_actor.run());

        let return_std_model = init_single_model(return_std_model_params, None);
        let (return_std_model_actor, return_std_model_tx) = ModelActor::new(return_std_model);
        tokio::spawn(return_std_model_actor.run());

        let return_skew_model = init_single_model(return_skew_model_params, None);
        let (return_skew_model_actor, return_skew_model_tx) = ModelActor::new(return_skew_model);
        tokio::spawn(return_skew_model_actor.run());

        let return_kurt_model = init_single_model(return_kurt_model_params, None);
        let (return_kurt_model_actor, return_kurt_model_tx) = ModelActor::new(return_kurt_model);
        tokio::spawn(return_kurt_model_actor.run());

        let action_model = init_single_model(action_model_params, None);
        let (action_model_actor, action_model_tx) = ModelActor::new(action_model);
        tokio::spawn(action_model_actor.run());

        let interpretator_model = init_single_model(interpretator_model_params, None);
        let (interpretator_model_actor, interpretator_model_tx) =
            ModelActor::new(interpretator_model);
        tokio::spawn(interpretator_model_actor.run());

        Self::new(
            volatility_model_tx,
            volume_model_tx,
            spread_model_tx,
            trend_strength_model_tx,
            range_model_tx,
            return_model_tx,
            return_mean_model_tx,
            return_std_model_tx,
            return_skew_model_tx,
            return_kurt_model_tx,
            action_model_tx,
            interpretator_model_tx,
            prediction_tx,
            config,
        )
    }
}

impl ModelDependencies for Ensemble {
    fn change_symbol_columns(&mut self, _: Option<Vec<String>>) {}
    fn check_model_trained(&self) -> bool {
        true
    }
    fn get_config(&self) -> &Config {
        &self.config
    }
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_prediction_tx(&self) -> &Option<mpsc::Sender<PredictionCommand>> {
        &self.prediction_tx
    }
    fn get_symbol_columns(&self) -> &Option<Vec<String>> {
        &None
    }
    fn get_target_index(&self) -> i32 {
        i32::MAX
    }
}

impl Model for Ensemble {
    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        Err(anyhow!("Not implemented!"))
    }
    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        Err(anyhow!("Not implemented!"))
    }
    fn normalize(
        &self,
        x_train: DenseMatrix<f64>,
        x_val: DenseMatrix<f64>,
    ) -> (DenseMatrix<f64>, DenseMatrix<f64>) {
        (x_train, x_val)
    }
}
