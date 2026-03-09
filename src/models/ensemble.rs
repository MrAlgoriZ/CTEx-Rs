use anyhow::anyhow;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::CONFIG_PATH;
use crate::data::requests::database::consts::SQLStandart;
use crate::engine::cycles::manager::{ModelActor, ModelCommand, PredictionsCommand};
use crate::engine::state::counters::Counters;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::SingleModelParams;
use crate::models::model::{Model, ModelDependencies, init_single_model};
use smartcore::linalg::basic::matrix::DenseMatrix;

pub struct Ensemble {
    future_volatility_model_tx: mpsc::Sender<ModelCommand>,
    future_volume_model_tx: mpsc::Sender<ModelCommand>,
    future_trend_strength_model_tx: mpsc::Sender<ModelCommand>,
    future_range_model_tx: mpsc::Sender<ModelCommand>,
    future_return_mean_model_tx: mpsc::Sender<ModelCommand>,
    future_return_std_model_tx: mpsc::Sender<ModelCommand>,
    future_return_skew_model_tx: mpsc::Sender<ModelCommand>,
    future_return_kurt_model_tx: mpsc::Sender<ModelCommand>,
    risk_score_model_tx: mpsc::Sender<ModelCommand>,
    drawdown_probability_model_tx: mpsc::Sender<ModelCommand>,
    tail_event_probability_model_tx: mpsc::Sender<ModelCommand>,
    liquidity_drop_probability_model_tx: mpsc::Sender<ModelCommand>,
    future_return_model_tx: mpsc::Sender<ModelCommand>,
    action_type_model_tx: mpsc::Sender<ModelCommand>,
    position_size_model_tx: mpsc::Sender<ModelCommand>,

    counters: Counters,
    name: String,
    config: Config,
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
}

impl Ensemble {
    pub fn new(
        future_volatility_model_tx: mpsc::Sender<ModelCommand>,
        future_volume_model_tx: mpsc::Sender<ModelCommand>,
        future_trend_strength_model_tx: mpsc::Sender<ModelCommand>,
        future_range_model_tx: mpsc::Sender<ModelCommand>,
        future_return_mean_model_tx: mpsc::Sender<ModelCommand>,
        future_return_std_model_tx: mpsc::Sender<ModelCommand>,
        future_return_skew_model_tx: mpsc::Sender<ModelCommand>,
        future_return_kurt_model_tx: mpsc::Sender<ModelCommand>,
        risk_score_model_tx: mpsc::Sender<ModelCommand>,
        drawdown_probability_model_tx: mpsc::Sender<ModelCommand>,
        tail_event_probability_model_tx: mpsc::Sender<ModelCommand>,
        liquidity_drop_probability_model_tx: mpsc::Sender<ModelCommand>,
        future_return_model_tx: mpsc::Sender<ModelCommand>,
        action_type_model_tx: mpsc::Sender<ModelCommand>,
        position_size_model_tx: mpsc::Sender<ModelCommand>,
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        config: Config,
    ) -> Self {
        Self {
            future_volatility_model_tx,
            future_volume_model_tx,
            future_trend_strength_model_tx,
            future_range_model_tx,
            future_return_mean_model_tx,
            future_return_std_model_tx,
            future_return_skew_model_tx,
            future_return_kurt_model_tx,
            risk_score_model_tx,
            drawdown_probability_model_tx,
            tail_event_probability_model_tx,
            liquidity_drop_probability_model_tx,
            future_return_model_tx,
            action_type_model_tx,
            position_size_model_tx,
            name: "Ensemble".to_string(),
            config: config.clone(),
            prediction_tx,
            counters: Counters::new(config.behaviour.accuracy_capacity),
        }
    }

    pub fn init(
        prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
        pool: PgPool,
        future_volatility_model_params: SingleModelParams,
        future_volume_model_params: SingleModelParams,
        future_trend_strength_model_params: SingleModelParams,
        future_range_model_params: SingleModelParams,
        future_return_mean_model_params: SingleModelParams,
        future_return_std_model_params: SingleModelParams,
        future_return_skew_model_params: SingleModelParams,
        future_return_kurt_model_params: SingleModelParams,
        risk_score_model_params: SingleModelParams,
        drawdown_probability_model_params: SingleModelParams,
        tail_event_probability_model_params: SingleModelParams,
        liquidity_drop_probability_model_params: SingleModelParams,
        future_return_model_params: SingleModelParams,
        action_type_model_params: SingleModelParams,
        position_size_model_params: SingleModelParams,
    ) -> Self {
        let config = load_config(CONFIG_PATH);

        let future_volatility_model = init_single_model(future_volatility_model_params, None);
        let (future_volatility_model_actor, future_volatility_model_tx) =
            ModelActor::new(future_volatility_model, pool.clone());
        tokio::spawn(future_volatility_model_actor.run());

        let future_volume_model = init_single_model(future_volume_model_params, None);
        let (future_volume_model_actor, future_volume_model_tx) =
            ModelActor::new(future_volume_model, pool.clone());
        tokio::spawn(future_volume_model_actor.run());

        let future_trend_strength_model =
            init_single_model(future_trend_strength_model_params, None);
        let (future_trend_strength_model_actor, future_trend_strength_model_tx) =
            ModelActor::new(future_trend_strength_model, pool.clone());
        tokio::spawn(future_trend_strength_model_actor.run());

        let future_range_model = init_single_model(future_range_model_params, None);
        let (future_range_model_actor, future_range_model_tx) =
            ModelActor::new(future_range_model, pool.clone());
        tokio::spawn(future_range_model_actor.run());

        let future_return_mean_model = init_single_model(future_return_mean_model_params, None);
        let (future_return_mean_model_actor, future_return_mean_model_tx) =
            ModelActor::new(future_return_mean_model, pool.clone());
        tokio::spawn(future_return_mean_model_actor.run());

        let future_return_std_model = init_single_model(future_return_std_model_params, None);
        let (future_return_std_model_actor, future_return_std_model_tx) =
            ModelActor::new(future_return_std_model, pool.clone());
        tokio::spawn(future_return_std_model_actor.run());

        let future_return_skew_model = init_single_model(future_return_skew_model_params, None);
        let (future_return_skew_model_actor, future_return_skew_model_tx) =
            ModelActor::new(future_return_skew_model, pool.clone());
        tokio::spawn(future_return_skew_model_actor.run());

        let future_return_kurt_model = init_single_model(future_return_kurt_model_params, None);
        let (future_return_kurt_model_actor, future_return_kurt_model_tx) =
            ModelActor::new(future_return_kurt_model, pool.clone());
        tokio::spawn(future_return_kurt_model_actor.run());

        let risk_score_model = init_single_model(risk_score_model_params, None);
        let (risk_score_model_actor, risk_score_model_tx) =
            ModelActor::new(risk_score_model, pool.clone());
        tokio::spawn(risk_score_model_actor.run());

        let drawdown_probability_model = init_single_model(drawdown_probability_model_params, None);
        let (drawdown_probability_model_actor, drawdown_probability_model_tx) =
            ModelActor::new(drawdown_probability_model, pool.clone());
        tokio::spawn(drawdown_probability_model_actor.run());

        let tail_event_probability_model =
            init_single_model(tail_event_probability_model_params, None);
        let (tail_event_probability_model_actor, tail_event_probability_model_tx) =
            ModelActor::new(tail_event_probability_model, pool.clone());
        tokio::spawn(tail_event_probability_model_actor.run());

        let liquidity_drop_probability_model =
            init_single_model(liquidity_drop_probability_model_params, None);
        let (liquidity_drop_probability_model_actor, liquidity_drop_probability_model_tx) =
            ModelActor::new(liquidity_drop_probability_model, pool.clone());
        tokio::spawn(liquidity_drop_probability_model_actor.run());

        let future_return_model = init_single_model(future_return_model_params, None);
        let (future_return_model_actor, future_return_model_tx) =
            ModelActor::new(future_return_model, pool.clone());
        tokio::spawn(future_return_model_actor.run());

        let action_type_model = init_single_model(action_type_model_params, None);
        let (action_type_model_actor, action_type_model_tx) =
            ModelActor::new(action_type_model, pool.clone());
        tokio::spawn(action_type_model_actor.run());

        let position_size_model = init_single_model(position_size_model_params, None);
        let (position_size_model_actor, position_size_model_tx) =
            ModelActor::new(position_size_model, pool.clone());
        tokio::spawn(position_size_model_actor.run());

        Self::new(
            future_volatility_model_tx,
            future_volume_model_tx,
            future_trend_strength_model_tx,
            future_range_model_tx,
            future_return_mean_model_tx,
            future_return_std_model_tx,
            future_return_skew_model_tx,
            future_return_kurt_model_tx,
            risk_score_model_tx,
            drawdown_probability_model_tx,
            tail_event_probability_model_tx,
            liquidity_drop_probability_model_tx,
            future_return_model_tx,
            action_type_model_tx,
            position_size_model_tx,
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
    fn get_prediction_tx(&self) -> &Option<mpsc::Sender<PredictionsCommand>> {
        &self.prediction_tx
    }
    fn get_symbol_columns(&self) -> &Option<Vec<String>> {
        &None
    }
    fn get_target_name(&self) -> &str {
        "position_size"
    }
    fn get_standart(&self) -> &SQLStandart {
        &SQLStandart::ThirdLayer
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
        todo!()
    }
    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        todo!()
    }
    fn normalize(
        &self,
        x_train: DenseMatrix<f64>,
        x_val: DenseMatrix<f64>,
    ) -> (DenseMatrix<f64>, DenseMatrix<f64>) {
        (x_train, x_val)
    }
}
