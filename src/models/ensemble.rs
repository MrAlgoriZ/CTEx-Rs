use anyhow::anyhow;
use log::info;
use smartcore::linalg::basic::matrix::DenseMatrix;
use sqlx::PgPool;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};

use crate::CONFIG_PATH;
use crate::data::data_interfaces::DataMap;
use crate::data::requests::database::standart::{
    SQLStandart, get_confidence_name, get_prediction_name,
};
use crate::engine::cycles::manager::{ModelActor, ModelCommand, PredictionsCommand};
use crate::engine::state::counters::Counters;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::models::SingleModelParams;
use crate::models::model::{Model, ModelDependencies, init_single_model};

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
    volatility_spike_probability_model_tx: mpsc::Sender<ModelCommand>,
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
        volatility_spike_probability_model_tx: mpsc::Sender<ModelCommand>,
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
            volatility_spike_probability_model_tx,
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
        volatility_spike_probability_model_params: SingleModelParams,
        liquidity_drop_probability_model_params: SingleModelParams,
        future_return_model_params: SingleModelParams,
        action_type_model_params: SingleModelParams,
        position_size_model_params: SingleModelParams,
    ) -> Self {
        let config = load_config(CONFIG_PATH);

        let future_volatility_model = init_single_model(
            future_volatility_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_volatility_model_actor, future_volatility_model_tx) =
            ModelActor::new(future_volatility_model);
        tokio::spawn(future_volatility_model_actor.run());

        let future_volume_model = init_single_model(
            future_volume_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_volume_model_actor, future_volume_model_tx) =
            ModelActor::new(future_volume_model);
        tokio::spawn(future_volume_model_actor.run());

        let future_trend_strength_model = init_single_model(
            future_trend_strength_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_trend_strength_model_actor, future_trend_strength_model_tx) =
            ModelActor::new(future_trend_strength_model);
        tokio::spawn(future_trend_strength_model_actor.run());

        let future_range_model = init_single_model(
            future_range_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_range_model_actor, future_range_model_tx) = ModelActor::new(future_range_model);
        tokio::spawn(future_range_model_actor.run());

        let future_return_mean_model = init_single_model(
            future_return_mean_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_return_mean_model_actor, future_return_mean_model_tx) =
            ModelActor::new(future_return_mean_model);
        tokio::spawn(future_return_mean_model_actor.run());

        let future_return_std_model = init_single_model(
            future_return_std_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_return_std_model_actor, future_return_std_model_tx) =
            ModelActor::new(future_return_std_model);
        tokio::spawn(future_return_std_model_actor.run());

        let future_return_skew_model = init_single_model(
            future_return_skew_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_return_skew_model_actor, future_return_skew_model_tx) =
            ModelActor::new(future_return_skew_model);
        tokio::spawn(future_return_skew_model_actor.run());

        let future_return_kurt_model = init_single_model(
            future_return_kurt_model_params,
            None,
            SQLStandart::FirstLayer,
            pool.clone(),
        );
        let (future_return_kurt_model_actor, future_return_kurt_model_tx) =
            ModelActor::new(future_return_kurt_model);
        tokio::spawn(future_return_kurt_model_actor.run());

        let risk_score_model = init_single_model(
            risk_score_model_params,
            None,
            SQLStandart::SecondLayer,
            pool.clone(),
        );
        let (risk_score_model_actor, risk_score_model_tx) = ModelActor::new(risk_score_model);
        tokio::spawn(risk_score_model_actor.run());

        let drawdown_probability_model = init_single_model(
            drawdown_probability_model_params,
            None,
            SQLStandart::SecondLayer,
            pool.clone(),
        );
        let (drawdown_probability_model_actor, drawdown_probability_model_tx) =
            ModelActor::new(drawdown_probability_model);
        tokio::spawn(drawdown_probability_model_actor.run());

        let tail_event_probability_model = init_single_model(
            tail_event_probability_model_params,
            None,
            SQLStandart::SecondLayer,
            pool.clone(),
        );
        let (tail_event_probability_model_actor, tail_event_probability_model_tx) =
            ModelActor::new(tail_event_probability_model);
        tokio::spawn(tail_event_probability_model_actor.run());

        let volatility_spike_probability_model = init_single_model(
            volatility_spike_probability_model_params,
            None,
            SQLStandart::SecondLayer,
            pool.clone(),
        );
        let (volatility_spike_probability_model_actor, volatility_spike_probability_model_tx) =
            ModelActor::new(volatility_spike_probability_model);
        tokio::spawn(volatility_spike_probability_model_actor.run());

        let liquidity_drop_probability_model = init_single_model(
            liquidity_drop_probability_model_params,
            None,
            SQLStandart::SecondLayer,
            pool.clone(),
        );
        let (liquidity_drop_probability_model_actor, liquidity_drop_probability_model_tx) =
            ModelActor::new(liquidity_drop_probability_model);
        tokio::spawn(liquidity_drop_probability_model_actor.run());

        let future_return_model = init_single_model(
            future_return_model_params,
            None,
            SQLStandart::ThirdLayer,
            pool.clone(),
        );
        let (future_return_model_actor, future_return_model_tx) =
            ModelActor::new(future_return_model);
        tokio::spawn(future_return_model_actor.run());

        let action_type_model = init_single_model(
            action_type_model_params,
            None,
            SQLStandart::ThirdLayer,
            pool.clone(),
        );

        let (action_type_model_actor, action_type_model_tx) = ModelActor::new(action_type_model);
        tokio::spawn(action_type_model_actor.run());

        let position_size_model = init_single_model(
            position_size_model_params,
            None,
            SQLStandart::ThirdLayer,
            pool.clone(),
        );
        let (position_size_model_actor, position_size_model_tx) =
            ModelActor::new(position_size_model);
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
            volatility_spike_probability_model_tx,
            liquidity_drop_probability_model_tx,
            future_return_model_tx,
            action_type_model_tx,
            position_size_model_tx,
            prediction_tx,
            config,
        )
    }

    fn get_model_by_name(&self, name: &str) -> Option<Sender<ModelCommand>> {
        match name {
            "future_volatility" => Some(self.future_volatility_model_tx.clone()),
            "future_volume" => Some(self.future_volume_model_tx.clone()),
            "future_trend_strength" => Some(self.future_trend_strength_model_tx.clone()),
            "future_range" => Some(self.future_range_model_tx.clone()),
            "future_return_mean" => Some(self.future_return_mean_model_tx.clone()),
            "future_return_std" => Some(self.future_return_std_model_tx.clone()),
            "future_return_skewness" => Some(self.future_return_skew_model_tx.clone()),
            "future_return_kurtosis" => Some(self.future_return_kurt_model_tx.clone()),
            "risk_score" => Some(self.risk_score_model_tx.clone()),
            "drawdown_probability" => Some(self.drawdown_probability_model_tx.clone()),
            "tail_event_probability" => Some(self.tail_event_probability_model_tx.clone()),
            "volatility_spike_probability" => {
                Some(self.volatility_spike_probability_model_tx.clone())
            }
            "liquidity_drop_probability" => Some(self.liquidity_drop_probability_model_tx.clone()),
            "future_return" => Some(self.future_return_model_tx.clone()),
            "action_type" => Some(self.action_type_model_tx.clone()),
            "position_size" => Some(self.position_size_model_tx.clone()),
            _ => None,
        }
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
        &SQLStandart::Dummy
    }
    fn get_pool(&self) -> Option<&PgPool> {
        None
    }
}

#[async_trait::async_trait]
impl Model for Ensemble {
    fn model_fit(
        &mut self,
        _: &DenseMatrix<f64>,
        _: &Vec<f64>,
        _: Option<&DenseMatrix<f64>>,
        _: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error> {
        Err(anyhow!("not implemented!"))
    }

    fn model_predict(&self, _: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error> {
        Err(anyhow!("not implemented!"))
    }

    async fn train(&mut self) -> Result<(), anyhow::Error> {
        let txs = [
            self.future_volatility_model_tx.clone(),
            self.future_volume_model_tx.clone(),
            self.future_trend_strength_model_tx.clone(),
            self.future_range_model_tx.clone(),
            self.future_return_mean_model_tx.clone(),
            self.future_return_std_model_tx.clone(),
            self.future_return_skew_model_tx.clone(),
            self.future_return_kurt_model_tx.clone(),
            self.risk_score_model_tx.clone(),
            self.drawdown_probability_model_tx.clone(),
            self.tail_event_probability_model_tx.clone(),
            self.volatility_spike_probability_model_tx.clone(),
            self.liquidity_drop_probability_model_tx.clone(),
            self.future_return_model_tx.clone(),
            self.action_type_model_tx.clone(),
            self.position_size_model_tx.clone(),
        ];

        let model_names = [
            "future_volatility",
            "future_volume",
            "future_trend_strength",
            "future_range",
            "future_return_mean",
            "future_return_std",
            "future_return_skew",
            "future_return_kurt",
            "risk_score",
            "drawdown_probability",
            "tail_event_probability",
            "volatility_spike_probability",
            "liquidity_drop_probability",
            "future_return",
            "action_type",
            "position_size",
        ];

        for (i, model_tx) in txs.iter().enumerate() {
            info!(
                "[Ensemble::train] Sending Train to model: {}",
                model_names[i]
            );
            let (tx, rx) = oneshot::channel();

            model_tx
                .send(ModelCommand::Train { respond_to: tx })
                .await
                .map_err(|e| {
                    anyhow!(
                        "[Ensemble::train] Failed to send Train to {}: {}",
                        model_names[i],
                        e
                    )
                })?;
            info!(
                "[Ensemble::train] Waiting for response from model: {}",
                model_names[i]
            );
            let result = rx.await.map_err(|e| {
                anyhow!(
                    "[Ensemble::train] Channel closed for model {} (recv error: {})",
                    model_names[i],
                    e
                )
            })?;
            info!(
                "[Ensemble::train] Model {} train result: {:?}",
                model_names[i],
                result.is_ok()
            );
            result.map_err(|e| {
                anyhow!(
                    "[Ensemble::train] Model {} training failed: {}",
                    model_names[i],
                    e
                )
            })?;
            info!(
                "[Ensemble::train] Model {} trained successfully",
                model_names[i]
            );
        }

        info!("[Ensemble::train] All models trained successfully");
        Ok(())
    }

    async fn predict(&self, data: DataMap) -> Result<DataMap, anyhow::Error> {
        let mut data = data.clone();
        let mut predictions = DataMap::new(data.symbol.clone(), BTreeMap::new());

        // TODO: Доставать accuracy и загружать как confidence

        // FIRST LAYER
        let fl_models = [
            self.future_volatility_model_tx.clone(),
            self.future_volume_model_tx.clone(),
            self.future_trend_strength_model_tx.clone(),
            self.future_range_model_tx.clone(),
            self.future_return_mean_model_tx.clone(),
            self.future_return_std_model_tx.clone(),
            self.future_return_skew_model_tx.clone(),
            self.future_return_kurt_model_tx.clone(),
        ];

        for model_tx in fl_models.iter() {
            let (tx, rx) = oneshot::channel();

            model_tx
                .send(ModelCommand::Predict {
                    data: data.to_standart(&SQLStandart::FirstLayer),
                    respond_to: tx,
                })
                .await
                .map_err(|e| {
                    anyhow!(
                        "[Ensemble::predict] Failed to send Predict to first layer model: {}",
                        e
                    )
                })?;
            let result = rx.await.map_err(|e| {
                anyhow!(
                    "[Ensemble::predict] Channel closed for first layer model (recv error: {})",
                    e
                )
            })?;
            for (k, v) in result.get_data().iter() {
                let key = get_prediction_name(k)
                    .ok_or_else(|| anyhow!("Prediction with name {} is not exists", k))?;
                predictions.entry(key.clone()).or_insert(*v);
                data.insert(key, *v);

                let confidence = match self.counters.get_option(k) {
                    Some(counter) => counter.get_accuracy(),
                    None => 100.0,
                };
                data.insert(
                    get_confidence_name(k)
                        .ok_or_else(|| anyhow!("Confidence with name {} is not exists", k))?,
                    confidence,
                );
            }
        }

        // SECOND LAYER
        let sl_models = [
            self.risk_score_model_tx.clone(),
            self.drawdown_probability_model_tx.clone(),
            self.tail_event_probability_model_tx.clone(),
            self.volatility_spike_probability_model_tx.clone(),
            self.liquidity_drop_probability_model_tx.clone(),
        ];

        for model_tx in sl_models.iter() {
            let (tx, rx) = oneshot::channel();

            model_tx
                .send(ModelCommand::Predict {
                    data: data.to_standart(&SQLStandart::SecondLayer),
                    respond_to: tx,
                })
                .await
                .map_err(|e| {
                    anyhow!(
                        "[Ensemble::predict] Failed to send Predict to second layer model: {}",
                        e
                    )
                })?;
            let result = rx.await.map_err(|e| {
                anyhow!(
                    "[Ensemble::predict] Channel closed for second layer model (recv error: {})",
                    e
                )
            })?;
            for (k, v) in result.get_data().iter() {
                let key = get_prediction_name(k)
                    .ok_or_else(|| anyhow!("Prediction with name {} is not exists", k))?;
                predictions.entry(key.clone()).or_insert(*v); // future_*_pred
                data.insert(key, *v);

                let confidence = match self.counters.get_option(k) {
                    Some(counter) => counter.get_accuracy(),
                    None => 100.0,
                };
                data.insert(
                    get_confidence_name(k)
                        .ok_or_else(|| anyhow!("Confidence with name {} is not exists", k))?,
                    confidence,
                );
            }
        }

        // THIRD LAYER
        let tl_models = [
            self.future_return_model_tx.clone(),
            self.action_type_model_tx.clone(),
            self.position_size_model_tx.clone(),
        ];
        for model_tx in tl_models.iter() {
            let (tx, rx) = oneshot::channel();

            model_tx
                .send(ModelCommand::Predict {
                    data: data.to_standart(&SQLStandart::ThirdLayer),
                    respond_to: tx,
                })
                .await
                .map_err(|e| {
                    anyhow!(
                        "[Ensemble::predict] Failed to send Predict to third layer model: {}",
                        e
                    )
                })?;
            let result = rx.await.map_err(|e| {
                anyhow!(
                    "[Ensemble::predict] Channel closed for third layer model (recv error: {})",
                    e
                )
            })?;
            for (k, v) in result.get_data().iter() {
                predictions.entry(k.to_string()).or_insert(*v);
                data.insert(k.to_string(), *v);
            }
        }

        Ok(predictions)
    }

    async fn handle_mistakes(
        &mut self,
        true_data: DataMap,
        predicted_data: DataMap,
    ) -> Result<(), anyhow::Error> {
        for (k, v) in true_data.get_data().iter() {
            if let Some(predicted) = predicted_data.get(k) {
                if (v - predicted).abs() < self.config.behaviour.success_threshold {
                    self.counters.get_mut(k).push(1); // future_*
                } else {
                    self.counters.get_mut(k).push(0);
                    let model_tx = self
                        .get_model_by_name(k)
                        .ok_or_else(|| anyhow!("Model tx with name '{}' not found", k))?;
                    let (tx, rx) = oneshot::channel();
                    model_tx
                        .send(ModelCommand::HandleMistakes {
                            true_data: true_data.clone(),
                            predicted_data: predicted_data.clone(),
                            respond_to: tx,
                        })
                        .await
                        .map_err(|e| anyhow!(
                            "[Ensemble::handle_mistakes] Failed to send HandleMistakes to model '{}': {}",
                            k, e
                        ))?;
                    let rx_result = rx.await
                        .map_err(|e| anyhow!(
                            "[Ensemble::handle_mistakes] Channel closed for model '{}' (recv error: {})",
                            k, e
                        ))?;
                    rx_result.map_err(|e| {
                        anyhow!(
                            "[Ensemble::handle_mistakes] Model '{}' HandleMistakes failed: {}",
                            k,
                            e
                        )
                    })?;
                }
            }
        }
        Ok(())
    }

    fn get_accuracy(&self) -> Option<DataMap> {
        let mut accs = BTreeMap::new();
        self.counters.symbols.iter().for_each(|model| {
            if !matches!(
                model.0.as_str(),
                "future_return" | "action_type" | "position_size" // future_*
            ) {
                let acc = model.1.get_accuracy();
                if let Some(conf_name) = get_confidence_name(model.0) {
                    accs.insert(conf_name, acc); // future_*_confidence
                }
            }
        });
        Some(DataMap::new("".to_string(), accs))
    }
}
