use anyhow::anyhow;
use log::{error, info, warn};
use sqlx::PgPool;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::actors::chain::{ChainActor, ChainCommand};
use crate::engine::actors::counter::{CounterActor, CounterCommand};
use crate::engine::actors::model::{ModelActor, ModelCommand};
use crate::engine::actors::prediction::{PredictionsActor, PredictionsCommand};
use crate::engine::actors::service::{ServiceActor, ServiceCommand};
use crate::engine::cycles::background::cycle::BackgroundCycle;
use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::loaderwm::cycle::LoaderWMCycle;
use crate::engine::cycles::sandbox::cycle::SandboxCycle;
use crate::engine::cycles::training::cycle::TrainingCycle;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::{Config, CycleType, RuntimeType};
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;
use crate::models::model::{Model, init_ensemble_model, init_single_model};
use crate::models::{ModelParams, ModelStructure};

pub enum CycleError {
    SymbolDoesNotExist,
    AnyhowError(anyhow::Error),
}

impl From<anyhow::Error> for CycleError {
    fn from(err: anyhow::Error) -> Self {
        CycleError::AnyhowError(err)
    }
}

pub enum SupervisorCommand {
    StartCycle {
        symbol: String,
        cycle_type: CycleType,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    StopCycle {
        symbol: String,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    StopAll {
        respond_to: oneshot::Sender<()>,
    },
    ListActive {
        respond_to: oneshot::Sender<Vec<String>>,
    },
    SetModel {
        model_tx: mpsc::Sender<ModelCommand>,
    },
    SetChain {
        chain_tx: mpsc::Sender<ChainCommand>,
    },
    ChainHandle {
        respond_to: oneshot::Sender<Option<mpsc::Sender<ChainCommand>>>,
    },
}

struct CycleSupervisor {
    workers: HashMap<String, WorkerHandle>,
    model_tx: Option<mpsc::Sender<ModelCommand>>,
    chain_tx: Option<mpsc::Sender<ChainCommand>>,
    counter_tx: mpsc::Sender<CounterCommand>,
    server_tx: mpsc::Sender<ServiceCommand>,
    inbox: mpsc::Receiver<SupervisorCommand>,
}

impl CycleSupervisor {
    fn new(
        counter_tx: mpsc::Sender<CounterCommand>,
        server_tx: mpsc::Sender<ServiceCommand>,
    ) -> (Self, mpsc::Sender<SupervisorCommand>) {
        let (tx, rx) = mpsc::channel(50);

        (
            Self {
                workers: HashMap::new(),
                model_tx: None,
                chain_tx: None,
                counter_tx,
                server_tx,
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        info!("Supervisor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                SupervisorCommand::StartCycle {
                    symbol,
                    cycle_type,
                    respond_to,
                } => {
                    let result = self.start_worker(symbol, cycle_type).await;
                    let _ = respond_to.send(result);
                }

                SupervisorCommand::StopCycle { symbol, respond_to } => {
                    let result = self.stop_worker(&symbol).await;
                    let _ = respond_to.send(result);
                }

                SupervisorCommand::StopAll { respond_to } => {
                    self.stop_all_workers().await;
                    let _ = respond_to.send(());
                }

                SupervisorCommand::ListActive { respond_to } => {
                    let active: Vec<String> = self.workers.keys().cloned().collect();
                    let _ = respond_to.send(active);
                }

                SupervisorCommand::SetModel { model_tx } => {
                    self.model_tx = Some(model_tx);
                    info!("Model has installed in Supervisor");
                }

                SupervisorCommand::SetChain { chain_tx } => {
                    self.chain_tx = Some(chain_tx);
                    info!("Chain has installed in Supervisor");
                }

                SupervisorCommand::ChainHandle { respond_to } => {
                    let _ = respond_to.send(self.chain_tx.clone());
                }
            }
        }

        warn!("Supervisor has stopped!");
    }

    async fn start_worker(
        &mut self,
        symbol: String,
        cycle_type: CycleType,
    ) -> Result<(), anyhow::Error> {
        if self.workers.contains_key(&symbol) {
            return Err(anyhow!(format!("Worker {} already running!", symbol)));
        }

        if matches!(cycle_type, CycleType::Training | CycleType::Sandbox) && self.model_tx.is_none()
        {
            return Err(anyhow!("Model is not initialized for cycle!"));
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let model_tx = self.model_tx.clone();
        let counter_tx = self.counter_tx.clone();
        let server_tx = self.server_tx.clone();
        let chain_tx = self.chain_tx.clone();
        let symbol_clone = symbol.clone();

        let task = tokio::spawn(async move {
            Self::worker_loop(
                symbol_clone,
                cycle_type,
                counter_tx,
                server_tx,
                model_tx,
                chain_tx,
                shutdown_rx,
            )
            .await;
        });

        self.workers.insert(
            symbol.clone(),
            WorkerHandle {
                symbol: symbol.clone(),
                task,
                shutdown_tx,
            },
        );

        info!("Worker {} has started ({:?})!", symbol, cycle_type);
        Ok(())
    }

    async fn stop_worker(&mut self, symbol: &str) -> Result<(), anyhow::Error> {
        match self.workers.remove(symbol) {
            Some(handle) => {
                handle.stop().await;
                Ok(())
            }
            None => Err(anyhow!(format!("Worker {} not found!", symbol))),
        }
    }

    async fn stop_all_workers(&mut self) {
        let workers = std::mem::take(&mut self.workers);
        for (_, handle) in workers {
            handle.stop().await;
        }
    }

    async fn worker_loop(
        symbol: String,
        cycle_type: CycleType,
        counter_tx: mpsc::Sender<CounterCommand>,
        server_tx: mpsc::Sender<ServiceCommand>,
        model_tx: Option<mpsc::Sender<ModelCommand>>,
        chain_tx: Option<mpsc::Sender<ChainCommand>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    warn!("Worker {} received shutdown signal!", symbol);
                    break;
                }

                result = Self::run_cycle_once(&symbol, cycle_type, &counter_tx, &server_tx, &model_tx, &chain_tx) => {
                    match result {
                        Ok(_) => {
                            info!("Worker {} has finished normally", symbol);
                            if let Some(ctx) = &chain_tx {
                                let (tx, rx) = oneshot::channel();
                                let _ = ctx
                                    .send(ChainCommand::DeleteChain {
                                        symbol: symbol.clone(),
                                        respond_to: tx,
                                    })
                                    .await;
                                let _ = rx.await;
                            }
                            break;
                        }
                        Err(e) => {
                            match e {
                                CycleError::AnyhowError(err) => {
                                    error!("Worker {} crashed: {}, restarting in 5 sec", symbol, err);
                                    if let Some(ctx) = &chain_tx {
                                        let (tx, rx) = oneshot::channel();
                                        let _ = ctx
                                            .send(ChainCommand::DeleteChain {
                                                symbol: symbol.clone(),
                                                respond_to: tx,
                                            })
                                            .await;
                                        let _ = rx.await;
                                    }
                                    sleep(Duration::from_secs(5)).await;
                                }
                                CycleError::SymbolDoesNotExist => {
                                    error!("Token {} does not exist!", symbol);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn run_cycle_once(
        symbol: &str,
        cycle_type: CycleType,
        counter_tx: &mpsc::Sender<CounterCommand>,
        server_tx: &mpsc::Sender<ServiceCommand>,
        model_tx: &Option<mpsc::Sender<ModelCommand>>,
        chain_tx: &Option<mpsc::Sender<ChainCommand>>,
    ) -> Result<(), CycleError> {
        let config = load_config();
        let client = CCXTClient::new(&config.exchange.main_exchange, server_tx.clone());

        match cycle_type {
            CycleType::Loader => {
                match config.model.model_struct {
                    ModelStructure::Ensemble => {
                        warn!(
                            "{}LoaderCycle not for Ensemble model type. Use LoaderWMCycle instead!",
                            Fore::YELLOW.as_str()
                        );
                    }
                    _ => {}
                }
                let cycle = LoaderCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle.run().await.map_err(CycleError::from)?,
                    RuntimeType::Backtest => {
                        cycle.run_backtest().await.map_err(CycleError::from)?
                    }
                }
            }
            CycleType::Loaderwm => {
                match config.model.model_struct {
                    ModelStructure::Single => {
                        warn!(
                            "{}LoaderWMCycle not for Single model type. Use LoaderCycle instead!",
                            Fore::YELLOW.as_str()
                        );
                    }
                    _ => {}
                }
                let cycle = LoaderWMCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model_tx.as_ref(), chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model_tx.as_ref(), chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                }
            }
            CycleType::Training => {
                let cycle = TrainingCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                let model = model_tx.as_ref().ok_or_else(|| {
                    CycleError::AnyhowError(anyhow!("Model not initialized for Training cycle!"))
                })?;
                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                }
            }
            CycleType::Sandbox => {
                let cycle = SandboxCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                let model = model_tx.as_ref().ok_or_else(|| {
                    CycleError::AnyhowError(anyhow!("Model not initialized for Sandbox cycle!"))
                })?;
                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                }
            }
        }
        Ok(())
    }
}

struct WorkerHandle {
    symbol: String,
    task: tokio::task::JoinHandle<()>,
    shutdown_tx: mpsc::Sender<()>,
}

impl WorkerHandle {
    async fn stop(self) {
        let _ = self.shutdown_tx.send(()).await;
        let _ = self.task.await;
        warn!("Worker {} has stopped!", self.symbol);
    }
}

pub struct CycleManager {
    config: Config,
    supervisor_tx: mpsc::Sender<SupervisorCommand>,
    counter_tx: mpsc::Sender<CounterCommand>,
    prediction_tx: mpsc::Sender<PredictionsCommand>,
    _counter_task: tokio::task::JoinHandle<()>,
    _supervisor_task: tokio::task::JoinHandle<()>,
    _servers_task: tokio::task::JoinHandle<()>,
    _prediction_task: tokio::task::JoinHandle<()>,
}

impl CycleManager {
    pub async fn new() -> Self {
        let config = load_config();
        let counter_capacity = config.behaviour.accuracy_capacity;
        let prediction_capacity = config.behaviour.predictions_capacity;

        let (servers_actor, servers_tx) = ServiceActor::new().await;
        let servers_task = tokio::spawn(servers_actor.run());

        let (counter_actor, counter_tx) = CounterActor::new(counter_capacity);
        let counter_task = tokio::spawn(counter_actor.run());

        let (prediction_actor, prediction_tx) = PredictionsActor::new(prediction_capacity);
        let prediction_task = tokio::spawn(prediction_actor.run());

        let (supervisor, supervisor_tx) =
            CycleSupervisor::new(counter_tx.clone(), servers_tx.clone());
        let supervisor_task = tokio::spawn(supervisor.run());

        let background_cycle = BackgroundCycle::new(load_config(), servers_tx);
        let _ = tokio::spawn(background_cycle.run());

        Self {
            config,
            supervisor_tx,
            counter_tx,
            prediction_tx,
            _counter_task: counter_task,
            _supervisor_task: supervisor_task,
            _servers_task: servers_task,
            _prediction_task: prediction_task,
        }
    }

    pub async fn run_all(
        &mut self,
        symbols: Vec<String>,
        cycle_types: HashMap<String, CycleType>,
    ) -> Result<(), anyhow::Error> {
        let needs_model = symbols.iter().any(|symbol| {
            matches!(
                cycle_types.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Training | CycleType::Sandbox
            ) | (matches!(
                cycle_types.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Loaderwm
            ) && self.config.runtime.with_model)
        });

        if needs_model {
            self.initialize_model().await.map_err(|e| anyhow!(e))?;
        }

        if needs_model && self.config.model.generate_plots {
            self.initialize_chain().await.map_err(|e| anyhow!(e))?;
        }

        for symbol in &symbols {
            let cycle_type = cycle_types.get(symbol).unwrap_or(&CycleType::Loader);
            self.add_cycle(symbol.clone(), *cycle_type).await?;
        }

        info!("Started {} cycles: {}", symbols.len(), symbols.join(", "));

        Ok(())
    }

    async fn initialize_model(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StopAll { respond_to: tx })
            .await
            .map_err(|_| "Supervisor is unavailable!")?;
        let _ = rx.await;

        let pool = PgPool::connect(&load_env().database_url)
            .await
            .map_err(|e| format!("Database connection error: {}", e))?;

        let params = load_config().model.params;

        let mut model: Box<dyn Model + Send + Sync> = match params {
            ModelParams::Ensemble {
                future_volatility_model_params,
                future_volume_model_params,
                future_trend_strength_model_params,
                future_range_model_params,
                future_return_mean_model_params,
                future_return_std_model_params,
                future_return_skew_model_params,
                future_return_kurt_model_params,
                risk_score_model_params,
                drawdown_probability_model_params,
                tail_event_probability_model_params,
                volatility_spike_probability_model_params,
                liquidity_drop_probability_model_params,
                future_return_model_params,
                action_type_model_params,
                position_size_model_params,
            } => init_ensemble_model(
                Some(self.prediction_tx.clone()),
                pool.clone(),
                future_volatility_model_params,
                future_volume_model_params,
                future_trend_strength_model_params,
                future_range_model_params,
                future_return_mean_model_params,
                future_return_std_model_params,
                future_return_skew_model_params,
                future_return_kurt_model_params,
                risk_score_model_params,
                drawdown_probability_model_params,
                tail_event_probability_model_params,
                volatility_spike_probability_model_params,
                liquidity_drop_probability_model_params,
                future_return_model_params,
                action_type_model_params,
                position_size_model_params,
            ),
            ModelParams::Single { params } => init_single_model(
                params,
                Some(self.prediction_tx.clone()),
                SQLStandart::SingleModel,
                pool,
            ),
        };

        model
            .train()
            .await
            .map_err(|e| format!("Model training error: {}", e))?;

        let (model_actor, model_tx) = ModelActor::new(model);
        tokio::spawn(model_actor.run());

        self.supervisor_tx
            .send(SupervisorCommand::SetModel { model_tx })
            .await
            .map_err(|_| "Failed to update model in Supervisor!")?;

        Ok(())
    }

    async fn initialize_chain(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StopAll { respond_to: tx })
            .await
            .map_err(|_| "Supervisor is unavailable!")?;
        let _ = rx.await;

        let (chain_actor, chain_tx) = ChainActor::new();
        tokio::spawn(chain_actor.run());

        self.supervisor_tx
            .send(SupervisorCommand::SetChain { chain_tx })
            .await
            .map_err(|_| "Failed to update chain in Supervisor!")?;

        Ok(())
    }

    pub async fn add_cycle(
        &self,
        symbol: String,
        cycle_type: CycleType,
    ) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StartCycle {
                symbol,
                cycle_type,
                respond_to: tx,
            })
            .await
            .map_err(|_| anyhow!("Supervisor is unavailable!"))?;

        rx.await
            .map_err(|_| anyhow!("No response from Supervisor!"))?
    }

    // Handles need only for REST API. If tx isn't need for API, don't do handle for this
    pub fn counter_handle(&self) -> mpsc::Sender<CounterCommand> {
        self.counter_tx.clone()
    }

    pub fn supervisor_handle(&self) -> mpsc::Sender<SupervisorCommand> {
        self.supervisor_tx.clone()
    }

    pub fn prediction_handle(&self) -> mpsc::Sender<PredictionsCommand> {
        self.prediction_tx.clone()
    }
}
