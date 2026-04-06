use serde::{Deserialize, Serialize};

use crate::models::{ModelParams, ModelStructure, TaskType};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub model: ModelConfig,
    pub backend: BackendConfig,
    pub servers: Vec<String>,
    pub prints: PrintsConfig,
    pub behaviour: BehaviourConfig,
    pub runtime: RuntimeConfig,
    pub symbols: Vec<String>,
    pub main_exchange: String,
    pub timeframes: TimeframesConfig,
    pub mode: PrintMode,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelConfig {
    pub model_struct: ModelStructure,
    pub params: ModelParams,
    pub train_test_split: TrainTestSplit,
    pub metric: MetricType,
    pub seed: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TrainTestSplit {
    pub train_ratio: f32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum MetricType {
    MAE,
    MSE,
    RMSE,
    R2,
    Acc,
    Threshold,
    RAll,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BehaviourConfig {
    pub success_threshold: f64,
    pub accuracy_capacity: usize,
    pub predictions_capacity: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PrintsConfig {
    pub model: ModelPrintsConfig,
    pub cycle: CyclePrintsConfig,
    pub manager: ManagerPrintsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelPrintsConfig {
    pub skipped_values: bool,
    pub metrics: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BackendConfig {
    pub enabled: bool,
    pub listener: String,
    pub admin_password: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ManagerPrintsConfig {
    pub manager_init: bool,
    pub additional_manager_prints: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CyclePrintsConfig {
    pub volatility: bool,
    pub cycle_start: bool,
    pub price: bool,
    pub target: bool,
    pub prediction: bool,
    pub accuracy: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeType {
    Realtime,
    Backtest,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuntimeConfig {
    #[serde(rename = "type")]
    pub runtime_type: RuntimeType,
    pub with_training: bool,
    pub with_saves: bool,
    pub with_model: bool,
    pub cycle_type: CycleType,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimeframesConfig {
    #[serde(rename = "main")]
    pub main_timeframe: String,
    #[serde(rename = "background")]
    pub background_timeframe: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CycleType {
    Loader,
    Loaderwm,
    Training,
    Sandbox,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PrintMode {
    Log,
    Print,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            model: ModelConfig {
                model_struct: ModelStructure::Single,
                params: ModelParams::Single {
                    params: crate::models::SingleModelParams::XGBoost {
                        task_type: TaskType::Regression,
                        target_type: crate::models::TargetType::FutureReturn,
                        n_estimators: 100,
                        max_depth: 5,
                    },
                },
                train_test_split: TrainTestSplit { train_ratio: 0.8 },
                metric: MetricType::R2,
                seed: 42,
            },
            backend: BackendConfig {
                enabled: true,
                listener: "0.0.0.0:3000".to_string(),
                admin_password: "123".to_string(),
            },
            servers: vec!["127.0.0.1:3737".to_string()],
            prints: PrintsConfig {
                model: ModelPrintsConfig {
                    skipped_values: true,
                    metrics: false,
                },
                cycle: CyclePrintsConfig {
                    volatility: true,
                    cycle_start: true,

                    price: false,
                    target: true,
                    prediction: true,
                    accuracy: true,
                },
                manager: ManagerPrintsConfig {
                    manager_init: true,
                    additional_manager_prints: true,
                },
            },
            behaviour: BehaviourConfig {
                success_threshold: 0.125,
                accuracy_capacity: 192,
                predictions_capacity: 96,
            },
            symbols: vec!["BTCUSDT".to_string()],
            runtime: RuntimeConfig {
                runtime_type: RuntimeType::Realtime,
                with_training: false,
                with_model: false,
                with_saves: true,
                cycle_type: CycleType::Loader,
            },
            main_exchange: "binance".to_string(),
            timeframes: TimeframesConfig {
                main_timeframe: "15m".to_string(),
                background_timeframe: "1m".to_string(),
            },
            mode: PrintMode::Print,
        }
    }
}
