use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub model: ModelConfig,
    pub backend: BackendConfig,
    pub prints: PrintsConfig,
    pub behaviour: BehaviourConfig,
    pub token: Vec<String>,
    pub cycle_type: String,
    pub mode: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelConfig {
    pub name: String,
    pub n_trees: usize,
    pub max_depth: u16,
    pub seed: u64,
    pub train_test_split: TTSConfig,
    pub metric: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BehaviourConfig {
    pub success_threshold: SuccessThresholdConfig,
    pub risk_threshold: RiskThresholdConfig,
    pub trading_mode_value: TradingModeConfig,
    pub accuracy_capacity: usize,
    pub feedback_engine_capacity: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SuccessThresholdConfig {
    pub minimum: f64,
    pub default: f64,
    pub maximum: f64,
    pub ratio: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RiskThresholdConfig {
    pub minimum: f64,
    pub default: f64,
    pub maximum: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TradingModeConfig {
    pub minimum: f64,
    pub default: f64,
    pub maximum: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PrintsConfig {
    pub model: ModelPrintsConfig,
    pub cycle: CyclePrintsConfig,
    pub manager: ManagerPrintsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelPrintsConfig {
    pub evualate: bool,
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
pub struct TTSConfig {
    pub train_ratio: f32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            model: ModelConfig {
                name: "RF".to_string(),
                n_trees: 100,
                max_depth: 5,
                seed: 42,
                train_test_split: TTSConfig { train_ratio: 0.8 },
                metric: "MAE".to_string(),
            },
            backend: BackendConfig {
                enabled: true,
                listener: "0.0.0.0:3000".to_string(),
                admin_password: "123".to_string(),
            },
            prints: PrintsConfig {
                model: ModelPrintsConfig {
                    evualate: true,
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
                success_threshold: SuccessThresholdConfig {
                    minimum: 0.2,
                    default: 0.125,
                    maximum: 1.7,
                    ratio: 1.25,
                },
                risk_threshold: RiskThresholdConfig {
                    minimum: 0.5,
                    default: 1.0,
                    maximum: 3.0,
                },
                trading_mode_value: TradingModeConfig {
                    minimum: -1.0,
                    default: 0.0,
                    maximum: 1.0,
                },
                accuracy_capacity: 192,
                feedback_engine_capacity: 5,
            },
            token: vec!["BTCUSDT".to_string()],
            cycle_type: "trading".to_string(),
            mode: "print".to_string(),
        }
    }
}
