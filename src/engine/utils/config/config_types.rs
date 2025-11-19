use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub model: ModelConfig,
    pub backend: BackendConfig,
    pub prints: PrintsConfig,
    pub data: DataConfig,
    pub token: Vec<String>,
    pub cycle_type: String,
    pub mode: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ModelConfig {
    pub name: String,
    pub n_trees: usize,
    pub max_depth: u16,
    pub seed: u64,
    pub train_test_split: TTSConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DataConfig {
    pub success_threshold: f64,
    pub accuracy_capacity: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PrintsConfig {
    pub volatility: bool,
    pub cycle_start: bool,
    pub model_evualate: bool,
    pub price: bool,
    pub target: bool,
    pub prediction: bool,
    pub accuracy: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    pub listener: String,
    pub admin_password: String,
}

#[derive(Debug, Deserialize, Serialize)]
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
            },
            backend: BackendConfig {
                listener: "0.0.0.0:3000".to_string(),
                admin_password: "123".to_string(),
            },
            prints: PrintsConfig {
                volatility: true,
                cycle_start: true,
                model_evualate: true,
                price: false,
                target: true,
                prediction: true,
                accuracy: true,
            },
            data: DataConfig {
                success_threshold: 8.0,
                accuracy_capacity: 96,
            },
            token: vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "BNBUSDT".to_string(),
                "DOGEUSDT".to_string(),
                "ADAUSDT".to_string(),
                "XRPUSDT".to_string(),
                "SOLUSDT".to_string(),
            ],
            cycle_type: "trading".to_string(),
            mode: "print".to_string(),
        }
    }
}
