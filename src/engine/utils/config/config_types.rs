use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub model: ModelConfig,
    pub data: DataConfig,
    pub token: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ModelConfig {
    pub n_estimators: usize,
    pub max_depth: usize,
    pub criterion: String,
}

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    pub success_threshold: f64,
}
