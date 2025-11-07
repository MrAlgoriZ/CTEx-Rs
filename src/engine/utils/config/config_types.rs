use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub model: ModelConfig,
    pub data: DataConfig,
    pub token: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ModelConfig {
    pub n_trees: usize,
    pub max_depth: u16,
    pub seed: u64,
}

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    pub success_threshold: f64,
}
