use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub model: ModelConfig,
    pub data: DataConfig,
    pub token: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ModelConfig {
    #[serde(rename = "type")]
    pub model_type: String,
    pub n_estimators: usize,
    pub max_depth: usize,
    pub criterion: String,
}

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    #[serde(rename = "type")]
    pub data_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}
