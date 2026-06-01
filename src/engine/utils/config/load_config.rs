use crate::engine::utils::config::config_types::{Config, ModelConfig, RawConfig};
use crate::{CONFIG_PATH, MODEL_CONFIG_PATH};
use log::debug;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::OnceLock;

static CONFIG_CACHE: OnceLock<Config> = OnceLock::new();

pub fn load_config() -> Config {
    CONFIG_CACHE
        .get_or_init(|| {
            let raw_config_file = File::open(CONFIG_PATH).expect("Cannot open config file");
            let raw_config_reader = BufReader::new(raw_config_file);
            let raw_config: RawConfig =
                serde_yaml::from_reader(raw_config_reader).expect("Cannot parse YAML");

            let model_config_file = File::open(MODEL_CONFIG_PATH).expect("Cannot open config file");
            let model_reader = BufReader::new(model_config_file);
            let model_config: ModelConfig =
                serde_yaml::from_reader(model_reader).expect("Cannot parse YAML");

            raw_config.to_config(model_config)
        })
        .clone()
}

pub fn ensure_config_exists(paths: Vec<&str>) {
    for path in paths {
        if !Path::new(path).exists() {
            let yaml = match path {
                CONFIG_PATH => {
                    let cfg = RawConfig::default();
                    serde_yaml::to_string(&cfg).expect("Default config serialization failed")
                }
                MODEL_CONFIG_PATH => {
                    let cfg = ModelConfig::default();
                    serde_yaml::to_string(&cfg).expect("Default model config serialization failed")
                }
                _ => {
                    let cfg = Config::default();
                    serde_yaml::to_string(&cfg).expect("Default config serialization failed")
                }
            };
            if let Some(parent) = Path::new(path).parent() {
                fs::create_dir_all(parent).expect("Failed to create directory for config");
            }

            let mut file = File::create(path).expect("Failed to create file for config");
            file.write_all(yaml.as_bytes())
                .expect("Failed to write default config");
            debug!("Created default config: {}", path);
        }
    }
}
