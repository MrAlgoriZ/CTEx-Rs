use crate::engine::utils::config::config_types::{
    BackendConfig, BehaviourConfig, Config, ExchangeConfig, ModelConfig, PrintsConfig,
    RuntimeConfig,
};
use crate::{
    BACKEND_CONFIG_PATH, BEHAVIOUR_CONFIG_PATH, EXCHANGE_CONFIG_PATH, MODEL_CONFIG_PATH,
    PRINTS_CONFIG_PATH, RUNTIME_CONFIG_PATH,
};

use anyhow::{Result, anyhow};
use log::debug;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::OnceLock;

static CONFIG: OnceLock<Config> = OnceLock::new();

struct ConfigBuilder {
    model: Option<ModelConfig>,
    backend: Option<BackendConfig>,
    prints: Option<PrintsConfig>,
    behaviour: Option<BehaviourConfig>,
    runtime: Option<RuntimeConfig>,
    exchange: Option<ExchangeConfig>,
}

impl ConfigBuilder {
    fn new() -> Self {
        Self {
            model: None,
            backend: None,
            prints: None,
            behaviour: None,
            runtime: None,
            exchange: None,
        }
    }

    fn model(mut self, model: ModelConfig) -> Self {
        self.model = Some(model);
        self
    }

    fn backend(mut self, backend: BackendConfig) -> Self {
        self.backend = Some(backend);
        self
    }

    fn prints(mut self, prints: PrintsConfig) -> Self {
        self.prints = Some(prints);
        self
    }

    fn behaviour(mut self, behaviour: BehaviourConfig) -> Self {
        self.behaviour = Some(behaviour);
        self
    }

    fn runtime(mut self, runtime: RuntimeConfig) -> Self {
        self.runtime = Some(runtime);
        self
    }

    fn exchange(mut self, exchange: ExchangeConfig) -> Self {
        self.exchange = Some(exchange);
        self
    }

    fn build(self) -> Result<Config> {
        let model = self.model.ok_or(anyhow!("ModelConfig is required"))?;
        let backend = self.backend.ok_or(anyhow!("BackendConfig is required"))?;
        let prints = self.prints.ok_or(anyhow!("PrintsConfig is required"))?;
        let behaviour = self
            .behaviour
            .ok_or(anyhow!("BehaviourConfig is required"))?;
        let runtime = self.runtime.ok_or(anyhow!("RuntimeConfig is required"))?;
        let exchange = self.exchange.ok_or(anyhow!("ExchangeConfig is required"))?;

        Ok(Config {
            model,
            backend,
            prints,
            behaviour,
            runtime,
            exchange,
        })
    }
}

pub fn config() -> &'static Config {
    CONFIG.get().expect("Config is not initialized")
}

pub fn load_config() -> Result<&'static Config> {
    CONFIG.get_or_init(|| load_config_inner().expect("Failed to load config"));

    Ok(config())
}

fn load_config_inner() -> Result<Config> {
    let model_config_file =
        File::open::<&OsStr>(MODEL_CONFIG_PATH.as_ref()).expect("Cannot open config file");
    let model_reader = BufReader::new(model_config_file);
    let model_config: ModelConfig =
        serde_yaml::from_reader(model_reader).expect("Cannot parse YAML");

    let backend_config_file =
        File::open::<&OsStr>(BACKEND_CONFIG_PATH.as_ref()).expect("Cannot open config file");
    let backend_reader = BufReader::new(backend_config_file);
    let backend_config: BackendConfig =
        serde_yaml::from_reader(backend_reader).expect("Cannot parse YAML");

    let behaviour_config_file =
        File::open::<&OsStr>(BEHAVIOUR_CONFIG_PATH.as_ref()).expect("Cannot open config file");
    let behaviour_reader = BufReader::new(behaviour_config_file);
    let behaviour_config: BehaviourConfig =
        serde_yaml::from_reader(behaviour_reader).expect("Cannot parse YAML");

    let exchange_config_file =
        File::open::<&OsStr>(EXCHANGE_CONFIG_PATH.as_ref()).expect("Cannot open config file");
    let exchange_reader = BufReader::new(exchange_config_file);
    let exchange_config: ExchangeConfig =
        serde_yaml::from_reader(exchange_reader).expect("Cannot parse YAML");

    let prints_config_file =
        File::open::<&OsStr>(PRINTS_CONFIG_PATH.as_ref()).expect("Cannot open config file");
    let prints_reader = BufReader::new(prints_config_file);
    let prints_config: PrintsConfig =
        serde_yaml::from_reader(prints_reader).expect("Cannot parse YAML");

    let runtime_config_file =
        File::open::<&OsStr>(RUNTIME_CONFIG_PATH.as_ref()).expect("Cannot open config file");
    let runtime_reader = BufReader::new(runtime_config_file);
    let runtime_config: RuntimeConfig =
        serde_yaml::from_reader(runtime_reader).expect("Cannot parse YAML");

    ConfigBuilder::new()
        .backend(backend_config)
        .behaviour(behaviour_config)
        .exchange(exchange_config)
        .model(model_config)
        .prints(prints_config)
        .runtime(runtime_config)
        .build()
}

pub fn ensure_config_exists(paths: Vec<&Path>) {
    for path in paths {
        if !Path::new(path).exists() {
            let model_config_path = &*MODEL_CONFIG_PATH;
            let backend_config_path = &*BACKEND_CONFIG_PATH;
            let behaviour_config_path = &*BEHAVIOUR_CONFIG_PATH;
            let exchange_config_path = &*EXCHANGE_CONFIG_PATH;
            let prints_config_path = &*PRINTS_CONFIG_PATH;
            let runtime_config_path = &*RUNTIME_CONFIG_PATH;

            let yaml = match path {
                p if p == model_config_path => {
                    let cfg = ModelConfig::default();
                    serde_yaml::to_string(&cfg).expect("Default model config serialization failed")
                }
                p if p == backend_config_path => {
                    let cfg = BackendConfig::default();
                    serde_yaml::to_string(&cfg)
                        .expect("Default backend config serialization failed")
                }
                p if p == behaviour_config_path => {
                    let cfg = BehaviourConfig::default();
                    serde_yaml::to_string(&cfg)
                        .expect("Default behaviour config serialization failed")
                }
                p if p == exchange_config_path => {
                    let cfg = ExchangeConfig::default();
                    serde_yaml::to_string(&cfg)
                        .expect("Default exchange config serialization failed")
                }
                p if p == prints_config_path => {
                    let cfg = PrintsConfig::default();
                    serde_yaml::to_string(&cfg).expect("Default prints config serialization failed")
                }
                p if p == runtime_config_path => {
                    let cfg = RuntimeConfig::default();
                    serde_yaml::to_string(&cfg)
                        .expect("Default runtime config serialization failed")
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
            debug!("Created default config: {}", path.to_string_lossy());
        }
    }
}
