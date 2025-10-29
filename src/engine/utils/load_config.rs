use crate::engine::utils::config_types::Config;
use std::fs::File;
use std::io::BufReader;

pub fn load_config(path: &str) -> Config {
    let file = File::open(path).expect("Cannot open config file");
    let reader = BufReader::new(file);
    let config: Config = serde_yaml::from_reader(reader).expect("Cannot parse YAML");
    config
}
