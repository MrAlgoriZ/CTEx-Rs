use crate::engine::utils::config::config_types::Config;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::OnceLock;

static CONFIG_CACHE: OnceLock<Config> = OnceLock::new();

pub fn load_config_with_cache(path: &str) -> &'static Config {
    CONFIG_CACHE.get_or_init(|| {
        let file = File::open(path).expect("Cannot open config file");
        let reader = BufReader::new(file);
        serde_yaml::from_reader(reader).expect("Cannot parse YAML")
    })
}
pub fn load_config(path: &str) -> Config {
    load_config_with_cache(path).clone()
}

pub fn ensure_config_exists(path: &str) {
    if !Path::new(path).exists() {
        let cfg = Config::default();
        let yaml = serde_yaml::to_string(&cfg).expect("Не удалось сериализовать дефолтный конфиг");

        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent).expect("Не удалось создать директорию для конфига");
        }

        let mut file = File::create(path).expect("Не удалось создать файл конфига");
        file.write_all(yaml.as_bytes())
            .expect("Не удалось записать дефолтный конфиг");
        println!("Создан дефолтный конфиг: {}", path);
    }
}
