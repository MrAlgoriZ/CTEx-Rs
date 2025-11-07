use crate::engine::utils::config::config_types::Config;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;

pub fn load_config(path: &str) -> Config {
    let file = File::open(path).expect("Cannot open config file");
    let reader = BufReader::new(file);
    let config: Config = serde_yaml::from_reader(reader).expect("Cannot parse YAML");
    config
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
    } else {
    }
}
