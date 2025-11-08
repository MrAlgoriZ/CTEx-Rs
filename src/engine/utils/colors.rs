use crate::engine::utils::config::load_config::load_config;
use std::sync::OnceLock;

pub enum Fore {
    RED,
    GREEN,
    YELLOW,
    BLUE,
    CYAN,
    WHITE,
}

impl Fore {
    pub fn as_str(&self) -> &'static str {
        static CONFIG_MODE: OnceLock<String> = OnceLock::new();

        let mode = CONFIG_MODE.get_or_init(|| load_config("config/config.yaml").mode);

        if mode == "print" {
            match self {
                Fore::RED => "\x1b[31m",
                Fore::GREEN => "\x1b[32m",
                Fore::YELLOW => "\x1b[33m",
                Fore::BLUE => "\x1b[34m",
                Fore::CYAN => "\x1b[36m",
                Fore::WHITE => "\x1b[37m",
            }
        } else {
            ""
        }
    }
}
