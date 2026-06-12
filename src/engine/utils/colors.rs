use crate::engine::utils::config::config_types::PrintMode;
use crate::engine::utils::config::load_config::load_config;

use std::sync::OnceLock;

pub enum Fore {
    Red,
    Green,
    Yellow,
    Blue,
    White,
}

impl Fore {
    pub fn as_str(&self) -> &'static str {
        static CONFIG_MODE: OnceLock<PrintMode> = OnceLock::new();

        let mode = CONFIG_MODE.get_or_init(|| load_config().mode);

        match mode {
            PrintMode::Print => match self {
                Fore::Red => "\x1b[31m",
                Fore::Green => "\x1b[32m",
                Fore::Yellow => "\x1b[33m",
                Fore::Blue => "\x1b[34m",
                Fore::White => "\x1b[37m",
            },
            PrintMode::Log => "",
        }
    }
}
