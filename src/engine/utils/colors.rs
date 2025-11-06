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
        match self {
            Fore::RED => "\x1b[31m",
            Fore::GREEN => "\x1b[32m",
            Fore::YELLOW => "\x1b[33m",
            Fore::BLUE => "\x1b[34m",
            Fore::CYAN => "\x1b[36m",
            Fore::WHITE => "\x1b[37m",
        }
    }
}
