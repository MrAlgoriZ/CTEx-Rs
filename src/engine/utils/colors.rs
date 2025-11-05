pub enum Fore {
    BLACK,
    RED,
    GREEN,
    YELLOW,
    BLUE,
    PURPLE,
    CYAN,
    WHITE,

    RESET,
}

impl Fore {
    pub fn as_str(&self) -> &'static str {
        match self {
            Fore::BLACK => "\x1b[30m",
            Fore::RED => "\x1b[31m",
            Fore::GREEN => "\x1b[32m",
            Fore::YELLOW => "\x1b[33m",
            Fore::BLUE => "\x1b[34m",
            Fore::PURPLE => "\x1b[35m",
            Fore::CYAN => "\x1b[36m",
            Fore::WHITE => "\x1b[37m",
            Fore::RESET => "\x1b[0m",
        }
    }
}
