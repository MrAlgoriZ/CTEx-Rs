pub enum Fore {
    BLACK,
    RED,
    GREEN,
    YELLOW,
    WHITE,
}

impl Fore {
    pub fn as_str(&self) -> &'static str {
        match self {
            Fore::BLACK => "\x1b[30m",
            Fore::RED => "\x1b[31m",
            Fore::GREEN => "\x1b[32m",
            Fore::YELLOW => "\x1b[33m",
            Fore::WHITE => "\x1b[37m",
        }
    }
}
