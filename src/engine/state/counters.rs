#[derive(Clone, Copy)]
pub struct SymbolCounters {
    pub success: u16,
    pub common: u16,
    pub saved: u16,
    pub significant_success: u16,
    pub significant_common: u16,
}

impl SymbolCounters {
    fn new() -> Self {
        SymbolCounters {
            success: 0,
            common: 0,
            saved: 0,
            significant_success: 0,
            significant_common: 0,
        }
    }

    pub fn reset(&mut self) {
        self.success = 0;
        self.common = 0;
        self.saved = 0;
        self.significant_success = 0;
        self.significant_common = 0;
    }
}

#[derive(Clone, Copy)]
pub struct Counters {
    pub btc: SymbolCounters,
    pub eth: SymbolCounters,
    pub bnb: SymbolCounters,
    pub total: SymbolCounters,
}

impl Counters {
    pub fn new() -> Self {
        Counters {
            btc: SymbolCounters::new(),
            eth: SymbolCounters::new(),
            bnb: SymbolCounters::new(),
            total: SymbolCounters::new(),
        }
    }

    pub fn get(&mut self, symbol: &str) -> &mut SymbolCounters {
        match symbol.to_lowercase().as_str() {
            "btc" => &mut self.btc,
            "eth" => &mut self.eth,
            "bnb" => &mut self.bnb,
            _ => &mut self.total,
        }
    }
}
