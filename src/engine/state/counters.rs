#[derive(Clone, Copy)]
pub struct SymbolCounters {
    pub success: u16,
    pub common: u16,
    pub saved: u16,
}

impl SymbolCounters {
    fn new() -> Self {
        SymbolCounters {
            success: 0,
            common: 0,
            saved: 0,
        }
    }

    pub fn reset(&mut self) {
        self.success = 0;
        self.common = 0;
        self.saved = 0;
    }
}

#[derive(Clone, Copy)]
pub struct Counters {
    pub btc: SymbolCounters,
    pub eth: SymbolCounters,
    pub bnb: SymbolCounters,
    pub doge: SymbolCounters,
    pub ada: SymbolCounters,
    pub xrp: SymbolCounters,
    pub sol: SymbolCounters,
    pub dot: SymbolCounters,
    pub avax: SymbolCounters,
    pub matic: SymbolCounters,
    pub link: SymbolCounters,
    pub ltc: SymbolCounters,
    pub bch: SymbolCounters,
    pub trx: SymbolCounters,
    pub near: SymbolCounters,
    pub apt: SymbolCounters,
    pub ton: SymbolCounters,
    pub sui: SymbolCounters,
    pub fil: SymbolCounters,
    pub ftm: SymbolCounters,
    pub total: SymbolCounters,
}

impl Counters {
    pub fn new() -> Self {
        Counters {
            btc: SymbolCounters::new(),
            eth: SymbolCounters::new(),
            bnb: SymbolCounters::new(),
            doge: SymbolCounters::new(),
            ada: SymbolCounters::new(),
            xrp: SymbolCounters::new(),
            sol: SymbolCounters::new(),
            dot: SymbolCounters::new(),
            avax: SymbolCounters::new(),
            matic: SymbolCounters::new(),
            link: SymbolCounters::new(),
            ltc: SymbolCounters::new(),
            bch: SymbolCounters::new(),
            trx: SymbolCounters::new(),
            near: SymbolCounters::new(),
            apt: SymbolCounters::new(),
            ton: SymbolCounters::new(),
            sui: SymbolCounters::new(),
            fil: SymbolCounters::new(),
            ftm: SymbolCounters::new(),
            total: SymbolCounters::new(),
        }
    }

    pub fn get(&mut self, symbol: &str) -> &mut SymbolCounters {
        match symbol.to_lowercase().as_str() {
            "btcusdt" => &mut self.btc,
            "ethusdt" => &mut self.eth,
            "bnbusdt" => &mut self.bnb,
            "dogeusdt" => &mut self.doge,
            "adausdt" => &mut self.ada,
            "xrpusdt" => &mut self.xrp,
            "solusdt" => &mut self.sol,
            "dotusdt" => &mut self.dot,
            "avaxusdt" => &mut self.avax,
            "maticusdt" => &mut self.matic,
            "linkusdt" => &mut self.link,
            "ltcusdt" => &mut self.ltc,
            "bchusdt" => &mut self.bch,
            "trxusdt" => &mut self.trx,
            "nearusdt" => &mut self.near,
            "aptusdt" => &mut self.apt,
            "tonusdt" => &mut self.ton,
            "suiusdt" => &mut self.sui,
            "filusdt" => &mut self.fil,
            "ftmusdt" => &mut self.ftm,
            _ => &mut self.total,
        }
    }
}
