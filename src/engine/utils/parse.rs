const QUOTES: [&str; 10] = [
    "USDT", "USDC", "BUSD", "DAI", "BTC", "ETH", "BNB", "EUR", "USD", "TRY",
];

pub fn parse_symbol(symbol: &str) -> Option<String> {
    for quote in QUOTES {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some(format!("{}/{}", base, quote));
            }
        }
    }

    None
}
