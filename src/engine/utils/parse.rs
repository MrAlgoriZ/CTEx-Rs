use crate::engine::utils::config::load_config::load_config;

const QUOTES: [&str; 10] = [
    "USDT", "USDC", "BUSD", "DAI", "BTC", "ETH", "BNB", "EUR", "USD", "TRY",
];

pub fn parse_symbol(symbol: &str) -> Option<String> {
    let config = load_config();
    let exchange = config.exchange.main_exchange.to_lowercase();

    match exchange.as_str() {
        "mexc" | "bybit" | "bitget" | "bingx" => Some(symbol.to_string()),
        _ => {
            for quote in QUOTES {
                if let Some(base) = symbol.strip_suffix(quote) {
                    if !base.is_empty() {
                        return Some(format!("{}/{}", base, quote));
                    }
                }
            }
            None
        }
    }
}
