use std::collections::{HashMap, VecDeque};

#[derive(Clone)]
pub struct SymbolCounters<T> {
    pub data: VecDeque<T>,
    capacity: usize,
}

impl<T> SymbolCounters<T> {
    pub fn new(capacity: usize) -> Self {
        SymbolCounters {
            data: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.data.len() == self.capacity {
            self.data.pop_front();
        }
        self.data.push_back(value);
    }
}

impl<T: Copy + Into<u16>> SymbolCounters<T> {
    pub fn get_accuracy(&self) -> f64 {
        let sum: u16 = self.data.iter().map(|&v| v.into()).sum();
        (sum as f64) / (self.data.len() as f64) * 100.0
    }

    pub fn get_shifted_accuracy(&self, window: usize) -> Option<f64> {
        if window == 0 || self.data.is_empty() {
            None
        } else {
            let actual_window = window.min(self.data.len());
            let sum: u16 = self
                .data
                .iter()
                .rev()
                .take(actual_window)
                .map(|&v| v.into())
                .sum();

            Some((sum as f64) / (actual_window as f64) * 100.0)
        }
    }
}

#[derive(Clone)]
pub struct Counters {
    pub symbols: HashMap<String, SymbolCounters<u8>>,
    pub capacity: usize,
}

impl Counters {
    pub fn new(capacity: usize) -> Self {
        Counters {
            symbols: HashMap::new(),
            capacity,
        }
    }

    pub fn get_mut(&mut self, symbol: &str) -> &mut SymbolCounters<u8> {
        let key = symbol.to_uppercase();
        self.symbols
            .entry(key)
            .or_insert_with(|| SymbolCounters::new(self.capacity))
    }

    pub fn get_option(&self, symbol: &str) -> Option<&SymbolCounters<u8>> {
        let key = symbol.to_uppercase();
        self.symbols.get(&key)
    }
}
