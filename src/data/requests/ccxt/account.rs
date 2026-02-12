use anyhow::anyhow;
use std::collections::{HashMap, VecDeque};

use crate::data::requests::ccxt::client::CCXTClient;

#[derive(Clone)]
pub enum Direction {
    Buy,
    Sell,
}

#[derive(Clone)]
pub struct Order {
    #[allow(unused)]
    pub symbol: String,
    #[allow(unused)]
    pub amount: f64,
    #[allow(unused)]
    pub direction: Direction,
}

pub trait IAccount {
    #[allow(unused)]
    fn init(api_key: String, secret_key: String) -> Self;
    #[allow(unused)]
    async fn create_order(
        &mut self,
        symbol: String,
        amount: f64,
        direction: Direction,
    ) -> Result<Order, anyhow::Error>;
    #[allow(unused)]
    async fn cancel_order(&mut self, order: Order) -> Result<(), anyhow::Error>;
    #[allow(unused)]
    async fn get_open_orders(&self) -> Result<Option<VecDeque<Order>>, anyhow::Error>;
    #[allow(unused)]
    async fn get_balance(&self) -> Result<Option<HashMap<String, f64>>, anyhow::Error>;
}

// pub struct Account {}
// impl IAccount for Account {}

pub struct DummyAccount {
    balance: HashMap<String, f64>,
    orders: VecDeque<Order>,
}

impl DummyAccount {
    pub fn init(api_key: String, secret_key: String) -> Self {
        drop(api_key);
        drop(secret_key);
        let mut balance = HashMap::new();
        balance.insert("USDT".to_string(), 100.0);
        Self {
            balance,
            orders: VecDeque::new(),
        }
    }

    pub async fn get_balance_usdt(
        &self,
        client: &CCXTClient,
    ) -> Result<Option<f64>, anyhow::Error> {
        let mut total_usdt = 0.0;

        for (symbol, amount) in &self.balance {
            if *amount == 0.0 {
                continue;
            } else if symbol.eq("USDT") {
                total_usdt += amount;
                continue;
            }

            let price = client.fetch_ticker(symbol).await?.bid;

            total_usdt += amount * price;
        }

        Ok(Some(total_usdt))
    }

    pub async fn create_fake_order(
        &mut self,
        symbol: String,
        amount: f64,
        direction: Direction,
        price: f64,
    ) -> Result<Order, anyhow::Error> {
        match direction {
            Direction::Buy => {
                let cost = amount * price;
                let usdt_balance = self
                    .balance
                    .get_mut("USDT")
                    .ok_or(anyhow!("No USDT balance"))?;
                if *usdt_balance < cost {
                    return Err(anyhow!("Insufficient USDT"));
                }
                *usdt_balance -= cost;
                *self.balance.entry(symbol.clone()).or_insert(0.0) += amount;
            }
            Direction::Sell => {
                let token_balance = self
                    .balance
                    .get_mut(&symbol)
                    .ok_or(anyhow!("No token balance"))?;
                if *token_balance < amount {
                    return Err(anyhow!("Insufficient token balance"));
                }
                *token_balance -= amount;
                *self.balance.entry("USDT".to_string()).or_insert(0.0) += amount * price;
            }
        }

        let order = Order {
            symbol,
            amount,
            direction,
        };
        self.orders.push_back(order.clone());
        Ok(order)
    }

    #[allow(unused)]
    async fn cancel_fake_order(
        &mut self,
        order: Order,
        client: &CCXTClient,
    ) -> Result<(), anyhow::Error> {
        if let Some(pos) = self.orders.iter().position(|o| {
            o.symbol == order.symbol && o.amount == order.amount && matches!(o.direction, _)
        }) {
            self.orders.remove(pos);

            let price = client.fetch_ticker(&order.symbol).await?.bid;
            match order.direction {
                Direction::Buy => {
                    *self.balance.entry("USDT".to_string()).or_insert(0.0) += order.amount * price;
                    *self.balance.entry(order.symbol).or_insert(0.0) -= order.amount;
                }
                Direction::Sell => {
                    *self.balance.entry("USDT".to_string()).or_insert(0.0) -= order.amount * price;
                    *self.balance.entry(order.symbol).or_insert(0.0) += order.amount;
                }
            }
            Ok(())
        } else {
            Err(anyhow!("Order not found"))
        }
    }

    #[allow(unused)]
    pub fn get_open_orders(&self) -> Result<Option<VecDeque<Order>>, anyhow::Error> {
        Ok(Some(self.orders.clone()))
    }

    #[allow(unused)]
    pub fn get_balance(&self) -> Result<Option<HashMap<String, f64>>, anyhow::Error> {
        Ok(Some(self.balance.clone()))
    }
}
