use crate::data::data_interfaces::Timeframe;
use crate::engine::cycles::manager::ServersCommand;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;

use anyhow::anyhow;
use chrono::Utc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

pub struct BackgroundCycle {
    config: Config,
    servers_tx: mpsc::Sender<ServersCommand>,
}

impl BackgroundCycle {
    pub fn new(config: Config, servers_tx: mpsc::Sender<ServersCommand>) -> Self {
        BackgroundCycle { config, servers_tx }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        println!(
            "{}{}BackgroundCycle запущен!",
            self.print_time(),
            Fore::YELLOW.as_str(),
        );
        loop {
            self.wait_for_next_interval().await?;

            let (tx, rx) = oneshot::channel();

            let _ = self
                .servers_tx
                .send(ServersCommand::RemoveAllWorkload { respond_to: tx })
                .await;

            rx.await??;

            let (tx, rx) = oneshot::channel();

            let _ = self
                .servers_tx
                .send(ServersCommand::UpdateActive { respond_to: tx })
                .await;

            rx.await??;
        }
    }

    async fn wait_for_next_interval(&self) -> Result<(), anyhow::Error> {
        let timeframe = Timeframe::from_str(&self.config.timeframes.background_timeframe)
            .expect("Invalid timeframe in config!");

        let now = Utc::now();

        match timeframe.seconds() {
            Some(interval) => {
                let now_ts = now.timestamp();
                let next_ts = (((now_ts as f64) / interval) + 1.0) * interval;
                let wait_secs = ((next_ts.round() as i64) - now_ts).max(0) as u64;

                if wait_secs > 0 {
                    sleep(Duration::from_secs(wait_secs)).await;
                }
            }

            None => {
                return Err(anyhow!("invalid timeframe in config"));
            }
        }

        sleep(Duration::from_secs(2)).await;

        if self.config.prints.cycle.cycle_start {
            println!("{}Фоновый цикл запустился", self.print_time());
        }

        Ok(())
    }

    fn print_time(&self) -> String {
        format!(
            "{}[{}] ",
            Fore::WHITE.as_str(),
            Utc::now().format("%H:%M:%S")
        )
    }
}
