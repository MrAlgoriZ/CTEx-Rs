use super::{mpsc, oneshot};
use crate::engine::state::chain::{Block, Chain};

use anyhow::Result;
use log::info;

pub enum ChainCommand {
    AddBlock {
        symbol: String,
        block: Block,
        respond_to: oneshot::Sender<()>,
    },
    DeleteChain {
        symbol: String,
        respond_to: oneshot::Sender<()>,
    },
    SavePlots {
        symbol: String,
        respond_to: oneshot::Sender<Result<()>>,
    },
}

pub struct ChainActor {
    chains: Chain,
    inbox: mpsc::Receiver<ChainCommand>,
}

impl ChainActor {
    pub fn new() -> (Self, mpsc::Sender<ChainCommand>) {
        let (tx, rx) = mpsc::channel(1000);

        let chains = Chain::new();

        (Self { chains, inbox: rx }, tx)
    }

    pub async fn run(mut self) {
        info!("ChainActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ChainCommand::AddBlock {
                    respond_to,
                    symbol,
                    block,
                } => {
                    let result = self.chains.add_block(&symbol, block);
                    let _ = respond_to.send(result);
                }
                ChainCommand::DeleteChain { symbol, respond_to } => {
                    let result = self.chains.delete_chain(&symbol);
                    let _ = respond_to.send(result);
                }
                ChainCommand::SavePlots { symbol, respond_to } => {
                    let result = self.chains.save_plots(&symbol);
                    let _ = respond_to.send(result);
                }
            }
        }
    }
}
