use super::{mpsc, oneshot};
use crate::data::data_interfaces::DataMap;
use crate::models::model::Model;

use anyhow::anyhow;
use log::{error, info, warn};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub enum ModelCommand {
    Predict {
        data: DataMap,
        respond_to: oneshot::Sender<DataMap>,
    },
    Train {
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    HandleMistakes {
        true_data: DataMap,
        predicted_data: DataMap,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    GetAccuracy {
        respond_to: oneshot::Sender<Option<DataMap>>,
    },
}

pub struct ModelActor {
    model: Arc<Mutex<Box<dyn Model + Send + Sync>>>,
    inbox: mpsc::Receiver<ModelCommand>,
}

impl ModelActor {
    pub fn new(model: Box<dyn Model + Send + Sync>) -> (Self, mpsc::Sender<ModelCommand>) {
        let (tx, rx) = mpsc::channel(10);
        (
            Self {
                model: Arc::new(Mutex::new(model)),
                inbox: rx,
            },
            tx,
        )
    }

    pub async fn run(mut self) {
        info!("ModelActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ModelCommand::Predict { data, respond_to } => {
                    // debug!("{:#?}", &data);
                    let model = self.model.clone();
                    let result = model.lock().await.predict(data).await;

                    let prediction = match result {
                        Ok(pred) => pred,
                        Err(e) => {
                            error!("Prediction error: {}", e);
                            DataMap::new("".to_string(), BTreeMap::new())
                        }
                    };

                    let _ = respond_to.send(prediction);
                }

                ModelCommand::Train { respond_to } => {
                    let result = {
                        let model = self.model.clone();

                        let mut locked = model.lock().await;
                        locked.train().await
                    };

                    match result {
                        Ok(_) => {
                            let _ = respond_to.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = respond_to.send(Err(e));
                        }
                    }
                }

                ModelCommand::HandleMistakes {
                    true_data,
                    predicted_data,
                    respond_to,
                } => {
                    let result = {
                        if true_data.is_empty() {
                            Err(anyhow!("True data is empty!"))
                        } else if predicted_data.is_empty() {
                            Err(anyhow!("Predicted data is empty!"))
                        } else if true_data.len() != predicted_data.len() {
                            Err(anyhow!("Data sizes do not match!"))
                        } else {
                            let model = self.model.clone();

                            let mut locked = model.lock().await;
                            locked.handle_mistakes(true_data, predicted_data).await
                        }
                    };

                    let _ = respond_to.send(result);
                }

                ModelCommand::GetAccuracy { respond_to } => {
                    let result = {
                        let model = self.model.clone();
                        let locked = model.lock().await;
                        locked.get_accuracy()
                    };

                    let _ = respond_to.send(result);
                }
            }
        }

        warn!("ModelActor has stopped!");
    }
}
