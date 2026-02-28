use crate::{Message, Payload};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{
    mpsc::Sender as MpscSender,
    oneshot::{self, Sender},
};

#[derive(Clone, Debug)]
pub struct Network<TPayload>
where
    TPayload: Payload,
{
    outbound: MpscSender<Message<TPayload>>,
    pending: Arc<Mutex<HashMap<usize, Sender<Message<TPayload>>>>>,
}
impl<TPayload> Network<TPayload>
where
    TPayload: Payload,
{
    pub fn new(outbound: MpscSender<Message<TPayload>>) -> Self {
        Self {
            outbound,
            pending: Default::default(),
        }
    }

    pub async fn send(&self, msg: Message<TPayload>) {
        self.outbound
            .send(msg)
            .await
            .expect("Unable to send message")
    }

    pub async fn rpc(&self, msg: Message<TPayload>) -> Result<Message<TPayload>, RpcError> {
        let msg_id = msg
            .body
            .msg_id
            .expect("Message id must be set for rpc calls");

        let (tx, rx) = oneshot::channel::<Message<TPayload>>();

        self.pending
            .lock()
            .expect("Unable to get lock over pending map")
            .insert(msg_id, tx);

        self.send(msg).await;
        let response = rx
            .await
            .map_err(|_e| panic!("Unable to receive response of rpc call"))?;
        Ok(response)
    }

    pub fn get_reply_channel(&self, msg_id: &usize) -> Option<Sender<Message<TPayload>>> {
        self.pending
            .lock()
            .expect("Unable to get lock over pending map")
            .remove(msg_id)
    }
}

#[derive(Debug)]
pub enum RpcError {
    Timeout,
}
