use crate::{Message, Payload};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{
    mpsc::Sender as MpscSender,
    oneshot::{self, Sender},
};

#[derive(Clone, Debug)]
pub struct Network {
    outbound: MpscSender<String>,
    pending: Arc<Mutex<HashMap<usize, Sender<String>>>>,
    next_id: Arc<AtomicUsize>,
}
impl Network {
    pub fn new(outbound: MpscSender<String>) -> Self {
        Self {
            outbound,
            pending: Default::default(),
            next_id: Default::default(),
        }
    }

    fn get_next_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn send_raw(&self, msg: String) {
        self.outbound
            .send(msg)
            .await
            .expect("Unable to send message")
    }

    pub async fn send<TPayload: Payload>(&self, msg: &Message<TPayload>) {
        let mut msg = msg.clone();
        let msg_id = self.get_next_id();
        let msg = msg.with_id(msg_id);
        let json = serde_json::to_string(msg).expect("Should be able to serialize message");
        self.send_raw(json).await
    }
    pub async fn send2<TPayload: Payload>(&self, msg: &Message<TPayload>) {
        let json = serde_json::to_string(msg).expect("Should be able to serialize message");
        self.send_raw(json).await
    }

    pub async fn rpc<TPayload: Payload>(
        &self,
        msg: &mut Message<TPayload>,
    ) -> anyhow::Result<Message<TPayload>> {
        let msg_id = self.get_next_id();
        let msg = msg.with_id(msg_id);

        let (tx, rx) = oneshot::channel::<String>();

        self.pending
            .lock()
            .expect("Unable to get lock over pending map")
            .insert(msg_id, tx);

        self.send2(msg).await;
        let response = rx.await.unwrap();
        let r = serde_json::from_str(&response)
            .unwrap_or_else(|_| panic!("Should be able to deserialize message {response}"));
        Ok(r)
    }

    pub fn get_reply_channel(&self, msg_id: &usize) -> Option<Sender<String>> {
        self.pending
            .lock()
            .expect("Unable to get lock over pending map")
            .remove(msg_id)
    }
}
