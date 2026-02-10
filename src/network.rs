use std::{
    collections::HashMap,
    sync::mpsc::{channel, Sender},
};

use crate::{Message, Payload};

#[derive(Clone)]
pub struct Network<TPayload>
where
    TPayload: Payload,
{
    outbound: Sender<Message<TPayload>>,
    pending: HashMap<usize, Sender<Message<TPayload>>>,
}
impl<TPayload> Network<TPayload>
where
    TPayload: Payload,
{
    pub fn new(outbound: Sender<Message<TPayload>>) -> Self {
        Self {
            outbound,
            pending: Default::default(),
        }
    }

    pub fn send(&self, msg: Message<TPayload>) {
        self.outbound.send(msg).expect("Unable to send message")
    }

    pub fn rpc(&mut self, msg: Message<TPayload>) -> anyhow::Result<Message<TPayload>> {
        let msg_id = msg
            .body
            .msg_id
            .expect("Message id must be set for rpc calls");
        self.send(msg);

        let (tx, rx) = channel();
        self.pending.insert(msg_id, tx);

        let response = rx.recv()?;
        self.pending.remove(&msg_id);
        Ok(response)
    }

    pub fn get_reply_channel(&self, msg_id: usize) -> Option<&Sender<Message<TPayload>>> {
        self.pending.get(&msg_id)
    }
}
