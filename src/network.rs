use std::{
    collections::HashMap,
    sync::{
        mpsc::{channel, RecvTimeoutError, Sender},
        Arc, Mutex,
    },
    time::Duration,
};

use crate::{Message, Payload};

#[derive(Clone, Debug)]
pub struct Network<TPayload>
where
    TPayload: Payload,
{
    outbound: Sender<Message<TPayload>>,
    pending: Arc<Mutex<HashMap<usize, Sender<Message<TPayload>>>>>,
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

    pub fn rpc(
        &mut self,
        msg: Message<TPayload>,
        timeout: Duration,
    ) -> Result<Message<TPayload>, RpcError> {
        let msg_id = msg
            .body
            .msg_id
            .expect("Message id must be set for rpc calls");

        let (tx, rx) = channel();
        {
            self.pending
                .lock()
                .expect("Unable to get lock over pending map")
                .insert(msg_id, tx);
            let p = &self.pending;
            eprintln!("still pending {p:?}");
        }
        self.send(msg);
        let response = rx.recv_timeout(timeout).map_err(|e| match e {
            RecvTimeoutError::Timeout => {
                {
                    self.pending
                        .lock()
                        .expect("Unable to get lock over pending map")
                        .remove(&msg_id);
                }
                RpcError::Timeout
            }
            RecvTimeoutError::Disconnected => panic!("Unable to receive response of rpc call"),
        })?;
        Ok(response)
    }

    pub fn get_reply_channel(&self, msg_id: &usize) -> Option<Sender<Message<TPayload>>> {
        self.pending
            .lock()
            .expect("Unable to get lock over pending map")
            .remove(msg_id)
    }
}

pub enum RpcError {
    Timeout,
}
