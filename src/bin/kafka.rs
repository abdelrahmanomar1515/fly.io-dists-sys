use async_trait::async_trait;
use gossip::{Message, Network, Node, Runtime};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, EchoNode>::run().await
}

#[derive(Clone)]
struct EchoNode {
    network: Network<Payload>,
    logs: Logs,
    offsets: Offsets,
}

#[derive(Clone, Debug)]
struct Logs {
    data: Arc<Mutex<HashMap<String, Vec<usize>>>>,
}

impl Logs {
    fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }
    fn append(&self, key: String, value: usize) -> usize {
        let mut lock = self.data.lock().unwrap();
        let mut len = 0;
        lock.entry(key.clone())
            .and_modify(|v| {
                len = v.len();
                v.push(value)
            })
            .or_insert(vec![value]);
        len
    }

    fn get_from_offset(&self, key: &String, offset: usize) -> Vec<usize> {
        let lock = self.data.lock().unwrap();
        (*lock
            .get(key)
            .unwrap_or(&vec![])
            .iter()
            .skip(offset)
            .copied()
            .collect::<Vec<usize>>())
        .to_vec()
    }
}

#[derive(Clone, Debug)]
struct Offsets {
    data: Arc<Mutex<HashMap<String, usize>>>,
}
impl Offsets {
    fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    fn commit(&self, offsets: &HashMap<String, usize>) {
        let mut lock = self.data.lock().unwrap();
        for (key, offset) in offsets.iter() {
            lock.insert(key.clone(), *offset);
        }
    }

    fn list(&self, keys: &[String]) -> HashMap<String, usize> {
        let lock = self.data.lock().unwrap();
        keys.iter()
            .filter_map(|key| lock.get(key).map(|v| (key.clone(), *v)))
            .collect()
    }
}

impl EchoNode {
    async fn handle_send(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Send { msg, key } = message.get_payload() else {
            panic!("Incorrect message type");
        };
        let offset = self.logs.append(key.clone(), *msg);
        self.network
            .send(message.reply(Payload::SendOk { offset }))
            .await;
        Ok(())
    }
    async fn handle_poll(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Poll { offsets } = message.get_payload() else {
            panic!("Incorrect message type");
        };

        let msgs = offsets
            .iter()
            .map(|(key, offset)| {
                (
                    key.clone(),
                    self.logs
                        .get_from_offset(key, *offset)
                        .iter()
                        .enumerate()
                        .map(|(i, msg)| (offset + i, *msg))
                        .collect(),
                )
            })
            .collect();
        eprintln!("{:?}", self.logs);
        self.network
            .send(message.reply(Payload::PollOk { msgs }))
            .await;
        Ok(())
    }
    async fn handle_commit_offsets(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::CommitOffsets { offsets } = message.get_payload() else {
            panic!("Incorrect message type");
        };
        self.offsets.commit(offsets);
        self.network
            .send(message.reply(Payload::CommitOffsetsOk))
            .await;
        Ok(())
    }
    async fn handle_list_committed_offsets(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::ListCommittedOffsets { keys } = message.get_payload() else {
            panic!("Incorrect message type");
        };
        let offsets = self.offsets.list(keys);
        self.network
            .send(message.reply(Payload::ListCommittedOffsetsOk { offsets }))
            .await;
        Ok(())
    }
}

#[async_trait]
impl Node<Payload> for EchoNode {
    fn from_init(_id: String, _neighbors: Vec<String>, network: Network<Payload>) -> Self {
        Self {
            network,
            logs: Logs::new(),
            offsets: Offsets::new(),
        }
    }

    async fn handle_message(&self, message: Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Send { .. } => self.handle_send(message).await?,
            Payload::Poll { .. } => self.handle_poll(message).await?,
            Payload::CommitOffsets { .. } => self.handle_commit_offsets(message).await?,
            Payload::ListCommittedOffsets { .. } => {
                self.handle_list_committed_offsets(message).await?
            }
            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => {
                eprintln!("Received unexpected message {message:?}");
            }
        };

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        msg: usize,
        key: String,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}
