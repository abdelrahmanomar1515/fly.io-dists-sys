use gossip::{retry, KeyValueStore, Message, Network, Node, RpcError, Runtime, Storable, Storage};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, KafkaNode>::run().await
}

#[derive(Clone)]
struct KafkaNode {
    network: Network,
    logs: Logs,
    offsets: Offsets,
}

struct CacheActor<T: Storable> {
    incoming: mpsc::Receiver<CacheMessage<T>>,
    store: HashMap<String, T>,
}

impl<T: Storable + Default> CacheActor<T> {
    async fn start(mut self) {
        while let Some(message) = self.incoming.recv().await {
            match message {
                CacheMessage::Store { key, value } => {
                    self.store.insert(key, value);
                }
                CacheMessage::Get { key, answer } => {
                    let r = self.store.get(&key).cloned().unwrap_or(Default::default());
                    answer.send(r).expect("Should be able to return answer");
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct CacheHandle<T: Storable> {
    sender: mpsc::Sender<CacheMessage<T>>,
}
impl<T: Storable + Default> CacheHandle<T> {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(10);
        let cache = CacheActor {
            incoming: rx,
            store: Default::default(),
        };
        tokio::spawn(async move {
            cache.start().await;
        });

        Self { sender: tx }
    }

    async fn get(&self, key: &str) -> T {
        let (tx, rx) = oneshot::channel();
        let msg = CacheMessage::Get {
            key: key.to_owned(),
            answer: tx,
        };
        self.sender
            .send(msg)
            .await
            .expect("Actor should be able to receive message");
        rx.await
            .expect("Should be able to receive one shot message")
    }

    async fn set(&self, key: String, value: T) {
        let msg = CacheMessage::Store { key, value };
        self.sender
            .send(msg)
            .await
            .expect("Actor should be able to receive message");
    }
}

enum CacheMessage<T> {
    Store {
        key: String,
        value: T,
    },
    Get {
        key: String,
        answer: oneshot::Sender<T>,
    },
}

#[derive(Clone, Debug)]
struct Logs {
    storage: KeyValueStore<Vec<usize>>,
    cache: CacheHandle<Vec<usize>>,
}

impl Logs {
    fn new(network: Network, node_id: String) -> Self {
        let storage = KeyValueStore::new("seq-kv", network.clone(), node_id);
        Self {
            storage: storage.clone(),
            cache: CacheHandle::new(),
        }
    }
    async fn append(&self, key: String, value: usize) -> anyhow::Result<(usize, Vec<usize>)> {
        let log_key = self.to_log_key(key.clone());
        let current_log = self.get(&key).await?;
        let offset = current_log.len();
        let mut new_log = current_log.clone();
        new_log.push(value);
        self.storage
            .cas(log_key, current_log, new_log.clone())
            .await?;
        Ok((offset, new_log))
    }
    async fn try_append(&self, key: String, value: usize) -> usize {
        let log_key = self.to_log_key(key.clone());
        let current_log = self.get_cached(&key).await;
        let offset = current_log.len();
        let mut new_log = current_log.clone();
        new_log.push(value);
        let (offset, new_log) = match self
            .storage
            .cas(log_key, current_log, new_log.clone())
            .await
        {
            Ok(_) => (offset, new_log),
            Err(_) => retry(
                || self.append(key.clone(), value),
                10,
                Duration::from_millis(1),
            )
            .await
            .expect("Couldn't commit after 10 retries"),
        };
        self.cache.set(key, new_log).await;
        offset
    }
    async fn get_from_offset(&self, key: &str, offset: usize) -> anyhow::Result<Vec<usize>> {
        let current_log = self.get_cached(key).await;
        Ok(current_log.iter().skip(offset).copied().collect())
    }
    async fn get_cached(&self, key: &str) -> Vec<usize> {
        self.cache.get(key).await
    }
    async fn get(&self, key: &str) -> anyhow::Result<Vec<usize>> {
        Ok(self
            .storage
            .get(self.to_log_key(key.to_owned()))
            .await
            .or_else(|e| match e {
                RpcError::KeyDoesNotExist => Ok(vec![]),
                e => Err(e),
            })?)
    }
    fn to_log_key(&self, key: String) -> String {
        "log-".to_owned() + &key
    }
}

#[derive(Clone, Debug)]
struct Offsets {
    storage: KeyValueStore<usize>,
}
impl Offsets {
    fn new(network: Network, node_id: String) -> Self {
        Self {
            storage: KeyValueStore::new("seq-kv", network, node_id),
        }
    }

    async fn commit(&self, key: String, offset: usize) -> anyhow::Result<()> {
        let current = self.get(&key).await?;
        self.storage.cas(key, current, offset).await?;
        Ok(())
    }

    async fn list(&self, keys: &[String]) -> anyhow::Result<HashMap<String, usize>> {
        let mut map = HashMap::new();
        for key in keys {
            let value = self.get(key).await?;
            if value != 0 {
                map.insert(key.clone(), value);
            }
        }
        Ok(map)
    }
    async fn get(&self, key: &str) -> anyhow::Result<usize> {
        Ok(self
            .storage
            .get(key.to_owned())
            .await
            .or_else(|e| match e {
                RpcError::KeyDoesNotExist => Ok(0),
                e => Err(e),
            })?)
    }
}

impl KafkaNode {
    async fn handle_send(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Send { msg, key } = message.get_payload() else {
            panic!("Incorrect message type");
        };
        let offset = self.logs.try_append(key.clone(), *msg).await;
        self.network
            .send(&message.reply(Payload::SendOk { offset }))
            .await;
        Ok(())
    }
    async fn handle_poll(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Poll { offsets } = message.get_payload() else {
            panic!("Incorrect message type");
        };

        let mut msgs: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
        for (key, offset) in offsets {
            let logs = self.logs.get_from_offset(key, *offset).await?;

            msgs.insert(
                key.clone(),
                logs.iter()
                    .enumerate()
                    .map(|(i, msg)| (offset + i, *msg))
                    .collect(),
            );
        }
        self.network
            .send(&message.reply(Payload::PollOk { msgs }))
            .await;
        Ok(())
    }
    async fn handle_commit_offsets(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::CommitOffsets { offsets } = message.get_payload() else {
            panic!("Incorrect message type");
        };

        for (key, offset) in offsets {
            retry(
                async || self.offsets.commit(key.clone(), *offset).await,
                10,
                Duration::from_millis(1),
            )
            .await?;
        }
        self.network
            .send(&message.reply(Payload::CommitOffsetsOk))
            .await;
        Ok(())
    }
    async fn handle_list_committed_offsets(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let Payload::ListCommittedOffsets { keys } = message.get_payload() else {
            panic!("Incorrect message type");
        };
        let offsets = self.offsets.list(keys).await?;
        self.network
            .send(&message.reply(Payload::ListCommittedOffsetsOk { offsets }))
            .await;
        Ok(())
    }
}

impl Node<Payload> for KafkaNode {
    fn from_init(id: String, _neighbors: Vec<String>, network: Network) -> Self {
        Self {
            network: network.clone(),
            logs: Logs::new(network.clone(), id.clone()),
            offsets: Offsets::new(network.clone(), id),
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
