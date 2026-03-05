use gossip::{Message, Network, Node, Runtime};
use serde::{ser::SerializeTuple, Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, TxnNode>::run().await
}

#[derive(Clone)]
struct StoreActorHandle {
    sender: mpsc::Sender<StoreMessage>,
}
impl StoreActorHandle {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(store_actor(rx));
        Self { sender: tx }
    }

    async fn read(&self, key: usize) -> Option<usize> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreMessage::Read { key, reply: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }
    async fn write(&self, key: usize, value: usize) {
        self.sender
            .send(StoreMessage::Write { key, value })
            .await
            .unwrap();
    }
}

enum StoreMessage {
    Read {
        key: usize,
        reply: oneshot::Sender<Option<usize>>,
    },
    Write {
        key: usize,
        value: usize,
    },
}

async fn store_actor(mut rx: mpsc::Receiver<StoreMessage>) {
    let mut store: HashMap<usize, usize> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            StoreMessage::Read { key, reply } => {
                let value = store.get(&key);
                reply.send(value.copied()).unwrap();
            }
            StoreMessage::Write { key, value } => {
                store.insert(key, value);
            }
        }
    }
}

#[derive(Clone)]
struct TxnNode {
    network: Network,
    store: StoreActorHandle,
}

impl Node<Payload> for TxnNode {
    fn from_init(_id: String, _neighbors: Vec<String>, network: Network) -> Self {
        let store = StoreActorHandle::new();
        Self { network, store }
    }

    async fn handle_message(&self, message: Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Txn { txn } => {
                let mut reply_txn = vec![];
                for op in txn {
                    match op {
                        Op::Read { key, .. } => {
                            let v = self.store.read(*key).await;
                            reply_txn.push(Op::Read {
                                key: *key,
                                value: v,
                            });
                        }
                        Op::Write { key, value } => {
                            self.store.write(*key, *value).await;
                            reply_txn.push(Op::Write {
                                key: *key,
                                value: *value,
                            });
                        }
                    }
                }
                let reply = message.reply(Payload::TxnOk { txn: reply_txn });
                self.network.send(&reply).await
            }
            Payload::TxnOk { .. } => {
                eprintln!("Received unexpected message: {message:?}");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<Op> },
    TxnOk { txn: Vec<Op> },
}

#[derive(Debug, Clone)]
enum Op {
    Read { key: usize, value: Option<usize> },
    Write { key: usize, value: usize },
}

impl Serialize for Op {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut tuple = serializer.serialize_tuple(3)?;
        match self {
            Op::Read { key, value } => {
                tuple.serialize_element(&'r')?;
                tuple.serialize_element(key)?;
                tuple.serialize_element(value)?;
                tuple.end()
            }
            Op::Write { key, value } => {
                tuple.serialize_element(&'w')?;
                tuple.serialize_element(key)?;
                tuple.serialize_element(value)?;
                tuple.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let tuple: (char, usize, Option<usize>) = Deserialize::deserialize(deserializer)?;
        match tuple.0 {
            'r' => Ok(Op::Read {
                key: tuple.1,
                value: tuple.2,
            }),
            'w' => Ok(Op::Write {
                key: tuple.1,
                value: tuple.2.unwrap(),
            }),
            _ => panic!("Got unexpected op"),
        }
    }
}
