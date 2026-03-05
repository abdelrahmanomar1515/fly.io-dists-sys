use gossip::{Message, Network, Node, Runtime};
use serde::{ser::SerializeTuple, Deserialize, Serialize};
use std::{collections::HashMap, thread, vec};
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, TxnNode>::run().await
}

#[derive(Clone)]
struct StoreActorHandle {
    store_sender: mpsc::Sender<StoreMessage>,
    history_sender: mpsc::Sender<Txn>,
}
impl StoreActorHandle {
    fn new(network: Network, neighbors: Vec<String>, node_id: String) -> Self {
        let (store_tx, store_rx) = mpsc::channel(100);
        let (txn_tx, txn_rx) = mpsc::channel(100);
        let handle = Self {
            store_sender: store_tx,
            history_sender: txn_tx,
        };

        tokio::spawn(store_actor(store_rx));

        tokio::spawn(replicator_actor(txn_rx, network, neighbors, node_id));

        handle
    }

    async fn commit(&self, txn: Txn) -> Txn {
        let (tx, rx) = oneshot::channel();
        self.store_sender
            .send(StoreMessage::Commit { txn, reply: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    fn print(&self) {
        let s = self.clone();
        thread::spawn(move || {
            s.store_sender.blocking_send(StoreMessage::Print).unwrap();
        });
    }
}

enum StoreMessage {
    Commit {
        txn: Txn,
        reply: oneshot::Sender<Txn>,
    },
    Print,
}

async fn store_actor(mut rx: mpsc::Receiver<StoreMessage>) {
    let mut store: HashMap<usize, usize> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            StoreMessage::Print => {
                eprintln!("Store value: {store:?}");
            }
            StoreMessage::Commit { txn, reply } => {
                let mut result = vec![];
                for op in txn {
                    match op {
                        Op::Read { key, .. } => {
                            result.push(Op::Read {
                                key,
                                value: store.get(&key).copied(),
                            });
                        }
                        write @ Op::Write { key, value } => {
                            result.push(write);
                            store.insert(key, value);
                        }
                    }
                }
                reply.send(result).unwrap();
            }
        }
    }
}

async fn replicator_actor(
    mut rx: mpsc::Receiver<Txn>,
    network: Network,
    neighbors: Vec<String>,
    node_id: String,
) {
    while let Some(txn) = rx.recv().await {
        for node in &neighbors {
            if *node == node_id {
                continue;
            }
            let msg = Message::new(
                node_id.clone(),
                node.clone(),
                Payload::Replicate { txn: txn.clone() },
            );
            network.send(&msg).await;
        }
    }
}

#[derive(Clone)]
struct TxnNode {
    network: Network,
    store: StoreActorHandle,
}

impl Node<Payload> for TxnNode {
    fn from_init(id: String, neighbors: Vec<String>, network: Network) -> Self {
        let store = StoreActorHandle::new(network.clone(), neighbors, id);
        Self { network, store }
    }

    async fn handle_message(&self, message: Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Txn { txn } => {
                self.store.history_sender.send(txn.clone()).await.unwrap();
                let reply_txn = self.store.commit(txn.clone()).await;
                let reply = message.reply(Payload::TxnOk { txn: reply_txn });
                self.network.send(&reply).await
            }
            Payload::Replicate { txn } => {
                self.store.commit(txn.clone()).await;
            }
            Payload::ReplicateOk | Payload::TxnOk { .. } => {
                eprintln!("Received unexpected message: {message:?}");
            }
        }
        Ok(())
    }
}

impl Drop for TxnNode {
    fn drop(&mut self) {
        self.store.print();
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Txn },
    TxnOk { txn: Txn },
    Replicate { txn: Txn },
    ReplicateOk,
}

type Txn = Vec<Op>;

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
