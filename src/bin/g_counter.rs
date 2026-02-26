use async_trait::async_trait;
use gossip::{Message, Network, Node, Runtime};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, GCounterNode>::run().await
}

#[derive(Clone)]
struct GCounterNode {
    node_id: String,
    known_values: Arc<Mutex<HashMap<String, usize>>>,
    network: Network<Payload>,
    msg_id: Arc<AtomicUsize>,
    current_value: Arc<AtomicUsize>,
}

impl GCounterNode {
    fn get_id(&mut self) -> usize {
        self.msg_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn store_compare_and_swap(&self, key: &str, from: usize, to: usize) {
        let msg_id = self.clone().get_id();
        let msg = Message::new(
            self.node_id.clone(),
            "seq-kv".into(),
            Payload::SeqKvCas {
                key: key.to_string(),
                from,
                to,
                create_if_not_exists: true,
            },
        )
        .with_id(msg_id);
        let _ = self.network.rpc(msg).await;
    }

    fn read_others(self, all_nodes: Vec<String>) {
        tokio::spawn(async move {
            loop {
                let rng = rand::rng().random_range(0..100);
                tokio::time::sleep(Duration::from_millis(1000 + rng)).await;
                for node in &all_nodes {
                    let mut s = self.clone();
                    let node = node.clone();
                    tokio::spawn(async move {
                        let msg_id = s.get_id();
                        let msg = Message::new(
                            s.node_id.clone(),
                            "seq-kv".into(),
                            Payload::SeqKvRead { key: node.clone() },
                        )
                        .with_id(msg_id);
                        let Ok(message) = s.network.rpc(msg).await else {
                            eprintln!("Couldn't send msg");
                            return;
                        };
                        let Payload::ReadOk { value } = message.body.payload else {
                            eprintln!("Message type must be read_ok and have a value");
                            return;
                        };

                        s.known_values
                            .lock()
                            .expect("Lock")
                            .insert(node.clone(), value);
                    });
                }
            }
        });
    }

    async fn handle_add(&self, msg: &Message<Payload>, delta: usize) -> anyhow::Result<()> {
        if delta == 0 {
            self.network.send(msg.reply(Payload::AddOk)).await;
            return Ok(());
        }

        let from = self.current_value.load(Ordering::SeqCst);
        let to = from + delta;
        self.current_value
            .compare_exchange(from, to, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|e| {
                eprintln!("Error saving new value for node");
                anyhow::anyhow!("Error saving new value for node: {e}")
            })?;

        self.store_compare_and_swap(&self.node_id.clone(), from, to)
            .await;
        self.network.send(msg.reply(Payload::AddOk)).await;
        Ok(())
    }

    async fn handle_read(&self, msg: &Message<Payload>) -> anyhow::Result<()> {
        let total: usize = self.known_values.lock().expect("Lock").values().sum();
        self.network
            .send(msg.reply(Payload::ReadOk { value: total }))
            .await;
        Ok(())
    }
}

#[async_trait]
impl Node<Payload> for GCounterNode {
    fn from_init(id: String, neighbors: Vec<String>, network: Network<Payload>) -> Self {
        let node = Self {
            network: network.clone(),
            node_id: id.clone(),
            known_values: Arc::new(Mutex::new(
                neighbors
                    .clone()
                    .into_iter()
                    .map(|node| (node, 0))
                    .collect(),
            )),
            msg_id: Arc::new(AtomicUsize::new(0)),
            current_value: Arc::new(AtomicUsize::new(0)),
        };
        node.clone().read_others(neighbors);
        node
    }

    async fn handle_message(&self, message: Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Add { delta } => self
                .handle_add(&message, *delta)
                .await
                .inspect_err(|e| eprintln!("Got error on add: {e}")),
            Payload::Read => self
                .handle_read(&message)
                .await
                .inspect_err(|e| eprintln!("Got error on read: {e}")),
            Payload::Error {
                code,
                text,
                in_reply_to,
            } => {
                eprintln!(
                    "Got error message with code: {code}, {text}, in reply to: {in_reply_to:?}"
                );
                Ok(())
            }
            Payload::ReadOk { .. }
            | Payload::AddOk
            | Payload::SeqKvRead { .. }
            | Payload::SeqKvWrite { .. }
            | Payload::SeqKvWriteOk
            | Payload::SeqKvCas { .. }
            | Payload::SeqKvCasOk => {
                eprintln!("Unhandled message type: {message:?}");
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },

    #[serde(rename = "read")]
    SeqKvRead {
        key: String,
    },
    #[serde(rename = "write")]
    SeqKvWrite {
        key: String,
        value: usize,
    },
    #[serde(rename = "write_ok")]
    SeqKvWriteOk,
    #[serde(rename = "cas")]
    SeqKvCas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
    #[serde(rename = "cas_ok")]
    SeqKvCasOk,

    Error {
        code: usize,
        text: String,
        in_reply_to: Option<usize>,
    },
}
