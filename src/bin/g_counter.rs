use gossip::{KeyValueStore, Message, Network, Node, Runtime, Storage};
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
    network: Network,
    current_value: Arc<AtomicUsize>,
    storage: KeyValueStore<usize>,
}

impl GCounterNode {
    fn read_others(self, all_nodes: Vec<String>) {
        tokio::spawn(async move {
            loop {
                let rng = rand::rng().random_range(0..100);
                tokio::time::sleep(Duration::from_millis(1000 + rng)).await;
                for node in &all_nodes {
                    let s = self.clone();
                    let node = node.clone();
                    tokio::spawn(async move {
                        let value = s.storage.get(node.clone()).await;
                        match value {
                            Ok(v) => {
                                s.known_values.lock().expect("Lock").insert(node, v);
                            }
                            Err(_) => {
                                eprintln!("Found some error ");
                            }
                        };
                    });
                }
            }
        });
    }

    async fn handle_add(&self, msg: &Message<Payload>, delta: usize) -> anyhow::Result<()> {
        if delta == 0 {
            self.network.send(&msg.reply(Payload::AddOk)).await;
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

        self.clone()
            .storage
            .cas(self.node_id.clone(), from, to)
            .await?;
        self.network.send(&msg.reply(Payload::AddOk)).await;
        Ok(())
    }

    async fn handle_read(&self, msg: &Message<Payload>) -> anyhow::Result<()> {
        let total: usize = self.known_values.lock().expect("Lock").values().sum();
        self.network
            .send(&msg.reply(Payload::ReadOk { value: total }))
            .await;
        Ok(())
    }
}

impl Node<Payload> for GCounterNode {
    fn from_init(id: String, neighbors: Vec<String>, network: Network) -> Self {
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
            current_value: Arc::new(AtomicUsize::new(0)),
            storage: KeyValueStore::new("seq-kv", network.clone(), id),
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
            Payload::ReadOk { .. } | Payload::AddOk => {
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
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
}
