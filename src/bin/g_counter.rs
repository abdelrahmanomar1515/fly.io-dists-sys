use anyhow::bail;
use gossip::{Message, Network, Node, Runtime};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

fn main() -> anyhow::Result<()> {
    Runtime::<Payload, GCounterNode>::run()
}

struct GCounterNode {
    node_id: String,
    msg_id_to_node: Arc<Mutex<HashMap<usize, String>>>,
    known_values: HashMap<String, usize>,
    network: Network<Payload>,
    cache: usize,
    msg_id: usize,
}

impl GCounterNode {
    fn get_id(&mut self) -> usize {
        self.msg_id += 1;
        self.msg_id
    }

    fn store_compare_and_swap(&mut self, key: &str, from: usize, to: usize) {
        let msg_id = self.get_id();
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
        self.network.send(msg)
    }

    fn read_others(
        node_id: String,
        all_nodes: Vec<String>,
        network: Network<Payload>,
        msg_id_to_node_id: Arc<Mutex<HashMap<usize, String>>>,
    ) {
        thread::spawn(move || loop {
            let rng = rand::rng().random_range(0..100);
            thread::sleep(Duration::from_millis(1000 + rng));
            for node in &all_nodes {
                let msg_id = rand::rng().random_range(1..99999999999);
                let msg = Message::new(
                    node_id.clone(),
                    "seq-kv".into(),
                    Payload::SeqKvRead { key: node.clone() },
                )
                .with_id(msg_id);
                msg_id_to_node_id
                    .lock()
                    .expect("Can't get lock")
                    .insert(msg_id, node.clone());
                eprintln!("Added entry {msg_id}: {node}");
                network.send(msg);
            }
        });
    }

    fn handle_add(&mut self, msg: &Message<Payload>, delta: usize) -> anyhow::Result<()> {
        if delta == 0 {
            return Ok(());
        }
        let from = self.cache;
        let to = self.cache + delta;
        self.cache += delta;

        self.store_compare_and_swap(&self.node_id.clone(), from, to);
        self.network.send(msg.reply(Payload::AddOk));
        Ok(())
    }

    fn handle_read(&mut self, msg: &Message<Payload>) -> anyhow::Result<()> {
        eprintln!("{:?}", self.known_values);
        let total: usize = self.known_values.values().sum();
        self.network
            .send(msg.reply(Payload::ReadOk { value: total }));
        eprintln!("Returned read result as {total}");
        Ok(())
    }

    fn handle_read_ok(&mut self, message: &Message<Payload>) -> anyhow::Result<()> {
        eprintln!("Current map: {:?}", self.msg_id_to_node);
        let Some(reply_msg_id) = message.body.in_reply_to else {
            eprintln!("in_reply_to should exist");
            bail!("in_reply_to should exist");
        };

        let mut lock = self.msg_id_to_node.lock().expect("Can't get lock again");
        let Some(node_id) = lock.remove(&reply_msg_id) else {
            bail!("Got reply of a message that we don't know which node it corresponds to");
        };
        let Payload::ReadOk { value } = message.body.payload else {
            eprintln!("Message type must be read_ok and have a value");
            bail!("Message type must be read_ok and have a value");
        };
        eprintln!("Got read_ok for msg_id {reply_msg_id} for node {node_id} with value {value}");

        eprintln!("Setting value {:?}", self.known_values);
        self.known_values.insert(node_id.clone(), value);
        Ok(())
    }
}

impl Node<Payload> for GCounterNode {
    fn from_init(id: String, neighbors: Vec<String>, network: Network<Payload>) -> Self {
        let msg_id_to_node: Arc<Mutex<HashMap<usize, String>>> = Default::default();
        GCounterNode::read_others(
            id.clone(),
            neighbors.clone(),
            network.clone(),
            msg_id_to_node.clone(),
        );
        Self {
            network,
            node_id: id,
            known_values: neighbors.into_iter().map(|node| (node, 0)).collect(),
            msg_id_to_node,
            cache: 0,
            msg_id: 0,
        }
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Add { delta } => self
                .handle_add(&message, *delta)
                .inspect_err(|e| eprintln!("Got error on add: {e}")),
            Payload::Read => self
                .handle_read(&message)
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
            Payload::ReadOk { .. } => self.handle_read_ok(&message),
            Payload::AddOk
            | Payload::SeqKvRead { .. }
            | Payload::SeqKvWrite { .. }
            | Payload::SeqKvWriteOk
            | Payload::SeqKvCas { .. }
            | Payload::SeqKvCasOk => {
                eprintln!("Unhandeld message type: {message:?}");
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
