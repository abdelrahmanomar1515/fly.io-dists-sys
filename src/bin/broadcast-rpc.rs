use async_trait::async_trait;
use core::panic;
use gossip::{Message, Network, Node, RpcError, Runtime};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, BoradcastNode>::run().await
}

struct BoradcastNode {
    messages: Arc<Mutex<HashSet<usize>>>,
    network: Network<Payload>,
}

#[async_trait]
impl Node<Payload> for BoradcastNode {
    fn from_init(id: String, neighbors: Vec<String>, network: Network<Payload>) -> Self {
        let messages: Arc<Mutex<HashSet<usize>>> = Default::default();
        let known_messages: Arc<Mutex<HashMap<String, HashSet<usize>>>> = Default::default();

        BoradcastNode::gossip(
            id,
            neighbors,
            messages.clone(),
            known_messages.clone(),
            network.clone(),
        );

        Self { messages, network }
    }

    async fn handle_message(&self, msg: Message<Payload>) -> anyhow::Result<()> {
        match msg.get_payload() {
            Payload::Broadcast { .. } => self.handle_broadcast(msg)?,
            Payload::Read => self.handle_read(msg)?,
            Payload::Gossip { .. } => self.handle_gossip(msg)?,
            // Payload::GossipOk { .. } => self.handle_gossip_ok(msg)?,
            Payload::Topology { .. } => self.handle_topology(msg)?,
            Payload::ReadOk { .. }
            | Payload::BroadcastOk
            | Payload::TopologyOk
            | Payload::GossipOk { .. } => {
                panic!("got invalid message")
            }
        }
        Ok(())
    }
}

impl BoradcastNode {
    fn gossip(
        node_id: String,
        all_nodes: Vec<String>,
        messages: Arc<Mutex<HashSet<usize>>>,
        known_messages: Arc<Mutex<HashMap<String, HashSet<usize>>>>,
        network: Network<Payload>,
    ) {
        tokio::spawn(async move {
            loop {
                let rng = rand::rng().random_range(0..100);
                tokio::time::sleep(Duration::from_millis(10 + rng)).await;
                for dest_id in all_nodes
                    .clone()
                    .into_iter()
                    .filter(|node| node_id != **node)
                {
                    let empty_set = HashSet::new();

                    let messages = {
                        let messages_known_to_node =
                            known_messages.lock().expect("Can't lock known messages");
                        let messages_known_to_node =
                            messages_known_to_node.get(&dest_id).unwrap_or(&empty_set);
                        let messages: HashSet<usize> = {
                            messages
                                .lock()
                                .expect("Can't lock messages")
                                .difference(messages_known_to_node)
                                .copied()
                                .collect()
                        };
                        messages
                    };

                    if messages.is_empty() {
                        continue;
                    }

                    let msg = Message::new(
                        node_id.clone(),
                        dest_id.clone(),
                        Payload::Gossip { messages },
                    )
                    .with_id(rand::rng().random::<u32>() as usize);
                    let mut n = network.clone();
                    let b = known_messages.clone();
                    tokio::spawn(async move {
                        match n.rpc(msg.clone(), Duration::from_millis(30)) {
                            Err(RpcError::Timeout) => {
                                // continue
                            }
                            Ok(reply) => {
                                eprintln!("Got reply for message {reply:?}");

                                let Payload::GossipOk { messages } = reply.body.payload else {
                                    panic!("expected gossip_ok");
                                };
                                let mut known_message = b.lock().expect("Unable to get lock");

                                known_message
                                    .entry(dest_id.clone())
                                    .and_modify(|v| v.extend(messages.clone()))
                                    .or_insert(messages.clone());
                            }
                        }
                    });
                }
            }
        });
    }

    fn handle_broadcast(&self, msg: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Broadcast { message } = msg.body.payload else {
            panic!("expected broadcast");
        };

        {
            let mut messages = self.messages.lock().expect("Unable to get lock");
            messages.insert(message);
        }
        let reply = msg.reply(Payload::BroadcastOk);
        self.network.send(reply);
        Ok(())
    }

    fn handle_read(&self, msg: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Read = msg.body.payload else {
            panic!("expected read");
        };

        let reply = msg.reply(Payload::ReadOk {
            messages: self.messages.lock().expect("Unable to get lock").clone(),
        });
        self.network.send(reply);
        Ok(())
    }

    fn handle_gossip(&self, msg: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Gossip {
            messages: incoming_messages,
        } = &msg.body.payload
        else {
            panic!("expected gossip");
        };

        {
            let mut messages = self.messages.lock().expect("Unable to get lock");
            messages.extend(incoming_messages);
        }

        let reply = msg.reply(Payload::GossipOk {
            messages: incoming_messages.clone(),
        });
        self.network.send(reply);
        Ok(())
    }

    fn handle_topology(&self, msg: Message<Payload>) -> anyhow::Result<()> {
        let Payload::Topology { .. } = &msg.body.payload else {
            panic!("expected topology");
        };

        let reply = msg.reply(Payload::TopologyOk);
        self.network.send(reply);
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        messages: HashSet<usize>,
    },
    GossipOk {
        messages: HashSet<usize>,
    },
}
