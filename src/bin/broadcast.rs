use core::panic;
use gossip::{Message, Runtime};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

fn main() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::new());
    let runtime2 = Arc::clone(&runtime);
    let node_state = NodeState::new();
    let node_state = Arc::new(Mutex::from(node_state));
    let node_state2 = Arc::clone(&node_state);

    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(75));
        node_state.lock().unwrap().gossip(&runtime2)
    });

    for msg in runtime.messages::<Payload>() {
        let Ok(msg) = msg else { panic!("got error") };

        let reply_payload = {
            let mut state1 = node_state2.lock().unwrap();
            state1.payload_reply(&msg, &runtime)
        };
        if let Some(reply_payload) = reply_payload {
            let reply = msg.reply(reply_payload);
            runtime.send(&reply)?
        }
    }
    Ok(())
}

struct NodeState {
    messages: HashSet<usize>,
    known_messages: HashMap<String, HashSet<usize>>,
    nearby_nodes: Vec<String>,
}

impl NodeState {
    fn new() -> Self {
        Self {
            messages: Default::default(),
            known_messages: Default::default(),
            nearby_nodes: Default::default(),
        }
    }

    fn payload_reply(&mut self, msg: &Message<Payload>, runtime: &Runtime) -> Option<Payload> {
        match msg.get_payload() {
            Payload::Broadcast { message } => {
                self.messages.insert(*message);
                Some(Payload::BroadcastOk)
            }
            Payload::Read => Some(Payload::ReadOk {
                messages: self.messages.clone(),
            }),
            Payload::Topology { topology } => {
                if let Some(nodes) = topology.get(&runtime.node_id) {
                    self.nearby_nodes = nodes.clone();
                };
                Some(Payload::TopologyOk)
            }
            Payload::Gossip { messages } => {
                self.messages.extend(messages.clone());
                Some(Payload::GossipOk {
                    messages: messages.clone(),
                })
            }
            Payload::GossipOk { messages } => {
                self.known_messages
                    .entry(msg.src.clone())
                    .and_modify(|v| v.extend(messages.clone()))
                    .or_insert(messages.clone());
                None
            }
            Payload::ReadOk { .. } | Payload::BroadcastOk | Payload::TopologyOk => {
                panic!("got invalid message")
            }
        }
    }

    fn gossip(&self, runtime: &Runtime) {
        for dest_id in &self.nearby_nodes {
            let empty_set = HashSet::new();
            let messages_known_to_node = self.known_messages.get(dest_id).unwrap_or(&empty_set);
            let messages: HashSet<usize> = self
                .messages
                .difference(messages_known_to_node)
                .copied()
                .collect();

            if messages.is_empty() {
                continue;
            }

            let msg = Message::new(
                runtime.node_id.clone(),
                dest_id.clone(),
                Payload::Gossip { messages },
            );
            runtime.send(&msg).expect("failed to send gossip message");
        }
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
