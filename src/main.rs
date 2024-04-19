use anyhow::Context;
use core::panic;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    sync::mpsc,
    thread::{self, sleep},
    time::Duration,
};

fn main() -> anyhow::Result<()> {
    let (stdout_send, stdout_recv) = mpsc::channel();
    let (msg_send, msg_recv) = mpsc::channel();
    let (timer_send, timer_recv) = mpsc::channel::<()>();

    thread::spawn(move || -> anyhow::Result<()> {
        let mut stdout = std::io::stdout().lock();
        loop {
            if let Ok(msg) = stdout_recv.recv() {
                eprintln!("output: {:?}", msg);
                serde_json::to_writer(&mut stdout, &msg).context("Serialize to stdout")?;
                writeln!(stdout).context("write to stdout")?
            }
        }
    });

    thread::spawn(move || -> anyhow::Result<()> {
        loop {
            sleep(Duration::from_millis(100));
            timer_send.send(())?;
        }
    });

    thread::spawn(move || -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for input in inputs {
            let input = input.context("deserialize stdin")?;
            eprintln!("inputs: {:?}", input);
            msg_send.send(input)?;
        }

        eprintln!("input stream finished");
        Ok(())
    });

    let mut node = NodeState {
        id: None,
        messages: HashSet::new(),
        nearby_nodes: vec![],
        timer_recv,
        msg_recv,
        stdout_send,
        known_messages: HashMap::new(),
    };
    node.handle().context("handling failed")?;
    eprintln!("somehow finished");

    Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Body {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: String,
    },
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

struct NodeState {
    id: Option<String>,
    messages: HashSet<usize>,
    known_messages: HashMap<String, HashSet<usize>>,
    nearby_nodes: Vec<String>,
    stdout_send: mpsc::Sender<Message>,
    msg_recv: mpsc::Receiver<Message>,
    timer_recv: mpsc::Receiver<()>,
}

impl NodeState {
    fn handle(&mut self) -> anyhow::Result<()> {
        loop {
            if self.timer_recv.try_recv().is_ok() {
                self.gossip().context("gossip failed")?;
            }

            let msg = self.msg_recv.try_recv();
            if msg.is_err() {
                continue;
            }
            let msg = msg.unwrap();

            if let Payload::Init { node_id, .. } = &msg.body.payload {
                self.id = Some(node_id.clone())
            }

            let reply = Message {
                src: msg.dest,
                dest: msg.src.clone(),
                body: Body {
                    msg_id: msg.body.msg_id.map(|id| id + 1),
                    in_reply_to: msg.body.msg_id,
                    payload: match msg.body.payload {
                        Payload::Echo { echo } => Payload::EchoOk { echo },
                        Payload::EchoOk { .. } => panic!("Received echo_ok"),
                        Payload::Init { .. } => Payload::InitOk,
                        Payload::InitOk => panic!("Received init_ok"),
                        Payload::Generate => {
                            let node_id = self
                                .id
                                .as_ref()
                                .expect("node_id should be set when calling generate");
                            let msg_id: usize = msg
                                .body
                                .msg_id
                                .expect("msg_id should be available in generate message");
                            Payload::GenerateOk {
                                id: format!("{node_id} - {}", msg_id),
                            }
                        }
                        Payload::GenerateOk { .. } => panic!("Received generate_ok"),
                        Payload::Broadcast { message } => {
                            self.messages.insert(message);
                            Payload::BroadcastOk
                        }
                        Payload::BroadcastOk => panic!("Received broadcast_ok"),
                        Payload::Read => Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                        Payload::ReadOk { .. } => panic!("Received read_ok"),
                        Payload::Topology { topology } => {
                            if let Some(nodes) = topology.get(
                                self.id
                                    .as_ref()
                                    .expect("node_id should be set when calling toplogy"),
                            ) {
                                self.nearby_nodes = nodes.clone();
                            };
                            Payload::TopologyOk
                        }
                        Payload::TopologyOk => panic!("Received toplogy_ok"),
                        Payload::Gossip { messages } => {
                            self.messages.extend(messages.clone());
                            Payload::GossipOk { messages }
                        }
                        Payload::GossipOk { messages } => {
                            self.known_messages
                                .entry(msg.src)
                                .and_modify(|v| v.extend(messages.clone()))
                                .or_insert(messages);
                            continue;
                        }
                    },
                },
            };
            self.stdout_send
                .send(reply)
                .context("sending to stdout channel")?;
        }
    }

    fn gossip(&self) -> anyhow::Result<()> {
        for node_id in &self.nearby_nodes {
            let empty_set = HashSet::new();
            let messages_known_to_node = self.known_messages.get(node_id).unwrap_or(&empty_set);
            let messages: HashSet<usize> = self
                .messages
                .difference(messages_known_to_node)
                .copied()
                .collect();

            if messages.is_empty() {
                continue;
            }

            let msg = Message {
                body: Body {
                    in_reply_to: None,
                    msg_id: None,
                    payload: Payload::Gossip { messages },
                },
                src: self.id.clone().expect("message id already set"),
                dest: node_id.clone(),
            };
            self.stdout_send.send(msg).context("injecting message")?
        }
        Ok(())
    }
}
