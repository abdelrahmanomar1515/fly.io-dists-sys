use anyhow::Context;
use core::panic;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Write, sync::mpsc, thread};

fn main() -> anyhow::Result<()> {
    let (stdout_send, stdout_recv) = mpsc::channel();
    let (msg_send, msg_recv) = mpsc::channel();

    thread::spawn(move || -> anyhow::Result<()> {
        let mut stdout = std::io::stdout().lock();
        eprintln!("can take lock");
        loop {
            if let Ok(msg) = stdout_recv.recv() {
                eprintln!("message to print: {:?}", msg);
                serde_json::to_writer(&mut stdout, &msg).context("Write to stdout")?;
                writeln!(stdout).context("write to stdout")?
            }
        }
    });

    let msg_send2 = msg_send.clone();
    thread::spawn(move || -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for input in inputs {
            let input = input.context("Maelstrom input from STDIN can't be deserialized")?;
            eprintln!("input: {:?}", input);
            msg_send2.send(input)?;
        }

        eprintln!("finished reading input ");
        Ok(())
    });

    let mut node = NodeState {
        id: None,
        messages: vec![],
        nearby_nodes: vec![],
        msg_send: msg_send.clone(),
        msg_recv,
        stdout_send,
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
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

impl Message {
    fn handle(self, node_state: &mut NodeState) -> Self {
        Message {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id: self.body.msg_id.map(|id| id + 1),
                in_reply_to: self.body.msg_id,
                payload: match self.body.payload {
                    Payload::Echo { echo } => Payload::EchoOk { echo },
                    Payload::EchoOk { .. } => panic!("Received echo_ok"),
                    Payload::Init { .. } => Payload::InitOk,
                    Payload::InitOk => panic!("Received init_ok"),
                    Payload::Generate => {
                        let node_id = node_state
                            .id
                            .as_ref()
                            .expect("node_id should be set when calling generate");
                        let msg_id: usize = self
                            .body
                            .msg_id
                            .expect("msg_id should be available in generate message");
                        Payload::GenerateOk {
                            id: format!("{node_id} - {}", msg_id),
                        }
                    }
                    Payload::GenerateOk { .. } => panic!("Received generate_ok"),
                    Payload::Broadcast { message } => {
                        node_state.messages.push(message);
                        node_state.broadcast(message);
                        Payload::BroadcastOk
                    }
                    Payload::BroadcastOk => panic!("Received broadcast_ok"),
                    Payload::Read => Payload::ReadOk {
                        messages: node_state.messages.clone(),
                    },
                    Payload::ReadOk { .. } => panic!("Received read_ok"),
                    Payload::Topology { topology } => {
                        if let Some(nodes) = topology.get(
                            node_state
                                .id
                                .as_ref()
                                .expect("node_id should be set when calling toplogy"),
                        ) {
                            node_state.nearby_nodes = nodes.clone();
                        };
                        Payload::TopologyOk
                    }
                    Payload::TopologyOk => panic!("Received toplogy_ok"),
                },
            },
        }
    }
}

struct NodeState {
    id: Option<String>,
    messages: Vec<usize>,
    nearby_nodes: Vec<String>,
    stdout_send: mpsc::Sender<Message>,
    msg_send: mpsc::Sender<Message>,
    msg_recv: mpsc::Receiver<Message>,
}

impl NodeState {
    fn handle(&mut self) -> anyhow::Result<()> {
        loop {
            let recv = &self.msg_recv;
            let msg = recv.recv().expect("receving failed ");
            eprintln!("message to handle: {:?}", msg);

            if let Payload::Init { node_id, .. } = &msg.body.payload {
                self.id = Some(node_id.clone())
            }
            let reply = msg.handle(self);
            eprintln!("handled message with reply : {:?}", reply);
            self.stdout_send
                .send(reply)
                .context("sending to stdout channel")?;
        }
    }

    fn broadcast(&self, msg: usize) {
        let msg_send = self.msg_send.clone();
        thread::spawn(move || -> anyhow::Result<()> {
            let msg = Message {
                body: Body {
                    msg_id: todo!(),
                    in_reply_to: todo!(),
                    payload: todo!(),
                },
                src: todo!(),
                dest: todo!(),
            };
            msg_send.send(msg).context("injecting message")?;
            todo!("Implement broadcasting")
        });
    }
}
