use anyhow::Context;
use core::panic;
use gossip::{main_loop, Handle};
use serde::{Deserialize, Serialize};
use std::sync::mpsc;

fn main() -> anyhow::Result<()> {
    main_loop(NodeState::new)
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
    Generate,
    GenerateOk {
        id: String,
    },
}

struct NodeState {
    id: Option<String>,
    stdout_send: mpsc::Sender<Message>,
    msg_recv: mpsc::Receiver<Message>,
}

impl NodeState {
    fn new(
        _timer_recv: mpsc::Receiver<()>,
        msg_recv: mpsc::Receiver<Message>,
        stdout_send: mpsc::Sender<Message>,
    ) -> Self {
        Self {
            id: None,
            stdout_send,
            msg_recv,
        }
    }
}
impl Handle for NodeState {
    fn handle(&mut self) -> anyhow::Result<()> {
        loop {
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
                    },
                },
            };
            self.stdout_send
                .send(reply)
                .context("sending to stdout channel")?;
        }
    }
}
