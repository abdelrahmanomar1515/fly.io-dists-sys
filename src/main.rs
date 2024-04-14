use anyhow::{anyhow, bail, Context};
use core::panic;
use serde::{Deserialize, Serialize};
use std::{
    env::args,
    io::{self, stdin, Stdin, Write},
};

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
}

impl Message {
    fn handle(self, node_id: &Option<String>) -> Self {
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
                        let node_id = node_id
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
                    Payload::GenerateOk { .. } => panic!("Received echo_ok"),
                },
            },
        }
    }
}

struct Node {
    id: Option<String>,
}

impl Node {
    fn handle(&mut self, msg: Message) -> Message {
        if let Payload::Init { node_id, .. } = &msg.body.payload {
            self.id = Some(node_id.clone())
        }
        msg.handle(&self.id)
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut node = Node { id: None };

    for input in inputs {
        let input = input.context("Maelstrom input from STDIN can't be deserialized")?;
        serde_json::to_writer(&mut stdout, &node.handle(input)).context("Write to stdout")?;
        write!(stdout, "\n")?;
    }

    Ok(())
}
