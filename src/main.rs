use anyhow::{anyhow, Context};
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
}

impl Payload {
    fn reply(self) -> Payload {
        match self {
            Payload::Echo { echo } => Payload::EchoOk { echo },
            Payload::EchoOk { .. } => panic!("Received echo_ok"),
            Payload::Init { .. } => Payload::InitOk,
            Payload::InitOk { .. } => panic!("Received init_ok"),
        }
    }
}

impl Message {
    fn handle(self) -> Self {
        Message {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id: self.body.msg_id.map(|id| id + 1),
                in_reply_to: self.body.msg_id,
                payload: self.body.payload.reply(),
            },
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    for input in inputs {
        let input = input.context("Maelstrom input from STDIN can't be deserialized")?;
        serde_json::to_writer(&mut stdout, &input.handle()).context("Write to stdout")?;
        write!(stdout, "\n")?;
    }

    Ok(())
}
