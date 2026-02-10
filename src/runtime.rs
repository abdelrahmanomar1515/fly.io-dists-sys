use crate::message::Body;
use crate::{
    message::{Message, Payload},
    node::Node,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    io::{stdout, Write},
    marker::PhantomData,
    sync::mpsc::{self},
    thread,
};

pub struct Runtime<TPayload: Payload, TNode: Node<TPayload>>(
    PhantomData<TPayload>,
    PhantomData<TNode>,
);

impl<TPayload: Payload, TNode: Node<TPayload>> Runtime<TPayload, TNode> {
    pub fn run() -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let init_msg = serde_json::Deserializer::from_reader(stdin)
            .into_iter::<Message<InitializationPayload>>()
            .next()
            .expect("first init")
            .context("deserialize init")
            .unwrap();
        let Message {
            body:
                Body {
                    payload:
                        InitializationPayload::Init {
                            ref node_id,
                            ref node_ids,
                        },
                    ..
                },
            ..
        } = init_msg
        else {
            panic!("first message not init")
        };

        let (stdin_tx, stdin_rx) = mpsc::channel();
        let (stdout_tx, stdout_rx) = mpsc::channel();

        thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            let msgs =
                serde_json::Deserializer::from_reader(stdin).into_iter::<Message<TPayload>>();
            for msg in msgs.map(|r| r.expect("Malformed message")) {
                stdin_tx.send(msg).expect("Unable to send out message");
            }
        });

        let mut node = TNode::from_init(node_id.clone(), node_ids.clone(), stdout_tx);

        let _jh = thread::spawn(|| {
            for msg in stdout_rx {
                let mut stdout = stdout().lock();
                let msg_json = serde_json::to_string(&msg).expect("Serialize out message");
                if let Err(error) = writeln!(stdout, "{msg_json}") {
                    eprintln!("Unable to send msg to stdout: {error}")
                }
            }
        });

        let reply = init_msg.reply(InitializationPayload::InitOk);
        {
            let mut stdout = stdout().lock();
            let reply = serde_json::to_string(&reply).context("Serialize init_ok")?;
            if let Err(error) = writeln!(stdout, "{reply}") {
                eprintln!("Unable to send init_ok msg to stdout: {error}")
            }
        }

        for msg in stdin_rx {
            node.handle_message(msg).context("Handling message")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitializationPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}
