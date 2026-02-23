use crate::message::Body;
use crate::Network;
use crate::{
    message::{Message, Payload},
    node::Node,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::mpsc::{self},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub struct Runtime<TPayload, TNode>(PhantomData<TPayload>, PhantomData<TNode>)
where
    TPayload: Payload,
    TNode: Node<TPayload>;

impl<TPayload, TNode> Runtime<TPayload, TNode>
where
    TPayload: Payload,
    TNode: Node<TPayload> + 'static,
{
    pub async fn run() -> anyhow::Result<()> {
        let stdin = tokio::io::stdin();
        let mut stdin_lines = BufReader::new(stdin).lines();

        let init_msg = &stdin_lines.next_line().await?.expect("First message");
        let init_msg = serde_json::from_str(init_msg).expect("Must be init");
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

        let network = Network::new(stdout_tx);
        let node = TNode::from_init(node_id.clone(), node_ids.clone(), network.clone());

        let reply = init_msg.reply(InitializationPayload::InitOk);
        {
            let mut stdout = tokio::io::stdout();
            let reply = serde_json::to_string(&reply).context("Serialize init_ok")?;
            eprintln!("Trying to send reply: {reply}");
            stdout
                .write_all(reply.as_bytes())
                .await
                .expect("Writing to stdout");
            stdout.write_all(b"\n").await.expect("Writing to stdout");
            eprintln!("wrote reply");
            stdout
                .flush()
                .await
                .expect("Unable to send init_ok msg to stdout: {error}");
            eprintln!("flushed write");
        }

        tokio::spawn(async {
            for msg in stdout_rx {
                eprintln!("Sending message: {msg:?}");
                let mut stdout = tokio::io::stdout();
                let msg_json = serde_json::to_string(&msg).expect("Serialize out message");
                stdout.write_all(msg_json.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
            tokio::io::Result::Ok(())
        });

        tokio::spawn(async move {
            let stdin = tokio::io::stdin();
            let mut lines = tokio::io::BufReader::new(stdin).lines();

            while let Some(line) = lines.next_line().await.expect("Malformed new line message") {
                let msg = serde_json::from_str(&line);
                let msg: Message<TPayload> = msg.expect("Malformed message");
                eprintln!("Got message: {msg:?}");
                if let Some(reply_channel) = msg
                    .body
                    .in_reply_to
                    .and_then(|msg_id| network.get_reply_channel(&msg_id))
                {
                    // eprintln!("reply channel {msg_id}");
                    if let Err(e) = reply_channel.send(msg) {
                        eprintln!("Unable to send to rpc handler: {e}");
                    }
                } else {
                    stdin_tx.send(msg).expect("Unable to send out message");
                };
            }
        });

        let node = Arc::new(node);
        for msg in stdin_rx.into_iter() {
            let node = node.clone();
            // eprintln!("strong count: {}", Arc::strong_count(&node));
            // eprintln!("weak count: {}", Arc::strong_count(&node));
            eprintln!("Handling message: {msg:?}");
            tokio::spawn(async move {
                let _ = node
                    .handle_message(msg)
                    .await
                    .inspect_err(|e| eprintln!("Got error when handling message: {e}"));
            });
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
