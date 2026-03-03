use crate::message::Body;
use crate::Network;
use crate::{
    message::{Message, Payload},
    node::Node,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt::Debug, marker::PhantomData};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::channel;

pub struct Runtime<TPayload, TNode>(PhantomData<TPayload>, PhantomData<TNode>)
where
    TPayload: Payload,
    TNode: Node<TPayload>;

impl<TPayload, TNode> Runtime<TPayload, TNode>
where
    TPayload: Payload,
    TNode: Node<TPayload> + 'static + Clone,
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

        let (stdin_tx, mut stdin_rx) = channel(1);
        let (stdout_tx, mut stdout_rx) = channel(1);

        let network = Network::new(stdout_tx);
        let node = TNode::from_init(node_id.clone(), node_ids.clone(), network.clone());

        let reply = init_msg.reply(InitializationPayload::InitOk);
        {
            let mut stdout = tokio::io::stdout();
            let reply = serde_json::to_string(&reply).context("Serialize init_ok")?;
            stdout
                .write_all(reply.as_bytes())
                .await
                .expect("Writing to stdout");
            stdout.write_all(b"\n").await.expect("Writing to stdout");
            stdout
                .flush()
                .await
                .expect("Unable to send init_ok msg to stdout: {error}");
        }

        tokio::spawn(async move {
            while let Some(msg) = stdout_rx.recv().await {
                eprintln!("Out: {msg}");
                let mut stdout = tokio::io::stdout();
                stdout.write_all(msg.as_bytes()).await?;
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
                let msg: Message<Value> = msg.expect("Malformed message");
                eprintln!("In : {line}");
                if let Some(reply_channel) = msg
                    .body
                    .in_reply_to
                    .and_then(|msg_id| network.get_reply_channel(&msg_id))
                {
                    // eprintln!("reply channel {msg_id}");
                    if let Err(e) = reply_channel.send(line) {
                        eprintln!("Unable to send to rpc handler: {e:?}");
                    }
                } else {
                    stdin_tx
                        .send(msg)
                        .await
                        .expect("Unable to send out message");
                };
            }
        });

        while let Some(msg) = stdin_rx.recv().await {
            let msg = msg.into_typed()?;
            let node = node.clone();
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
