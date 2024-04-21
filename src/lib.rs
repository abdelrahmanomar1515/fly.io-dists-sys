use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io::Write};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

impl<T> Message<T> {
    pub fn new(src: String, dest: String, payload: T) -> Message<T> {
        Self {
            src,
            dest,
            body: Body {
                msg_id: None,
                in_reply_to: None,
                payload,
            },
        }
    }

    pub fn reply<Payload>(&self, payload: Payload) -> Message<Payload> {
        Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: Body {
                msg_id: self.body.msg_id.map(|id| id + 1),
                in_reply_to: self.body.msg_id,
                payload,
            },
        }
    }

    pub fn get_payload(&self) -> &T {
        &self.body.payload
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

pub struct Runtime {
    pub node_id: String,
}

impl Runtime {
    pub fn new() -> Self {
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
                    payload: InitializationPayload::Init { ref node_id, .. },
                    ..
                },
            ..
        } = init_msg
        else {
            panic!("first message not init")
        };

        let node = Self {
            node_id: node_id.clone(),
        };
        let reply = init_msg.reply(InitializationPayload::InitOk);
        node.send(&reply).expect("reply init ok failed");

        node
    }

    pub fn messages<'de, Payload: Deserialize<'de> + 'de>(
        &self,
    ) -> impl Iterator<Item = Result<Message<Payload>, serde_json::Error>> + 'de {
        let stdin = std::io::stdin().lock();
        serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>()
    }

    pub fn send<Payload: Serialize>(&self, msg: &Message<Payload>) -> anyhow::Result<()> {
        let mut stdout = std::io::stdout().lock();
        serde_json::to_writer(&mut stdout, &msg).context("Serialize to stdout")?;
        writeln!(stdout).context("write to stdout")?;
        Ok(())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
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
