use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::Debug,
    io::{stdout, Write},
    marker::PhantomData,
    sync::mpsc::{self, Sender},
    thread,
};

pub trait Payload: Clone + Debug + Serialize + DeserializeOwned + Send + 'static {}
impl<P: Clone + Debug + Serialize + DeserializeOwned + Send + 'static> Payload for P {}

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

pub trait Node<Payload> {
    fn from_init(id: String, neighbors: Vec<String>, send_tx: Sender<Message<Payload>>) -> Self;
    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()>;
}
