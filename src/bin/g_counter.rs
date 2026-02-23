use gossip::{Message, Network, Node, Runtime};
use serde::{Deserialize, Serialize};
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    Runtime::<Payload, GCounterNode>::run()
}

const TIMEOUT: std::time::Duration = Duration::from_secs(3);

struct GCounterNode {
    node_id: String,
    all_nodes: Vec<String>,
    network: Network<Payload>,
    cache: usize,
    msg_id: usize,
}

impl GCounterNode {
    fn get_id(&mut self) -> usize {
        self.msg_id += 1;
        self.msg_id
    }

    fn store_write(&mut self, key: &str, value: usize) -> anyhow::Result<Message<Payload>> {
        let msg_id = self.get_id();
        let msg = Message::new(
            self.node_id.clone(),
            "seq-kv".into(),
            Payload::SeqKvWrite {
                key: key.to_string(),
                value,
            },
        )
        .with_id(msg_id);
        self.network
            .rpc(msg, TIMEOUT)
            .map_err(|e| anyhow::anyhow!("Unable to write to store: {e:?}"))
    }

    fn store_read(&mut self, key: &str) -> anyhow::Result<Message<Payload>> {
        let msg_id = self.get_id();
        let msg = Message::new(
            self.node_id.clone(),
            "seq-kv".into(),
            Payload::SeqKvRead {
                key: key.to_string(),
            },
        )
        .with_id(msg_id);
        self.network
            .rpc(msg, TIMEOUT)
            .map_err(|e| anyhow::anyhow!("Unable to read from store: {e:?}"))
    }

    fn store_compare_and_swap(
        &mut self,
        key: &str,
        from: usize,
        to: usize,
    ) -> anyhow::Result<Message<Payload>> {
        let msg_id = self.get_id();
        let msg = Message::new(
            self.node_id.clone(),
            "seq-kv".into(),
            Payload::SeqKvCas {
                key: key.to_string(),
                from,
                to,
                create_if_not_exists: true,
            },
        )
        .with_id(msg_id);
        self.network
            .rpc(msg, TIMEOUT)
            .map_err(|e| anyhow::anyhow!("Unable to cas store: {e:?}"))
    }

    fn handle_add(&mut self, msg: &Message<Payload>, delta: usize) -> anyhow::Result<()> {
        if delta == 0 {
            return Ok(());
        }
        let from = self.cache;
        let to = self.cache + delta;
        self.cache += delta;

        self.store_compare_and_swap(&self.node_id.clone(), from, to)
            .map_err(|e| anyhow::anyhow!("Unable to handle add when saving to store: {e}"))?;
        match self.store_compare_and_swap(&self.node_id.clone(), from, to) {
            Ok(..) => Ok(()),
            Err(error) => Err(anyhow::anyhow!(
                "Unable to handle add when saving to store: {error}"
            )),
        }?;

        self.network.send(msg.reply(Payload::AddOk));
        Ok(())
    }

    fn handle_read(&mut self, msg: &Message<Payload>) -> anyhow::Result<()> {
        let mut total = self.cache;
        for node in self.all_nodes.clone() {
            if node == self.node_id {
                continue;
            }
            let reply = self.store_read(&node).inspect_err(|e| {
                eprintln!("Unable to read value of node: {node} with error {e}")
            })?;
            if let Payload::ReadOk { value } = reply.get_payload() {
                total += value;
            } else {
                eprintln!("Something wrong wtf: {reply:?}");
            }
        }

        self.network
            .send(msg.reply(Payload::ReadOk { value: total }));
        eprintln!("Returned read result as {total}");
        Ok(())
    }
}

impl Node<Payload> for GCounterNode {
    fn from_init(id: String, neighbors: Vec<String>, network: Network<Payload>) -> Self {
        Self {
            network,
            node_id: id,
            all_nodes: neighbors,
            cache: 0,
            msg_id: 0,
        }
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Add { delta } => self
                .handle_add(&message, *delta)
                .inspect_err(|e| eprintln!("Got error on add: {e}")),
            Payload::Read => self
                .handle_read(&message)
                .inspect_err(|e| eprintln!("Got error on read: {e}")),
            Payload::Error {
                code,
                text,
                in_reply_to,
            } => {
                eprintln!(
                    "Got error message with code: {code}, {text}, in reply to: {in_reply_to:?}"
                );
                Ok(())
            }
            Payload::ReadOk { .. }
            | Payload::AddOk
            | Payload::SeqKvRead { .. }
            | Payload::SeqKvWrite { .. }
            | Payload::SeqKvWriteOk
            | Payload::SeqKvCas { .. }
            | Payload::SeqKvCasOk => {
                eprintln!("Unhandeld message type: {message:?}");
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },

    #[serde(rename = "read")]
    SeqKvRead {
        key: String,
    },
    #[serde(rename = "write")]
    SeqKvWrite {
        key: String,
        value: usize,
    },
    #[serde(rename = "write_ok")]
    SeqKvWriteOk,
    #[serde(rename = "cas")]
    SeqKvCas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
    #[serde(rename = "cas_ok")]
    SeqKvCasOk,

    Error {
        code: usize,
        text: String,
        in_reply_to: Option<usize>,
    },
}
