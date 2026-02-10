use gossip::{Message, Node, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::mpsc::Sender;

fn main() -> anyhow::Result<()> {
    Runtime::<Payload, UniqueIdsNode>::run()
}

struct UniqueIdsNode {
    node_id: String,
    outbound: Sender<Message<Payload>>,
}
impl Node<Payload> for UniqueIdsNode {
    fn from_init(
        id: String,
        _neighbors: Vec<String>,
        send_tx: std::sync::mpsc::Sender<gossip::Message<Payload>>,
    ) -> Self {
        Self {
            node_id: id,
            outbound: send_tx,
        }
    }

    fn handle_message(&mut self, message: gossip::Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Generate => {
                let msg_id: usize = message
                    .body
                    .msg_id
                    .expect("msg_id should be available in generate message");
                let node_id = &self.node_id;
                self.outbound.send(message.reply(Payload::GenerateOk {
                    id: format!("{node_id}-{msg_id}"),
                }))?
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}
