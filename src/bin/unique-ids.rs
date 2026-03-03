use gossip::{Network, Node, Runtime};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Runtime::<Payload, UniqueIdsNode>::run().await
}

#[derive(Clone)]
struct UniqueIdsNode {
    node_id: String,
    network: Network,
}
impl Node<Payload> for UniqueIdsNode {
    fn from_init(id: String, _neighbors: Vec<String>, network: Network) -> Self {
        Self {
            node_id: id,
            network,
        }
    }

    async fn handle_message(&self, message: gossip::Message<Payload>) -> anyhow::Result<()> {
        match message.get_payload() {
            Payload::Generate => {
                let msg_id: usize = message
                    .body
                    .msg_id
                    .expect("msg_id should be available in generate message");
                let node_id = &self.node_id;
                self.network
                    .send(&message.reply(Payload::GenerateOk {
                        id: format!("{node_id}-{msg_id}"),
                    }))
                    .await
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
