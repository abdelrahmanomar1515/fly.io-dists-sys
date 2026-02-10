use gossip::{Message, Network, Node, Runtime};
use serde::{Deserialize, Serialize};

fn main() -> anyhow::Result<()> {
    Runtime::<Payload, EchoNode>::run()
}

struct EchoNode {
    network: Network<Payload>,
}

impl Node<Payload> for EchoNode {
    fn from_init(_id: String, _neighbors: Vec<String>, network: Network<Payload>) -> Self {
        Self { network }
    }

    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()> {
        if let Payload::Echo { echo } = message.get_payload() {
            self.network
                .send(message.reply(Payload::EchoOk { echo: echo.clone() }))
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}
