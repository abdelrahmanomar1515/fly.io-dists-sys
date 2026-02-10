use gossip::{Message, Node, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::mpsc::Sender;

fn main() -> anyhow::Result<()> {
    Runtime::<Payload, EchoNode>::run()
}

struct EchoNode {
    outbound: Sender<Message<Payload>>,
}

impl Node<Payload> for EchoNode {
    fn from_init(
        _id: String,
        _neighbors: Vec<String>,
        send_tx: Sender<gossip::Message<Payload>>,
    ) -> Self {
        Self { outbound: send_tx }
    }

    fn handle_message(&self, message: Message<Payload>) -> anyhow::Result<()> {
        if let Payload::Echo { echo } = message.get_payload() {
            self.outbound
                .send(message.reply(Payload::EchoOk { echo: echo.clone() }))?
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
