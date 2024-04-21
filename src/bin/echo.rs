use core::panic;
use gossip::Runtime;
use serde::{Deserialize, Serialize};

fn main() -> anyhow::Result<()> {
    let runtime = Runtime::new();
    for msg in runtime.messages::<Payload>() {
        let Ok(msg) = msg else { panic!("got error") };

        let reply_payload = match msg.get_payload() {
            Payload::Echo { echo } => Payload::EchoOk { echo: echo.clone() },
            Payload::EchoOk { .. } => continue,
        };
        let reply = msg.reply(reply_payload);
        runtime.send(&reply)?
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}
