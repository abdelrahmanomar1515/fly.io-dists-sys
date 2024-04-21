use core::panic;
use gossip::Runtime;
use serde::{Deserialize, Serialize};

fn main() -> anyhow::Result<()> {
    let runtime = Runtime::new();
    for msg in runtime.messages::<Payload>() {
        let Ok(msg) = msg else { panic!("got error") };

        let reply_payload = match msg.get_payload() {
            Payload::Generate => {
                let node_id = runtime.node_id.clone();
                let msg_id: usize = msg
                    .body
                    .msg_id
                    .expect("msg_id should be available in generate message");
                Payload::GenerateOk {
                    id: format!("{node_id} - {}", msg_id),
                }
            }
            Payload::GenerateOk { .. } => continue,
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
    Generate,
    GenerateOk { id: String },
}
