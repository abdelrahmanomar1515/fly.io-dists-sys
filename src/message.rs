use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;

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

    pub fn is_reply(&self) -> bool {
        self.body.in_reply_to.is_some()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}
