use crate::message::Message;
use std::sync::mpsc::Sender;

pub trait Node<Payload> {
    fn from_init(id: String, neighbors: Vec<String>, send_tx: Sender<Message<Payload>>) -> Self;
    fn handle_message(&mut self, message: Message<Payload>) -> anyhow::Result<()>;
}
