use crate::{message::Message, Network, Payload};

pub trait Node<TPayload>
where
    TPayload: Payload,
{
    fn from_init(id: String, neighbors: Vec<String>, network: Network<TPayload>) -> Self;
    fn handle_message(&mut self, message: Message<TPayload>) -> anyhow::Result<()>;
}
