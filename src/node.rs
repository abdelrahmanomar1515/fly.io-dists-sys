use crate::{message::Message, Network, Payload};
use async_trait::async_trait;

#[async_trait]
pub trait Node<TPayload>: Send + Sync
where
    TPayload: Payload,
{
    fn from_init(id: String, neighbors: Vec<String>, network: Network<TPayload>) -> Self;
    async fn handle_message(&self, message: Message<TPayload>) -> anyhow::Result<()>;
}
