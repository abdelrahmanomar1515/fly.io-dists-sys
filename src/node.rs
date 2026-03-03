use crate::{message::Message, Network, Payload};
use std::{future::Future, marker::Send};

pub trait Node<TPayload>: Send + Sync
where
    TPayload: Payload,
{
    fn from_init(id: String, neighbors: Vec<String>, network: Network) -> Self;
    fn handle_message(
        &self,
        message: Message<TPayload>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
