use crate::{Message, Network, RpcError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Debug, future::Future, marker::PhantomData};

pub trait Storable: Serialize + Debug + Clone + Send + Sync + DeserializeOwned + 'static {}
impl<AnyT: Serialize + Debug + Clone + Send + Sync + DeserializeOwned + 'static> Storable for AnyT {}
pub trait Storage<TValue: Storable> {
    fn get_type(&self) -> &str;
    fn get_src(&self) -> &str;
    fn get_network(&self) -> &Network;
    fn get(&self, key: String) -> impl Future<Output = Result<TValue, RpcError>> + Send
    where
        Self: Sync,
    {
        async {
            let src = self.get_src();
            let dest = self.get_type();
            let msg = &mut Message::new(
                src.to_string(),
                dest.to_string(),
                StoragePaylod::<TValue>::Read { key },
            );
            let response = self.get_network().rpc(msg).await.unwrap();
            let value = match response.get_payload() {
                StoragePaylod::ReadOk { value } => value,
                StoragePaylod::Error { .. } => {
                    return Err(RpcError::KeyDoesNotExist);
                }
                _ => {
                    panic!("Received wrong message {response:?}");
                }
            };
            Ok(value.clone())
        }
    }
    fn set(&self, key: String, value: TValue) -> impl Future<Output = Result<(), RpcError>> + Send
    where
        Self: Sync,
    {
        async {
            let network = self.get_network();
            let src = self.get_src();
            let dest = self.get_type();
            let msg = &mut Message::new(
                src.to_string(),
                dest.to_string(),
                StoragePaylod::Write { key, value },
            );
            let reponse = network.rpc(msg).await.map_err(RpcError::Unknown)?;
            let StoragePaylod::WriteOk = reponse.get_payload() else {
                panic!("Received wrong message")
            };
            Ok(())
        }
    }
    fn cas(
        &self,
        key: String,
        from: TValue,
        to: TValue,
    ) -> impl Future<Output = Result<(), RpcError>> + Send
    where
        Self: Sync,
    {
        async {
            let network = self.get_network();
            let src = self.get_src();
            let dest = self.get_type();
            let msg = &mut Message::new(
                src.to_string(),
                dest.to_string(),
                StoragePaylod::Cas {
                    key,
                    from,
                    to,
                    create_if_not_exists: true,
                },
            );
            let response = network.rpc(msg).await.map_err(RpcError::Unknown)?;
            match response.get_payload() {
                StoragePaylod::CasOk => (),
                StoragePaylod::Error { .. } => {
                    return Err(RpcError::CasFail);
                }
                _ => {
                    panic!("Received wrong message {response:?}");
                }
            };
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum StoragePaylod<TValue: Serialize + Send> {
    Read {
        key: String,
    },
    ReadOk {
        value: TValue,
    },
    Write {
        key: String,
        value: TValue,
    },
    WriteOk,
    Cas {
        key: String,
        from: TValue,
        to: TValue,
        create_if_not_exists: bool,
    },
    CasOk,
    Error {
        code: usize,
        text: String,
        in_reply_to: Option<usize>,
    },
}

#[derive(Clone, Debug)]
pub struct KeyValueStore<T> {
    store_type: &'static str,
    node_id: String,
    network: Network,
    _phantom: PhantomData<T>,
}
impl<T> KeyValueStore<T> {
    pub fn new(store_type: &'static str, network: Network, node_id: String) -> Self {
        Self {
            store_type,
            network,
            node_id,
            _phantom: Default::default(),
        }
    }
}
impl<T: Storable> Storage<T> for KeyValueStore<T> {
    fn get_type(&self) -> &str {
        self.store_type
    }

    fn get_src(&self) -> &str {
        &self.node_id
    }

    fn get_network(&self) -> &Network {
        &self.network
    }
}
