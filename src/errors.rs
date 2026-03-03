use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum RpcError {
    KeyDoesNotExist,
    CasFail,
    Unknown(anyhow::Error),
}

impl Error for RpcError {}

impl Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
