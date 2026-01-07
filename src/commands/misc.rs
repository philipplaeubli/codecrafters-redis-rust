use bytes::Bytes;

use super::{CommandError, utils::extract_key};
use crate::{
    parser::RedisType,
    store::{Store, StoreError},
};

pub fn handle_ping(arguments: &[RedisType]) -> Result<RedisType, CommandError> {
    if !arguments.is_empty() {
        // as per https://redis.io/docs/latest/commands/ping/, ping should return the arguments passed to it
        return handle_echo(arguments);
    }
    Ok(RedisType::SimpleString(Bytes::from_static(b"PONG")))
}

pub fn handle_echo(arguments: &[RedisType]) -> Result<RedisType, CommandError> {
    let message = arguments.first();
    match message {
        Some(RedisType::BulkString(value)) => Ok(RedisType::BulkString(value.clone())),
        _ => Ok(RedisType::SimpleString(Bytes::from_static(b""))),
    }
}

pub fn handle_type(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;
    match store.get_type(key) {
        Ok(resp) => Ok(RedisType::SimpleString(resp)),
        Err(error) => match error {
            StoreError::KeyNotFound => Ok(RedisType::SimpleString("none".into())),
            _ => Err(CommandError::StoreError(error)),
        },
    }
}
