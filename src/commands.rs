use std::fmt::Display;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    command_utils::{argument_as_str, extract_key},
    keys::{handle_get, handle_set},
    lists::{handle_blpop, handle_llen, handle_lpop, handle_lpush, handle_lrange, handle_rpush},
    parser::RedisType,
    store::{Store, StoreError},
    streams::{handle_xadd, handle_xrange, handle_xread},
};

#[derive(Debug)]
pub enum CommandError {
    InvalidInput(String),
    UnknownCommand(String),
    StoreError(StoreError),
}

fn handle_pong(arguments: &[RedisType]) -> Result<RedisType, CommandError> {
    if !arguments.is_empty() {
        // as per https://redis.io/docs/latest/commands/ping/, ping should return the arguments passed to it
        return handle_echo(arguments);
    }
    Ok(RedisType::SimpleString(Bytes::from_static(b"PONG")))
}

fn handle_echo(arguments: &[RedisType]) -> Result<RedisType, CommandError> {
    let message = arguments.first();
    match message {
        Some(RedisType::BulkString(value)) => Ok(RedisType::BulkString(value.clone())),
        _ => Ok(RedisType::SimpleString(Bytes::from_static(b""))),
    }
}

fn handle_type(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;
    match store.get_type(key) {
        Ok(resp) => Ok(RedisType::SimpleString(resp)),
        Err(error) => match error {
            StoreError::KeyNotFound => Ok(RedisType::SimpleString("none".into())),
            _ => Err(CommandError::StoreError(error)),
        },
    }
}

#[derive(Debug)]
pub enum CommandResponse {
    Immediate(RedisType),
    WaitForBLPOP {
        timeout: f64,
        receiver: oneshot::Receiver<RedisType>,
        key: Bytes,
        client_id: u64,
    },
    WaitForXREAD {
        timeout: u128,
        receiver: oneshot::Receiver<RedisType>,
        client_id: u64,
    },
}

pub fn handle_command(
    input: RedisType,
    store: &mut Store,
) -> Result<CommandResponse, CommandError> {
    let RedisType::Array(Some(elements)) = input else {
        return Err(CommandError::InvalidInput(
            "The supplied input has an invalid format or redis type: Input needs to be of RedisType::Array".to_string(),
        ));
    };

    let command = argument_as_str(&elements, 0)?.to_ascii_uppercase();

    let arguments = &elements[1..];

    match command.as_str() {
        "PING" => Ok(CommandResponse::Immediate(handle_pong(arguments)?)),
        "ECHO" => Ok(CommandResponse::Immediate(handle_echo(arguments)?)),
        "LRANGE" => Ok(CommandResponse::Immediate(handle_lrange(arguments, store)?)),
        "RPUSH" => Ok(CommandResponse::Immediate(handle_rpush(arguments, store)?)),
        "LPUSH" => Ok(CommandResponse::Immediate(handle_lpush(arguments, store)?)),
        "GET" => Ok(CommandResponse::Immediate(handle_get(arguments, store)?)),
        "SET" => Ok(CommandResponse::Immediate(handle_set(arguments, store)?)),
        "LLEN" => Ok(CommandResponse::Immediate(handle_llen(arguments, store)?)),
        "LPOP" => Ok(CommandResponse::Immediate(handle_lpop(arguments, store)?)),
        "TYPE" => Ok(CommandResponse::Immediate(handle_type(arguments, store)?)),
        "XADD" => Ok(CommandResponse::Immediate(handle_xadd(arguments, store)?)),
        "XRANGE" => Ok(CommandResponse::Immediate(handle_xrange(arguments, store)?)),
        "XREAD" => handle_xread(arguments, store),
        "BLPOP" => handle_blpop(arguments, store),

        _ => Err(CommandError::UnknownCommand(format!(
            "redis command {} not supported",
            command
        ))),
    }
}

impl Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::InvalidInput(message) => write!(f, "Invalid Input: {}", message),
            CommandError::UnknownCommand(message) => write!(f, "Invalid Input: {}", message),
            CommandError::StoreError(store_error) => write!(f, "Store Error: {}", store_error),
        }
    }
}
