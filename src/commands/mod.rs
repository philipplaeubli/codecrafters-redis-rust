use std::fmt::Display;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{parser::RedisType, store::Store};

mod keys;
mod lists;
mod misc;
mod streams;
pub mod utils;

use keys::{handle_get, handle_set};
use lists::{handle_blpop, handle_llen, handle_lpop, handle_lpush, handle_lrange, handle_rpush};
use misc::{handle_echo, handle_ping, handle_type};
pub use streams::xread_output_to_redis_type;
use streams::{handle_xadd, handle_xrange, handle_xread};
use utils::argument_as_str;

use crate::store::StoreError;

#[derive(Debug)]
pub enum CommandError {
    InvalidInput(String),
    UnknownCommand(String),
    StoreError(StoreError),
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
        "PING" => Ok(CommandResponse::Immediate(handle_ping(arguments)?)),
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
