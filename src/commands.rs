use std::str::FromStr;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    parser::RedisType,
    store::{Store, StoreError},
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

fn handle_get(arguments: &[RedisType], store: &Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let value = store.get(key.clone());
    match value {
        Ok(value) => Ok(RedisType::BulkString(value.clone())),
        Err(StoreError::KeyExpired) => Ok(RedisType::NullBulkString), // we handle key expiration and return a null bulk string
        Err(StoreError::KeyNotFound) => Ok(RedisType::NullBulkString),
        Err(StoreError::TimeError) => Err(CommandError::InvalidInput(format!(
            "Unable to convert expiry to unix timestamp"
        ))),
    }
}

fn handle_set(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    if arguments.len() != 2 && arguments.len() != 4 {
        // either it's a simple SET, or it's a SET with an expiry
        return Err(CommandError::InvalidInput(format!(
            "Invalid input: expected 2 or 4 arguments"
        )));
    }

    let key = extract_key(arguments)?;
    let value = argument_as_bytes(arguments, 1)?;

    let mut expiry: Option<u128> = None;
    if arguments.len() == 4 {
        let expiry_unit = argument_as_str(&arguments, 2)?;
        let expiry_value: u128 = argument_as_number(&arguments, 3)?;

        let unit_factor = match expiry_unit {
            "EX" => 1000,
            "PX" => 1,
            _ => {
                return Err(CommandError::InvalidInput(format!(
                    "Invalid input: expiry unit of SET must be either 'EX' or 'PX'"
                )));
            }
        };
        expiry = Some(expiry_value * unit_factor);
    }

    store
        .set_with_expiry(key.clone(), value.clone(), expiry)
        .map_err(|store_error| match store_error {
            StoreError::TimeError => {
                CommandError::InvalidInput(format!("Unable to convert expiry to unix timestamp"))
            }
            _ => CommandError::StoreError(store_error),
        })?;
    Ok(RedisType::SimpleString(Bytes::from_static(b"OK")))
}

fn handle_rpush(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let values = arguments[1..]
        .iter()
        .filter_map(|arg| match arg {
            RedisType::BulkString(value) => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<Bytes>>();

    let new_length = store
        .rpush(key.clone(), values)
        .map_err(|store_error| CommandError::StoreError(store_error))?;

    Ok(RedisType::Integer(new_length as i128))
}

fn handle_lpush(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let values = arguments[1..]
        .iter()
        .filter_map(|arg| match arg {
            RedisType::BulkString(value) => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<Bytes>>();
    let new_length = store
        .lpush(key.clone(), values)
        .map_err(|store_error| CommandError::StoreError(store_error))?;

    Ok(RedisType::Integer(new_length as i128))
}

fn handle_lrange(arguments: &[RedisType], store: &Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;
    let start: i128 = argument_as_number(arguments, 1)?;
    let end: i128 = argument_as_number(arguments, 2)?;

    let result = store.lrange(key.clone(), start, end);

    let response = if let Ok(values) = result {
        RedisType::Array(Some(
            values
                .into_iter()
                .map(|v| RedisType::BulkString(v))
                .collect(),
        ))
    } else {
        RedisType::Array(Some(vec![]))
    };
    Ok(response)
}

fn handle_llen(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let len = store
        .llen(key.clone())
        .map_err(|store_error| CommandError::StoreError(store_error))?;

    Ok(RedisType::Integer(len as i128))
}

fn handle_lpop(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;
    let mut amount = 1;

    if arguments.len() > 1 {
        amount = argument_as_number(arguments, 1)?;
    }

    let removed_elements = store.lpop(key.clone(), amount);

    match removed_elements {
        Ok(removed_elements) => {
            if removed_elements.is_empty() {
                Ok(RedisType::NullBulkString)
            } else if removed_elements.len() == 1 {
                let element = &removed_elements[0];
                Ok(RedisType::BulkString(element.clone()))
            } else {
                let resp = RedisType::Array(Some(
                    removed_elements
                        .into_iter()
                        .map(|element| RedisType::BulkString(element.clone()))
                        .collect(),
                ));
                Ok(resp)
            }
        }
        Err(StoreError::KeyNotFound) => Ok(RedisType::NullBulkString),
        Err(err) => Err(CommandError::StoreError(err)),
    }
}

fn handle_blpop(
    arguments: &[RedisType],
    store: &mut Store,
) -> Result<CommandResponse, CommandError> {
    let key = extract_key(arguments)?;
    let timeout: f64 = argument_as_number(arguments, 1)?;

    // Check if data available first
    if let Some(values) = store.lpop_for_blpop(key) {
        // Data available - send immediately
        let response = RedisType::Array(Some(
            values
                .into_iter()
                .map(|element| RedisType::BulkString(element))
                .collect(),
        ));
        return Ok(CommandResponse::Immediate(response));
    }

    // No data - register for waiting
    let (tx, rx) = oneshot::channel();
    let identifier = store.register_waiting_client(key.clone(), tx);
    println!(
        "Waiting with timeout {} for client: {}",
        timeout, identifier
    );
    Ok(CommandResponse::Wait {
        timeout,
        receiver: rx,
        key: key.clone(),
        client_id: identifier,
    })
}

fn argument_as_bytes(arguments: &[RedisType], index: usize) -> Result<&Bytes, CommandError> {
    let bytes = match arguments.get(index) {
        Some(RedisType::BulkString(b)) => b,
        Some(RedisType::SimpleString(b)) => b,
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "Invalid argument: Must be a bulkstring"
            )));
        }
    };
    Ok(bytes)
}
fn extract_key(arguments: &[RedisType]) -> Result<&Bytes, CommandError> {
    argument_as_bytes(arguments, 0)
}
fn argument_as_str(arguments: &[RedisType], index: usize) -> Result<&str, CommandError> {
    let bytes = match arguments.get(index) {
        Some(RedisType::BulkString(b)) => b,
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "Invalid argument: Must be a bulkstring"
            )));
        }
    };

    str::from_utf8(bytes).map_err(|_| {
        CommandError::InvalidInput(format!("Invalid argument: Must be a valid UTF-8 string"))
    })
}

fn argument_as_number<T>(arguments: &[RedisType], index: usize) -> Result<T, CommandError>
where
    T: FromStr,
{
    let s = argument_as_str(arguments, index)?;
    s.parse::<T>()
        .map_err(|_| CommandError::InvalidInput("Unable to parse argument to a number".into()))
}

#[derive(Debug)]
pub enum CommandResponse {
    Immediate(RedisType),
    Wait {
        timeout: f64,
        receiver: oneshot::Receiver<RedisType>,
        key: Bytes,
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
        "BLPOP" => handle_blpop(arguments, store),

        _ => Err(CommandError::UnknownCommand(format!(
            "redis command {} not supported",
            command
        ))),
    }
}
