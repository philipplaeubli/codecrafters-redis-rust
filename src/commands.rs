use std::{fmt::Display, str::FromStr};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    parser::RedisType,
    store::{Store, StoreError, StreamId},
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
        Err(StoreError::TimeError) => Err(CommandError::InvalidInput(
            "Unable to convert expiry to unix timestamp".into(),
        )),

        Err(StoreError::StreamIdSmallerThanLast) => Err(CommandError::InvalidInput(
            "Stream ID is smaller than the last ID".into(),
        )),
        Err(StoreError::StreamIdNotGreaterThan0) => Err(CommandError::InvalidInput(
            "Stream ID must be greater than 0-0".into(),
        )),
    }
}

fn handle_set(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    if arguments.len() != 2 && arguments.len() != 4 {
        // either it's a simple SET, or it's a SET with an expiry
        return Err(CommandError::InvalidInput(
            "Invalid input: expected 2 or 4 arguments".into(),
        ));
    }

    let key = extract_key(arguments)?;
    let value = argument_as_bytes(arguments, 1)?;

    let mut expiry: Option<u128> = None;
    if arguments.len() == 4 {
        let expiry_unit = argument_as_str(arguments, 2)?;
        let expiry_value: u128 = argument_as_number(arguments, 3)?;

        let unit_factor = match expiry_unit {
            "EX" => 1000,
            "PX" => 1,
            _ => {
                return Err(CommandError::InvalidInput(
                    "Invalid input: expiry unit of SET must be either 'EX' or 'PX'".into(),
                ));
            }
        };
        expiry = Some(expiry_value * unit_factor);
    }

    store
        .set_with_expiry(key.clone(), value.clone(), expiry)
        .map_err(|store_error| match store_error {
            StoreError::TimeError => {
                CommandError::InvalidInput("Unable to convert expiry to unix timestamp".into())
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
        .map_err(CommandError::StoreError)?;

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
        .map_err(CommandError::StoreError)?;

    Ok(RedisType::Integer(new_length as i128))
}

fn handle_lrange(arguments: &[RedisType], store: &Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;
    let start: i128 = argument_as_number(arguments, 1)?;
    let end: i128 = argument_as_number(arguments, 2)?;

    let result = store.lrange(key.clone(), start, end);

    let response = if let Ok(values) = result {
        RedisType::Array(Some(
            values.into_iter().map(RedisType::BulkString).collect(),
        ))
    } else {
        RedisType::Array(Some(vec![]))
    };
    Ok(response)
}

fn handle_llen(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let len = store.llen(key).map_err(CommandError::StoreError)?;

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
                        .map(RedisType::BulkString)
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
            values.into_iter().map(RedisType::BulkString).collect(),
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

fn handle_type(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;
    let key_type = store.get_type(key).map_err(CommandError::StoreError)?;
    Ok(RedisType::SimpleString(key_type))
}

fn handle_xadd(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let (ms, seq) = extract_stream_id_values(&arguments[1])?;

    match store.xadd(key, seq, ms, &arguments[2..]) {
        Ok(id) => Ok(id.into()),
        Err(StoreError::StreamIdSmallerThanLast) => Ok(RedisType::SimpleError(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .into(),
        )),
        Err(StoreError::StreamIdNotGreaterThan0) => Ok(RedisType::SimpleError(
            "ERR The ID specified in XADD must be greater than 0-0".into(),
        )),
        Err(other) => Err(CommandError::InvalidInput(format!(
            "Unable to add to stream: {:?}",
            other
        ))),
    }
}

fn extract_stream_id_values(
    argument: &RedisType,
) -> Result<(Option<u128>, Option<u128>), CommandError> {
    let (ms, seq) = match argument {
        RedisType::BulkString(bytes) => {
            // find the separator of the stream id
            let pos = bytes.windows(1).position(|char| char == [b'-']);

            if let Some(pos) = pos
                && pos > 0
                && pos < bytes.len()
            {
                let ms_slice = &bytes[0..pos];
                let seq_slice = &bytes[pos + 1..];
                let parse_err =
                    || CommandError::InvalidInput("Unable to parse stream key".to_string());
                let ms = if ms_slice == b"*" {
                    None
                } else {
                    let ms = str::from_utf8(ms_slice)
                        .map_err(|_| parse_err())?
                        .parse()
                        .map_err(|_| parse_err())?;
                    Some(ms)
                };
                let seq = if seq_slice == b"*" {
                    None
                } else {
                    let seq = str::from_utf8(seq_slice)
                        .map_err(|_| parse_err())?
                        .parse::<u128>()
                        .map_err(|_| parse_err())?;

                    Some(seq)
                };

                (ms, seq)
            } else {
                (None, None)
            }
        }
        _ => {
            return Err(CommandError::InvalidInput(
                "Stream id must be bulk string".to_string(),
            ));
        }
    };
    Ok((ms, seq))
}

fn handle_xrange(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let stream_key = extract_key(arguments)?;
    let (start_ms, start_sq) = extract_stream_id_values(&arguments[1])?;
    let (end_ms, end_sq) = extract_stream_id_values(&arguments[2])?;

    let start_stream_id = start_ms
        .map(|start_ms| {
            Some(StreamId {
                ms: start_ms,
                seq: start_sq.unwrap_or(0),
            })
        })
        .unwrap_or(None);

    let end_stream_id = end_ms
        .map(|end_ms| {
            Some(StreamId {
                ms: end_ms,
                seq: end_sq.unwrap_or(0),
            })
        })
        .unwrap_or(None);

    let result: Vec<RedisType> = store
        .xrange(stream_key, start_stream_id, end_stream_id)
        .iter()
        .map(|(id, map)| {
            RedisType::Array(Some(vec![
                id.into(),
                RedisType::Array(Some(
                    map.iter()
                        .flat_map(|(key, value)| [key.clone().into(), value.clone().into()])
                        .collect(),
                )),
            ]))
        })
        .collect();
    Ok(RedisType::Array(Some(result)))
}

fn handle_xread(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let xread_args = &arguments[1..];

    let (stream_keys, stream_ids) = xread_args.split_at(xread_args.len() / 2);

    let keys: Vec<&Bytes> = stream_keys
        .iter()
        .map(redis_type_as_bytes) // -> Result<&Bytes, CommandError>
        .collect::<Result<Vec<_>, _>>()?;

    let ids: Vec<StreamId> = stream_ids
        .iter()
        .map(extract_stream_id_values) // -> Result<&Bytes, CommandError>
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .map(|(ms, seq)| StreamId {
            ms: ms.unwrap_or(0),
            seq: seq.unwrap_or(0),
        })
        .collect();
    let keys_and_ids: Vec<(&Bytes, StreamId)> = keys.into_iter().zip(ids.into_iter()).collect();
    let result = keys_and_ids
        .into_iter()
        .map(|(key, stream)| {
            // super double array encoding
            RedisType::Array(Some(vec![
                RedisType::BulkString(key.clone()),
                RedisType::Array(Some(
                    store
                        .xread(key, stream)
                        .iter()
                        .map(|(id, map)| {
                            RedisType::Array(Some(vec![
                                id.into(),
                                RedisType::Array(Some(
                                    map.iter()
                                        .flat_map(|(key, value)| {
                                            [key.clone().into(), value.clone().into()]
                                        })
                                        .collect(),
                                )),
                            ]))
                        })
                        .collect(),
                )),
            ]))
        })
        .collect();

    Ok(RedisType::Array(Some(result)))
}

fn argument_as_bytes(arguments: &[RedisType], index: usize) -> Result<&Bytes, CommandError> {
    let bytes = match arguments.get(index) {
        Some(RedisType::BulkString(b)) => b,
        Some(RedisType::SimpleString(b)) => b,
        _ => {
            return Err(CommandError::InvalidInput(
                "Invalid argument: Must be a bulkstring".into(),
            ));
        }
    };
    Ok(bytes)
}
fn redis_type_as_bytes(redis_type: &RedisType) -> Result<&Bytes, CommandError> {
    match redis_type {
        RedisType::BulkString(b) => Ok(b),
        RedisType::SimpleString(b) => Ok(b),
        _ => Err(CommandError::InvalidInput(
            "Invalid argument: Must be a bulkstring".into(),
        )),
    }
}

fn extract_key(arguments: &[RedisType]) -> Result<&Bytes, CommandError> {
    argument_as_bytes(arguments, 0)
}
fn argument_as_str(arguments: &[RedisType], index: usize) -> Result<&str, CommandError> {
    match arguments.get(index) {
        Some(RedisType::BulkString(b)) => str::from_utf8(b).map_err(|_| {
            CommandError::InvalidInput("Invalid argument: Must be a valid UTF-8 string".into())
        }),
        _ => {
            return Err(CommandError::InvalidInput(
                "Invalid argument: Must be a bulkstring".into(),
            ));
        }
    }
}

fn argument_as_number<T>(arguments: &[RedisType], index: usize) -> Result<T, CommandError>
where
    T: FromStr,
{
    argument_as_str(arguments, index)?
        .parse::<T>()
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
        "TYPE" => Ok(CommandResponse::Immediate(handle_type(arguments, store)?)),
        "XADD" => Ok(CommandResponse::Immediate(handle_xadd(arguments, store)?)),
        "XRANGE" => Ok(CommandResponse::Immediate(handle_xrange(arguments, store)?)),
        "XREAD" => Ok(CommandResponse::Immediate(handle_xread(arguments, store)?)),
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
