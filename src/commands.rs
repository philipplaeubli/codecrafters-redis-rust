use std::{
    fmt::format,
    sync::{Arc, RwLock},
};

use crate::{
    parser::{RedisData, RedisType, RespParseError},
    store::{SharedStore, Store, StoreError},
};

#[derive(Debug)]
pub enum CommandError {
    InvalidInput(String),
    UnknownCommand(String),
    StoreError(StoreError),
}

fn handle_pong(arguments: &[RedisType]) -> Result<String, CommandError> {
    if !arguments.is_empty() {
        // as per https://redis.io/docs/latest/commands/ping/, ping should return the arguments passed to it
        return handle_echo(arguments);
    }
    Ok("+PONG\r\n".to_string())
}

fn handle_echo(arguments: &[RedisType]) -> Result<String, CommandError> {
    let message = arguments
        .iter()
        .map(|f| match f {
            RedisType::BulkString(value) => value.data.clone(),
            _ => "".to_string(),
        })
        .filter(|s| !s.is_empty())
        .collect::<Vec<String>>()
        .join(" ");
    let result = RedisType::BulkString(RedisData {
        data: message,
        buffer_length: 0,
    });
    Ok(result.to_string())
}

async fn handle_get(arguments: &[RedisType], store: &SharedStore) -> Result<String, CommandError> {
    let key = match arguments.first() {
        Some(RedisType::BulkString(value)) => value.data.clone(),
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "Key not found or not of correct type (must be BulkString)"
            )));
        }
    };
    let reader = store.read().await;
    let value = reader.get(key.as_str());
    match value {
        Ok(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
        Err(StoreError::KeyExpired) => Ok("$-1\r\n".to_string()), // we handle key expiration and return a null bulk string
        Err(StoreError::KeyNotFound) => Ok("$-1\r\n".to_string()),
        Err(StoreError::TimeError) => Err(CommandError::InvalidInput(format!(
            "Unable to convert expiry to unix timestamp"
        ))),
    }
}

async fn handle_set(arguments: &[RedisType], store: &SharedStore) -> Result<String, CommandError> {
    if arguments.len() != 2 && arguments.len() != 4 {
        // either it's a simple SET, or it's a SET with an expiry
        return Err(CommandError::InvalidInput(format!(
            "Invalid input: expected 2 or 4 arguments"
        )));
    }

    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "Invalid input: first argument of SET must be a key of type bulkstring"
            )));
        }
    };
    let value = match &arguments[1] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "Invalid input: second argument of SET must be a value of type bulkstring"
            )));
        }
    };
    let mut expiry: Option<u128> = None;
    if arguments.len() == 4 {
        let expiry_unit = match &arguments[2] {
            RedisType::BulkString(value) => value.data.clone(),
            _ => {
                return Err(CommandError::InvalidInput(format!(
                    "Invalid input: expiry unit of SET must be a bulkstring"
                )));
            }
        };

        let expiry_as_string = match &arguments[3] {
            RedisType::BulkString(value) => value.data.clone(),
            _ => {
                return Err(CommandError::InvalidInput(format!(
                    "Invalid input: expiry value of SET must be a bulkstring"
                )));
            }
        };
        let expiry_value: u128 = expiry_as_string.parse::<u128>().map_err(|_| {
            CommandError::InvalidInput(format!("Unable to parse expiry to a number"))
        })?;
        let unit_factor = match expiry_unit.as_str() {
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

    let mut writer = store.write().await;
    writer
        .set_with_expiry(key.as_str(), value.as_str(), expiry)
        .map_err(|store_error| match store_error {
            StoreError::TimeError => {
                CommandError::InvalidInput(format!("Unable to convert expiry to unix timestamp"))
            }
            _ => CommandError::StoreError(store_error),
        })?;

    Ok("+OK\r\n".to_string())
}

async fn handle_rpush(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, CommandError> {
    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "First argument / key of RPUSH must be a bulkstring"
            )));
        }
    };

    let values = arguments[1..]
        .iter()
        .map(|f| match f {
            RedisType::BulkString(value) => value.data.to_owned(),
            _ => "".to_owned(),
        })
        .filter(|val| !val.is_empty())
        .collect::<Vec<String>>();

    let mut writer = store.write().await;
    let new_length = writer
        .rpush(key.as_str(), values)
        .map_err(|store_error| CommandError::StoreError(store_error))?;

    Ok(format!(":{}\r\n", new_length))
}

async fn handle_lpush(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, CommandError> {
    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "First argument / key of LPUSH must be a bulkstring"
            )));
        }
    };

    let values = arguments[1..]
        .iter()
        .map(|f| match f {
            RedisType::BulkString(value) => value.data.to_owned(),
            _ => "".to_owned(),
        })
        .filter(|val| !val.is_empty())
        .collect::<Vec<String>>();
    let mut writer = store.write().await;
    let new_length = writer
        .lpush(key.as_str(), values)
        .map_err(|store_error| CommandError::StoreError(store_error))?;

    Ok(format!(":{}\r\n", new_length))
}

async fn handle_lrange(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, CommandError> {
    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "First argument / key of LRANGE must be a bulkstring"
            )));
        }
    };

    let start = match &arguments[1] {
        RedisType::BulkString(value) => value.data.parse::<i128>().map_err(|_| {
            CommandError::InvalidInput(format!(
                "Unable to convert start/from argument of LRANGE to number"
            ))
        })?,
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "Start/from argument of LRANGE must be a bulkstring"
            )));
        }
    };
    let end = match &arguments[2] {
        RedisType::BulkString(value) => value.data.parse::<i128>().map_err(|_| {
            CommandError::InvalidInput(format!(
                "Unable to convert end/to argument of LRANGE to number"
            ))
        })?,
        _ => {
            return Err(CommandError::InvalidInput(format!(
                "End/to argument of LRANGE must be a bulkstring"
            )));
        }
    };
    let reader = store.read().await;
    let result = reader.lrange(key.as_str(), start, end);
    let response = if let Ok(values) = result {
        RedisType::Array(RedisData {
            data: values
                .into_iter()
                .map(|v| {
                    RedisType::BulkString(RedisData {
                        data: v,
                        buffer_length: 0,
                    })
                })
                .collect(),
            buffer_length: 0,
        })
    } else {
        RedisType::Array(RedisData {
            data: vec![],
            buffer_length: 0,
        })
    };
    Ok(response.to_string())
}

pub async fn handle_command(input: RedisType, store: &SharedStore) -> Result<String, CommandError> {
    let RedisType::Array(elements) = input else {
        return Err(CommandError::InvalidInput(
            "The supplied input has an invalid format or redis type: Input needs to be of RedisType::Array".to_string(),
        ));
    };

    let first_element = elements.data.first().ok_or(CommandError::InvalidInput(
        "The input is empty and cannot be processed".to_string(),
    ))?;
    let arguments = &elements.data[1..];
    match first_element {
        RedisType::BulkString(value) => {
            let command = value.data.to_uppercase();
            match command.as_str() {
                "PING" => handle_pong(arguments),
                "ECHO" => handle_echo(arguments),
                "LRANGE" => handle_lrange(arguments, store).await,
                "RPUSH" => handle_rpush(arguments, store).await,
                "LPUSH" => handle_lpush(arguments, store).await,
                "GET" => handle_get(arguments, store).await,
                "SET" => handle_set(arguments, store).await,
                _ => Err(CommandError::UnknownCommand(format!("redis command {} not supported", command))),
            }
        }
        _ => Err(CommandError::InvalidInput(
            "The supplied command has an invalid format or redis type: The first argument should be a RedisType::BulkString.".to_string(),
        )),
    }
}
