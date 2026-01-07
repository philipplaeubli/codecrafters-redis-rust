use bytes::Bytes;

use crate::{
    command_utils::{argument_as_bytes, argument_as_number, argument_as_str, extract_key},
    commands::CommandError,
    parser::RedisType,
    store::{Store, StoreError},
};

pub fn handle_get(arguments: &[RedisType], store: &Store) -> Result<RedisType, CommandError> {
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
pub fn handle_set(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
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
