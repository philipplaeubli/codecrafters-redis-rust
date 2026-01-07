use std::{collections::HashMap, str::FromStr};

use bytes::Bytes;

use super::CommandError;
use crate::{parser::RedisType, store::StreamId};

pub fn argument_as_bytes(arguments: &[RedisType], index: usize) -> Result<&Bytes, CommandError> {
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

pub fn redis_type_as_bytes(redis_type: &RedisType) -> Result<&Bytes, CommandError> {
    match redis_type {
        RedisType::BulkString(b) => Ok(b),
        RedisType::SimpleString(b) => Ok(b),
        _ => Err(CommandError::InvalidInput(
            "Invalid argument: Must be a bulkstring".into(),
        )),
    }
}

pub fn extract_key(arguments: &[RedisType]) -> Result<&Bytes, CommandError> {
    argument_as_bytes(arguments, 0)
}

pub fn argument_as_str(arguments: &[RedisType], index: usize) -> Result<&str, CommandError> {
    match arguments.get(index) {
        Some(RedisType::BulkString(b)) => str::from_utf8(b).map_err(|_| {
            CommandError::InvalidInput("Invalid argument: Must be a valid UTF-8 string".into())
        }),
        _ => Err(CommandError::InvalidInput(
            "Invalid argument: Must be a bulkstring".into(),
        )),
    }
}

pub fn argument_as_number<T>(arguments: &[RedisType], index: usize) -> Result<T, CommandError>
where
    T: FromStr,
{
    argument_as_str(arguments, index)?
        .parse::<T>()
        .map_err(|_| CommandError::InvalidInput("Unable to parse argument to a number".into()))
}

pub fn xread_output_to_redis_type(
    key: Bytes,
    input: Vec<(StreamId, HashMap<Bytes, Bytes>)>,
) -> RedisType {
    let res: Vec<RedisType> = input
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

    RedisType::Array(Some(vec![
        RedisType::BulkString(key),
        RedisType::Array(Some(res)),
    ]))
}
