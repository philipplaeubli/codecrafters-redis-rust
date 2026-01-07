use bytes::Bytes;
use tokio::sync::oneshot;

use super::{
    CommandError, CommandResponse,
    utils::{argument_as_number, extract_key},
};
use crate::{
    parser::RedisType,
    store::{Store, StoreError},
};

pub fn handle_rpush(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
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

pub fn handle_lpush(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
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

pub fn handle_lrange(arguments: &[RedisType], store: &Store) -> Result<RedisType, CommandError> {
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

pub fn handle_llen(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
    let key = extract_key(arguments)?;

    let len = store.llen(key).map_err(CommandError::StoreError)?;

    Ok(RedisType::Integer(len as i128))
}

pub fn handle_lpop(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
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

pub fn handle_blpop(
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
    let identifier = store.register_blpop_waiting_client(key.clone(), tx);
    println!(
        "Waiting with timeout {} for client: {}",
        timeout, identifier
    );
    Ok(CommandResponse::WaitForBLPOP {
        timeout,
        receiver: rx,
        key: key.clone(),
        client_id: identifier,
    })
}
