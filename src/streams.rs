use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    command_utils::{argument_as_number, argument_as_str, extract_key, redis_type_as_bytes},
    commands::{CommandError, CommandResponse},
    parser::RedisType,
    store::{Store, StoreError, StreamId},
};

pub fn handle_xadd(arguments: &[RedisType], store: &mut Store) -> Result<RedisType, CommandError> {
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
pub fn handle_xrange(
    arguments: &[RedisType],
    store: &mut Store,
) -> Result<RedisType, CommandError> {
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

fn handle_xread_immediate(
    keys_and_ids: &[RedisType],
    store: &mut Store,
) -> Result<RedisType, CommandError> {
    let (stream_keys, stream_ids) = keys_and_ids.split_at(keys_and_ids.len() / 2);

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
    let keys_and_ids: Vec<(&Bytes, StreamId)> = keys.into_iter().zip(ids).collect();
    let result = keys_and_ids
        .into_iter()
        .map(|(key, stream)| {
            xread_output_to_redis_type(key.clone(), store.xread(key, stream, false))
        })
        .collect();

    Ok(RedisType::Array(Some(result)))
}

pub fn handle_xread(
    arguments: &[RedisType],
    store: &mut Store,
) -> Result<CommandResponse, CommandError> {
    let possible_block = argument_as_str(arguments, 0)?;

    if possible_block.to_uppercase() == "BLOCK" {
        let timeout: u128 = argument_as_number(arguments, 1)?;
        let last_argument = argument_as_str(arguments, arguments.len() - 1)?;
        let keys_and_ids = &arguments[3..];

        let resp = handle_xread_immediate(keys_and_ids, store)?;
        if let RedisType::Array(Some(array)) = &resp
            && !array.is_empty()
        {
            // data structure is [[id, [field, value]]] -> [field, value] is empty -> no data
            let has_some_content = array
                .first()
                .and_then(|first_inner| {
                    if let RedisType::Array(Some(some_inner)) = &first_inner {
                        Some(some_inner)
                    } else {
                        None
                    }
                })
                .map(|first_inner| {
                    first_inner.iter().any(
                        |item| matches!(item, RedisType::Array(Some(inner)) if !inner.is_empty()),
                    )
                })
                .unwrap_or(false);

            if has_some_content && last_argument != "$" {
                Ok(CommandResponse::Immediate(resp))
            } else {
                // No data - register for waiting
                let keys_only = keys_and_ids.split_at(keys_and_ids.len() / 2).0.to_vec();
                let key_as_bytes: Vec<Bytes> = keys_only
                    .iter()
                    .map(redis_type_as_bytes)
                    .collect::<Result<Vec<&Bytes>, _>>()?
                    .into_iter()
                    .cloned()
                    .collect();

                let (tx, rx) = oneshot::channel();
                let identifier = store.register_xread_waiting_client(key_as_bytes, tx);
                println!(
                    "XREAD Waiting with timeout {} for client: {}",
                    timeout, identifier
                );

                Ok(CommandResponse::WaitForXREAD {
                    timeout,
                    receiver: rx,
                    client_id: identifier,
                })
            }

            // May be not enough to just check the outmost array for data.
        } else {
            Ok(CommandResponse::Immediate(resp))
        }
    } else {
        let keys_and_ids = &arguments[1..];
        let resp = handle_xread_immediate(keys_and_ids, store)?;
        Ok(CommandResponse::Immediate(resp))
    }
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
