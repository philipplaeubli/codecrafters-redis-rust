use std::collections::HashMap;

use bytes::Bytes;

use crate::{parser::RedisType, store::StreamId};

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

    RedisType::Array(Some(vec![RedisType::Array(Some(vec![
        RedisType::BulkString(key),
        RedisType::Array(Some(res)),
    ]))]))
}
