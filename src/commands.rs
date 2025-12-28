use crate::{
    parser::{RedisData, RedisType, RespParseError},
    store::SharedStore,
};

fn handle_pong(arguments: &[RedisType]) -> Result<String, RespParseError> {
    if !arguments.is_empty() {
        // as per https://redis.io/docs/latest/commands/ping/, ping should return the arguments passed to it
        return handle_echo(arguments);
    }
    Ok("+PONG\r\n".to_string())
}

fn handle_echo(arguments: &[RedisType]) -> Result<String, RespParseError> {
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

async fn handle_get(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, RespParseError> {
    let key = match arguments.first() {
        Some(RedisType::BulkString(value)) => value.data.clone(),
        _ => return Err(RespParseError::KeyNotFound),
    };
    let reader = store.read().await;
    let value = reader.get(key.as_str());
    match value {
        Ok(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
        Err(RespParseError::KeyExpired) => Ok("$-1\r\n".to_string()), // we handle key expiration and return a null bulk string
        Err(error) => Err(error), // handle other errors somewhere else
    }
}

async fn handle_set(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, RespParseError> {
    if arguments.len() != 2 && arguments.len() != 4 {
        // either it's a simple SET, or it's a SET with an expiry
        return Err(RespParseError::InvalidFormat);
    }

    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => return Err(RespParseError::KeyNotFound),
    };
    let value = match &arguments[1] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => return Err(RespParseError::KeyNotFound),
    };
    let mut expiry: Option<u128> = None;
    if arguments.len() == 4 {
        let expiry_unit = match &arguments[2] {
            RedisType::BulkString(value) => value.data.clone(),
            _ => return Err(RespParseError::KeyNotFound),
        };

        let expiry_as_string = match &arguments[3] {
            RedisType::BulkString(value) => value.data.clone(),
            _ => return Err(RespParseError::KeyNotFound),
        };
        let expiry_value: u128 = expiry_as_string.parse::<u128>()?;
        let unit_factor = match expiry_unit.as_str() {
            "EX" => 1000,
            "PX" => 1,
            _ => return Err(RespParseError::InvalidFormat),
        };
        expiry = Some(expiry_value * unit_factor);
    }

    let mut writer = store.write().await;
    writer.set_with_expiry(key.as_str(), value.as_str(), expiry)?;

    Ok("+OK\r\n".to_string())
}

async fn handle_rpush(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, RespParseError> {
    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => return Err(RespParseError::InvalidFormat),
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
    let new_length = writer.rpush(key.as_str(), values)?;

    Ok(format!(":{}\r\n", new_length))
}

async fn handle_lrange(
    arguments: &[RedisType],
    store: &SharedStore,
) -> Result<String, RespParseError> {
    let key = match &arguments[0] {
        RedisType::BulkString(value) => value.data.clone(),
        _ => return Err(RespParseError::InvalidFormat),
    };

    let start = match &arguments[1] {
        RedisType::BulkString(value) => value
            .data
            .parse::<i128>()
            .map_err(|_| RespParseError::InvalidFormat)?,
        _ => return Err(RespParseError::InvalidFormat),
    };
    let end = match &arguments[2] {
        RedisType::BulkString(value) => value
            .data
            .parse::<i128>()
            .map_err(|_| RespParseError::InvalidFormat)?,
        _ => return Err(RespParseError::InvalidFormat),
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

pub async fn handle_command(
    input: RedisType,
    store: &SharedStore,
) -> Result<String, RespParseError> {
    let RedisType::Array(elements) = input else {
        unreachable!("parse_array must return RedisType::Array")
    };

    let first_element = elements.data.first().ok_or(RespParseError::InvalidFormat)?;
    let arguments = &elements.data[1..];
    match first_element {
        RedisType::BulkString(value) => {
            let command = value.data.to_uppercase();
            match command.as_str() {
                "PING" => handle_pong(arguments),
                "ECHO" => handle_echo(arguments),
                "LRANGE" => handle_lrange(arguments, store).await,
                "RPUSH" => handle_rpush(arguments, store).await,
                "GET" => handle_get(arguments, store).await,
                "SET" => handle_set(arguments, store).await,
                _ => Err(RespParseError::InvalidFormat),
            }
        }
        _ => Err(RespParseError::InvalidFormat),
    }
}
