#![allow(unused_imports)]
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::IntoStringError,
    io::{BufRead, BufReader, Error, Read, Write},
    sync::Arc,
    thread,
};

use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::{
    parser::{RedisType, RespParseError, parse_resp},
    store::{SharedStore, Store},
};
mod parser;
mod store;
async fn handle_input(input: RedisType, store: &SharedStore) -> Result<String, RespParseError> {
    let RedisType::Array(elements) = input else {
        unreachable!("parse_array must return RedisType::Array")
    };

    let first_element = elements.first().ok_or(RespParseError::InvalidFormat)?;

    match first_element {
        RedisType::BulkString(s) => {
            let command = s.to_string().to_uppercase();
            match command.as_str() {
                "PING" => Ok("+PONG\r\n".to_string()),

                "ECHO" => {
                    let some = &elements[1..];
                    let message = some
                        .iter()
                        .map(|f| match f {
                            RedisType::BulkString(value) => value,
                            _ => "",
                        })
                        .collect::<Vec<&str>>()
                        .join(" ");

                    Ok(format!("${}\r\n{}\r\n", message.len(), message))
                }
                "GET" => {
                    let key = match elements.get(1) {
                        Some(RedisType::BulkString(value)) => value,
                        _ => return Err(RespParseError::KeyNotFound),
                    };
                    let reader = store.read().await;
                    let value = reader.get(key);
                    match value {
                        Ok(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                        Err(RespParseError::KeyExpired) => Ok("$-1\r\n".to_string()), // we handle key expiration and return a null bulk string
                        Err(error) => Err(error), // handle other errors somewhere else
                    }
                }
                "SET" => {
                    if elements.len() != 3 && elements.len() != 5 {
                        // either it's a simple set, or it's a set with an expiry
                        return Err(RespParseError::InvalidFormat);
                    }

                    let key = match elements.get(1) {
                        Some(RedisType::BulkString(value)) => value,
                        _ => return Err(RespParseError::KeyNotFound),
                    };
                    let value = match elements.get(2) {
                        Some(RedisType::BulkString(value)) => value,
                        _ => return Err(RespParseError::KeyNotFound),
                    };
                    let mut expiry: Option<u128> = None;
                    if elements.len() == 5 {
                        let expiry_unit = match elements.get(3) {
                            Some(RedisType::BulkString(value)) => value,
                            _ => return Err(RespParseError::KeyNotFound),
                        };

                        let expiry_as_string = match elements.get(4) {
                            Some(RedisType::BulkString(value)) => value,
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
                    writer.set_with_expiry(key, value, expiry)?;

                    Ok("+OK\r\n".to_string())
                }
                _ => Err(RespParseError::InvalidFormat),
            }
        }
        _ => Err(RespParseError::InvalidFormat),
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
) -> Result<(), RespParseError> {
    let mut buffer = [0; 1024];
    loop {
        let read_length = stream.read(&mut buffer).await?;

        if read_length == 0 {
            println!("Received empty input");
            break;
        }
        let input_str = match str::from_utf8(&buffer[0..read_length]) {
            Ok(input_str) => input_str,
            Err(_) => {
                println!("Received invalid input");
                break;
            }
        };
        println!("Received input: {:?}", input_str);
        let result = parse_resp(&buffer[0..read_length])?;
        println!("Parsed response: {:?}", result);
        let response = handle_input(result, &store).await?;

        stream.write_all(response.as_bytes()).await?
    }
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let redis_address =
        std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    let tcp_listener = TcpListener::bind(&redis_address).await?;

    // setting up the central data store (ARC at the moment / automated referece counting)
    let store: SharedStore = Arc::new(RwLock::new(Store::new()));

    println!("Listening on {} - awaiting connections", redis_address);
    loop {
        let (stream, _addr) = tcp_listener.accept().await?;
        println!("Accepted connection from client");

        let store = store.clone(); // this does not clone it (because it's an Arc type, it only increases the reference count)

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store).await {
                eprintln!("Error during connection handling: {:?}", e);
            }
        });
    }
}
