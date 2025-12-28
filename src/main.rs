#![allow(unused_imports)]
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::IntoStringError,
    hash::RandomState,
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
    commands::{CommandError, handle_command},
    parser::{RedisData, RedisType, RespParseError, parse_resp},
    store::{SharedStore, Store},
};
mod commands;
mod parser;
mod store;

#[derive(Debug)]
enum RedisError {
    ParseError(RespParseError),
    CommandError(CommandError),
    IoError(io::Error),
}

async fn handle_connection(mut stream: TcpStream, store: SharedStore) -> Result<(), RedisError> {
    let mut buffer = [0; 1024];
    loop {
        let read_length = stream
            .read(&mut buffer)
            .await
            .map_err(|io_error| RedisError::IoError(io_error))?;

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
        let result =
            parse_resp(&buffer[0..read_length]).map_err(|err| RedisError::ParseError(err))?;
        println!("Parsed response: {:?}", result);
        let response = handle_command(result, &store)
            .await
            .map_err(|command_error| RedisError::CommandError(command_error))?;

        stream
            .write_all(response.as_bytes())
            .await
            .map_err(|io_error| RedisError::IoError(io_error))?;
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
