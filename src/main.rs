#![allow(unused_imports)]
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::IntoStringError,
    hash::RandomState,
    io::{BufRead, BufReader, Error, Read, Write},
    thread,
};

use bytes::BytesMut;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        RwLock,
        mpsc::{self, Sender},
        oneshot,
    },
};

use crate::{
    commands::{CommandError, handle_command},
    parser::{RedisType, RespParseError, parse_resp},
    store::Store,
};
mod commands;
mod parser;
mod store;

#[derive(Debug)]
enum RedisError {
    ParseError(RespParseError),
    CommandError(CommandError),
    IoError(io::Error),
    TokioError,
}
#[derive(Debug)]
struct RedisMessage {
    message: RedisType,
    reply: oneshot::Sender<RedisType>,
}

async fn handle_connection(
    mut stream: TcpStream,
    sender: &Sender<RedisMessage>,
) -> Result<(), RedisError> {
    let mut buffer = BytesMut::with_capacity(1024);
    loop {
        let read_length = stream
            .read_buf(&mut buffer)
            .await
            .map_err(|io_error| RedisError::IoError(io_error))?;
        if read_length == 0 {
            println!("Client closed connection");
            break;
        }
        let result = parse_resp(&mut buffer).map_err(|err| RedisError::ParseError(err))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        let message = RedisMessage {
            message: result,
            reply: reply_tx,
        };
        sender
            .send(message)
            .await
            .map_err(|_| RedisError::TokioError)?;

        let response = reply_rx.await.map_err(|_| RedisError::TokioError)?;

        let res = response.to_bytes();
        stream
            .write_all(&res)
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
    let (tx, mut rx) = mpsc::channel::<RedisMessage>(128); // create channel for communication between tasks

    // setting up the central data store (ARC at the moment / automated referece counting)

    let _ = tokio::spawn(async move {
        // Start receiving messages
        let mut store = Store::new();

        while let Some(cmd) = rx.recv().await {
            println!("Received command: {:?}", cmd);
            let response = handle_command(cmd.message, &mut store).await.unwrap();

            let _ = cmd.reply.send(response);
        }
    });

    println!("Listening on {} - awaiting connections", redis_address);
    loop {
        let (stream, _addr) = tcp_listener.accept().await?;
        println!("Accepted connection from client");

        let sender = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, &sender).await {
                // Handle errors here

                match e {
                    RedisError::ParseError(resp_parse_error) => match resp_parse_error {
                        RespParseError::InvalidFormat => {
                            eprintln!("Invalid format")
                        }
                    },
                    RedisError::CommandError(command_error) => match command_error {
                        CommandError::InvalidInput(message) => {
                            eprintln!("Command Error - Invalid input: {}", message)
                        }
                        CommandError::UnknownCommand(message) => {
                            eprintln!("Command Error - Unknown command: {}", message)
                        }
                        CommandError::StoreError(store_error) => match store_error {
                            store::StoreError::KeyNotFound => {
                                eprintln!("Store Error - Key not found")
                            }
                            store::StoreError::KeyExpired => {
                                eprintln!("Store Error - Key expired")
                            }
                            store::StoreError::TimeError => {
                                eprintln!("Store Error - Time conversion error")
                            }
                        },
                    },
                    RedisError::IoError(error) => {
                        eprintln!("IO error: {:?}", error)
                    }
                    RedisError::TokioError => {
                        eprintln!("Unknown async error")
                    }
                }
            }
        });
    }
}
