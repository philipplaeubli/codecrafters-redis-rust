use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
    time::timeout,
};

use crate::{
    commands::{CommandResponse, handle_command},
    parser::{RedisType, RespParseError, parse_resp},
    store::Store,
};
mod commands;
mod parser;
mod store;

#[derive(Debug)]
enum RedisError {
    ParseError(RespParseError),
    IoError(io::Error),
    TokioError,
}

#[derive(Debug)]
enum RedisMessage {
    SendMessage {
        message: RedisType,
        reply: oneshot::Sender<CommandResponse>,
    },
    SendTimeout {
        key: Bytes,
        identifier: u64,
    },
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
        let message = RedisMessage::SendMessage {
            message: result,
            reply: reply_tx,
        };
        sender
            .send(message)
            .await
            .map_err(|_| RedisError::TokioError)?;

        let command_response = reply_rx.await.map_err(|_| RedisError::TokioError)?;
        let response = match command_response {
            CommandResponse::Immediate(redis_type) => redis_type,
            CommandResponse::Wait {
                timeout: timeout_sec,
                receiver,
                key,
                client_id,
            } => {
                println!("Received wait command for client: {}", client_id);
                let result = if timeout_sec == 0.0 {
                    // timeout=0 means wait forever
                    println!("Waiting forever for client: {}", client_id);
                    receiver.await.ok()
                } else {
                    println!(
                        "Waiting with timeout {} for client: {}",
                        timeout_sec, client_id
                    );
                    match timeout(Duration::from_secs_f64(timeout_sec), receiver).await {
                        Ok(Ok(value)) => Some(value),
                        Ok(Err(_)) | Err(_) => {
                            // Timeout or channel closed - send cleanup message
                            println!(
                                "Timeout or channel closed, sending cleanup message to client: {}",
                                client_id
                            );
                            let _ = sender
                                .send(RedisMessage::SendTimeout {
                                    key,
                                    identifier: client_id,
                                })
                                .await;
                            None
                        }
                    }
                };

                result.unwrap_or(RedisType::Array(None))
            }
        };

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
            match cmd {
                RedisMessage::SendMessage { message, reply } => {
                    println!("Received command: {:?}", message);
                    let command = handle_command(message, &mut store);
                    match command {
                        Ok(response) => {
                            let _ = reply.send(response);
                        }
                        Err(err) => {
                            let _ = reply.send(CommandResponse::Immediate(RedisType::SimpleError(
                                Bytes::from(format!("ERR {:?}", err)),
                            )));
                        }
                    }
                }
                RedisMessage::SendTimeout { key, identifier } => {
                    println!(
                        "Cleaning up blocked client {} for key {:?}",
                        identifier, key
                    );
                    store.remove_blocked_client(&key, identifier);
                }
            }
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
