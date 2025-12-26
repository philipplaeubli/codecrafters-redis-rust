#![allow(unused_imports)]
use std::{
    borrow::Cow,
    ffi::IntoStringError,
    io::{BufRead, BufReader, Error, Read, Write},
    thread,
};

use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::parser::{RedisType, RespParseError, parse_resp};
mod parser;

fn handle_input(input: RedisType) -> Result<String, RespParseError> {
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
                _ => Err(RespParseError::InvalidFormat),
            }
        }
        _ => Err(RespParseError::InvalidFormat),
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), RespParseError> {
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
        let response = handle_input(result)?;

        stream.write_all(response.as_bytes()).await?
    }
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let redis_address =
        std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    let tcp_listener = TcpListener::bind(&redis_address).await?;

    println!("Listening on {} - awaiting connections", redis_address);
    loop {
        let (stream, _addr) = tcp_listener.accept().await?;
        println!("Accepted connection from client");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream).await {
                eprintln!("Error during connection handling: {:?}", e);
            }
        });
    }
}
