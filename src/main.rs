#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    thread,
};

use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    let mut buffer = [0u8; 128];
    loop {
        let result = stream.read(&mut buffer).await?;
        if result == 0 {
            // Connection closed by client
            println!("Connection closed by client");
            return Ok(());
        } else if result > 0 {
            // don't care if we got some error, just send PONG
            stream.write_all(b"+PONG\r\n").await?;
            println!("Sent PONG");
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Bind the TCP listener to the specified address and port
    let addr = std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let tcp_binding = TcpListener::bind(&addr);
    let tcp_listener = tcp_binding.await?;
    println!("Listening on {} - awaiting connections", addr);
    loop {
        let (stream, _addr) = tcp_listener.accept().await?;
        println!("Accepted connection from client");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream).await {
                eprintln!("Error handling connection: {e}");
            }
        });
    }
}
