#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

fn handle_stream(mut stream: &TcpStream) -> Result<(), std::io::Error> {
    //let mut string_buffer = String::new();
    // stream.read_to_string(&mut string_buffer)?;
    // println!("Received data: {}", string_buffer);

    loop {
        stream.write_all(b"+PONG\r\n")?;
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Bind the TCP listener to the specified address and port
    let addr = std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    let tcp_binding = TcpListener::bind(&addr);

    if let Ok(listener) = tcp_binding {
        for stream in listener.incoming() {
            match stream {
                Ok(incoming_stream) => {
                    println!("accepted new connection on {}", addr);
                    let _ = handle_stream(&incoming_stream);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    } else {
        panic!("Failed to bind to port");
    }
}
