#![allow(unused_imports)]
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Bind the TCP listener to the specified address and port
    let addr = std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    let tcp_binding = TcpListener::bind(&addr);

    if let Ok(listener) = tcp_binding {
        for stream in listener.incoming() {
            match stream {
                Ok(_stream) => {
                    println!("accepted new connection on {}", addr);
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
