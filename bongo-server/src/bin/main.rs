use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, BufReader, AsyncBufReadExt};
use tokio::net::tcp::WriteHalf;

mod webserver;

use webserver::{Webserver, ClientToServerProto, Request};

#[tokio::main]
async fn main() {
    Webserver::new(
        "localhost:8080",
        ClientToServerProto::new(1024),
        |request: Request| {
            println!("Hello World.");
            return String::from("");
        },
    ).start();
}