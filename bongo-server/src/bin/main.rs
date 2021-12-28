mod webserver;

use webserver::{Webserver, ClientToServerProto, Request};

#[tokio::main]
async fn main() {
    Webserver::new(
        "localhost:8080",
        ClientToServerProto::new(1024),
        |request: Request| {
            println!("request: '{}'", request.sql);
            return request.sql;
        },
    ).start().await.unwrap();
}