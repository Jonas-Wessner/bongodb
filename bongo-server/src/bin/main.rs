mod webserver;

use webserver::{Webserver, BongoRequestParser, Request};

#[tokio::main]
async fn main() {
    Webserver::new(
        "localhost:8080",
        BongoRequestParser::new(1024),
        |request: Request| {
            println!("request: '{}'", request.sql);
            return request.sql;
        },
    ).start().await.unwrap();
}