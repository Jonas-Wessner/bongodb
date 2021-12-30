mod webserver;
mod executor;
mod bongo_request;
mod bongo_response;

use crate::bongo_server::webserver::{Webserver};
use crate::bongo_server::bongo_request::{BongoRequestParser, BongoRequest};
use crate::bongo_server::bongo_response::{Serialize};
use crate::bongo_server::executor::Executor;

pub struct BongoServer {}

// TODO: fix bug: only the first request gets a response on the tcp socket when connecting via telnet

impl BongoServer {
    pub async fn start_new(address: &str) -> String {
        let executor = Executor::new();

        Webserver::new(
            address,
            BongoRequestParser::new(1024),
            move |request: BongoRequest| {
                println!("request: '{}'", request.sql);

                return executor.execute(&request).serialize();
            },
        ).start().await
    }
}
