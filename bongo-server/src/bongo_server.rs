mod webserver;
mod executor;
mod sql_parser;
mod statements;
mod types;
mod serialize;
mod bongo_request;
mod bongo_response;

use crate::bongo_server::webserver::{Webserver};
use crate::bongo_server::bongo_request::{BongoRequestParser, BongoRequest};
use crate::bongo_server::serialize::{Serialize};
use crate::bongo_server::executor::Executor;

pub struct BongoServer {}

impl BongoServer {
    pub async fn start_new(address: &str) -> String {
        let executor = Executor::new();

        Webserver::new(
            address,
            BongoRequestParser::new(1024),
            move |request: BongoRequest| -> String {
                let serialized_response = executor.execute(&request).serialize();
                println!("request: '{}'", request.sql);
                println!("response: '{}'", serialized_response);
                return serialized_response;
            },
        ).start().await
    }
}
