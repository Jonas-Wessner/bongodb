#![feature(iter_intersperse)]

mod sql_parser;
mod statement;
mod types;
mod serialize;
mod bongo_request;
mod bongo_response;

mod webserver;
mod executor;


use crate::webserver::{Webserver};
use crate::bongo_request::{BongoRequestParser, BongoRequest};
use crate::serialize::{Serialize};
use crate::executor::Executor;

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

