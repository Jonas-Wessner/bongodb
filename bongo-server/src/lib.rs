#![feature(iter_intersperse)]

pub mod util;
pub mod sql_parser;
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
use crate::types::BongoError;

pub struct BongoServer {}

impl BongoServer {
    pub async fn start_new(address: &str) -> BongoError {
        let executor = Executor::new();

        BongoError::WebServerError(
            Webserver::new(
                address,
                BongoRequestParser::new(1024),
                move |request: BongoRequest| -> String {
                    let serialized_response = executor.execute(&request).serialize();
                    println!("request: '{}'", request.sql);
                    println!("response: '{}'", serialized_response);
                    return serialized_response;
                },
            ).start().await)
    }
}

