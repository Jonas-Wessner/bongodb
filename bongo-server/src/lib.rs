#![feature(iter_intersperse)]

pub mod util;
pub mod sql_parser;
mod statement;
mod executor;


use bongo_core::bongo_request::{BongoRequest, BongoRequestParser};
use bongo_core::types::BongoError;
use webserver::Webserver;
use bongo_core::serialize::Serialize;
use crate::executor::Executor;

pub struct BongoServer {}

impl BongoServer {
    pub async fn start_new(address: &str) -> BongoError {
        let executor = Executor::new();

        BongoError::WebServerError(
            Webserver::new(
                address,
                BongoRequestParser::new(),
                move |request: BongoRequest| -> String {
                    let serialized_response = executor.execute(&request).serialize();
                    println!("request: '{}'", request.sql);
                    println!("response: '{}'", serialized_response);
                    return serialized_response;
                },
            ).start().await)
    }
}

