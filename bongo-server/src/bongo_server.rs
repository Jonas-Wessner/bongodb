mod webserver;
mod executor;
mod bongo_request;
mod bongo_response;

use crate::bongo_server::webserver::{Webserver};
use crate::bongo_server::bongo_request::{BongoRequestParser, BongoRequest};
use crate::bongo_server::executor::Executor;

pub struct BongoServer {}

impl BongoServer {
    pub async fn start_new(address: &str) -> String {
        let executor = Executor::new();

        Webserver::new(
            address,
            BongoRequestParser::new(1024),
            |request: BongoRequest| {
                println!("request: '{}'", request.sql);

                return executor.execute().serialize();
            },
        ).start().await
    }
}
