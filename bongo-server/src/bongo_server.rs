mod webserver;

use crate::bongo_server::webserver::{Webserver, BongoRequestParser, BongoRequest};

pub struct BongoServer {}

impl BongoServer {
    pub async fn start_new(address: &str) -> String {
        Webserver::new(
            address,
            BongoRequestParser::new(1024),
            |request: BongoRequest| {
                println!("request: '{}'", request.sql);
                return request.sql;
            },
        ).start().await
    }
}
