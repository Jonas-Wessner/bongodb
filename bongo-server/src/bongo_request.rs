use serde::{Deserialize, Serialize};
use serde_json::{Result};

use crate::webserver::RequestParser;

///
/// A Request from a bongo client to a `BongoServer`
///
#[derive(Serialize, Deserialize)]
pub struct BongoRequest {
    pub sql: String,
}

///
/// A Parser that can parse requests of type `BongoRequest`.
///
pub struct BongoRequestParser {}

impl BongoRequestParser {
    ///
    /// Creates a new instance of `BongoRequestParser`
    ///
    pub fn new() -> BongoRequestParser {
        Self {}
    }
}

impl RequestParser<BongoRequest> for BongoRequestParser {
    ///
    /// TODO: update documentation
    ///
    ///
    fn parse(&self, bytes: &[u8]) -> Option<BongoRequest> {
        // unwrapping is safe assuming all bytes are ASCII values

        let result: Result<BongoRequest> = serde_json::from_str(&String::from_utf8(bytes.to_vec()).unwrap());
        return if result.is_ok() {
            Some(result.unwrap())
        } else {
            None
        };
    }
}