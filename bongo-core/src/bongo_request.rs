use serde::{Deserialize, Serialize};
use serde_json::Result;
use webserver::RequestParser;

///
/// A Request from a bongo client to a `BongoServer`
///
#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
    fn parse(&self, bytes: &[u8]) -> Option<BongoRequest> {
        // unwrapping is safe assuming all bytes are ASCII values
        let result: Result<BongoRequest> =
            serde_json::from_str(&String::from_utf8(bytes.to_vec()).unwrap());
        return if result.is_ok() {
            Some(result.unwrap())
        } else {
            None
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::bongo_request::{BongoRequest, BongoRequestParser};
    use webserver::RequestParser;

    #[test]
    fn parse_simple_request() {
        let request = r#"{ "sql": "SELECT * FROM table_1;" }"#;

        let result = BongoRequestParser::new().parse(request.as_bytes());

        let expected = Some(BongoRequest {
            sql: "SELECT * FROM table_1;".to_string(),
        });

        assert_eq!(expected, result);
    }

    #[test]
    fn none_on_invalid_request() {
        // NOTE: one '"' to much
        let request = r#"{ "sql": ""SELECT * FROM table_1;" }"#;

        let result = BongoRequestParser::new().parse(request.as_bytes());

        let expected = None;

        assert_eq!(expected, result);
    }

    #[test]
    fn escape_quotes_and_curly_braces() {
        // NOTE: one '"' to much
        let request = r#"{ "sql": "SELECT * FROM \"table_1\";" }"#;

        let result = BongoRequestParser::new().parse(request.as_bytes());

        let expected = Some(BongoRequest {
            sql: "SELECT * FROM \"table_1\";".to_string(),
        });

        assert_eq!(expected, result);
    }
}
