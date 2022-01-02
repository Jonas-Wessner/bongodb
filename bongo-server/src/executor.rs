use crate::bongo_response::{BongoResponse};
use crate::bongo_request::BongoRequest;
use crate::sql_parser::SqlParser;
use crate::types::BongoDataType;

pub struct Executor {}


impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&self, request: &BongoRequest) -> BongoResponse {
        return match SqlParser::parse(&request.sql) {
            Ok(statement) => {
                println!("sql has been parsed with the following resulting statement: {:?}", statement);

                //  return an example BongoResponse
                BongoResponse::Success(Some(vec![
                    vec![
                        BongoDataType::Int(1),
                        BongoDataType::Varchar(String::from("Marc"), "Marc".len()),
                        BongoDataType::Bool(true)
                    ],
                    vec![
                        BongoDataType::Int(2),
                        BongoDataType::Varchar(String::from("Garry"), "Garry".len()),
                        BongoDataType::Bool(false)
                    ]
                ]))
            }
            Err(m) => {
                let message = format!("error parsing request with message {}", m);
                println!("{}", message);
                BongoResponse::Error(message)
            }
        }
    }
}