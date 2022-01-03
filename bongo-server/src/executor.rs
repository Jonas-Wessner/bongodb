use crate::bongo_response::{BongoResponse};
use crate::bongo_request::BongoRequest;
use crate::sql_parser::parser::SqlParser;
use crate::types::BongoLiteral;

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
                        BongoLiteral::Int(1),
                        BongoLiteral::Varchar(String::from("Marc"), "Marc".len()),
                        BongoLiteral::Bool(true)
                    ],
                    vec![
                        BongoLiteral::Int(2),
                        BongoLiteral::Varchar(String::from("Garry"), "Garry".len()),
                        BongoLiteral::Bool(false)
                    ]
                ]))
            }
            Err(err) => {
                let message = format!("error parsing request: {:?}", err);
                println!("{}", message);
                BongoResponse::Error(message)
            }
        }
    }
}