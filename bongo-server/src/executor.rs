use bongo_core::bongo_request::BongoRequest;
use bongo_core::bongo_response::BongoResponse;
use bongo_core::types::BongoLiteral;
use crate::sql_parser::parser::SqlParser;

///
/// An `Executor` can execute a `BongoRequest`
///
pub struct Executor {}


impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    ///
    /// Executes a `BongoRequest` by first parsing it and then executing its contents.
    /// returns a `BongoResponse` representing the result of execution.
    ///
    pub fn execute(&self, request: &BongoRequest) -> BongoResponse {
        return match SqlParser::parse(&request.sql) {
            Ok(statement) => {
                println!("sql has been parsed with the following resulting statement:\n{:?}", statement);

                // TODO: implement execution of statement and return BongoResponse

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
                let message = format!("Error parsing request: {:?}", err);
                println!("{}", message);
                BongoResponse::Error(err)
            }
        }
    }
}