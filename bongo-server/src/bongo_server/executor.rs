use crate::bongo_server::bongo_response::{BongoResponse, BongoDataType};
use crate::bongo_server::bongo_request::BongoRequest;

pub struct Executor {}


impl Executor {
    pub fn new() -> Self {
        Self{}
    }

    pub fn execute(&self, request: &BongoRequest) -> BongoResponse {
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
}