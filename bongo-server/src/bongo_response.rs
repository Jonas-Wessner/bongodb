use crate::types::{Row, BongoError};
use crate::serialize::Serialize;

///
/// `BongoResponse` represents the result of the execution of a `BongoRequest`.
///
pub enum BongoResponse {
    ///
    /// `Success` represents the successful execution of a `BongoRequest`.
    /// It contains an `Optional` containing the result of the execution.
    /// Only select statements return a result which is a `Vec<Row>`, all other statements
    /// return the `None` variant.
    ///
    /// By this declaration all `Row`s in the vector could have different sizes and data types,
    /// however this makes no sense in the context of BongoDB. Therefore the implementation must
    /// guarantee that every Row has the same size and data types so that this behaviour can be
    /// assumed by the consumer of the `BongoResponse::Success` type.
    ///
    /// Note that there is a semantic difference between `BongoResponse::Success` containing
    /// `Some` of an empty `Vec` and containing `None`. `None` means that there was no result,
    /// an empty `Vec` means that there was a result containing zero rows.
    ///
    Success(Option<Vec<Row>>),
    ///
    /// `Error` represents that the execution of a `BongoRequest` was not successful.
    /// It contains the error message as a `String`.
    ///
    Error(BongoError),
}

///
/// Implementation of `Serialize` for `BongoResponse`
///
/// Responses are serialized to the following format:
///
/// `{ "successful": <0_or_1>, "error": "<error_message>", "data": <array_of_rows_or_null> }`
///
/// <array_of_rows_or_null> in the non-null case has the structure of a json array of rows
/// where each row is a json array of attributes:
///
/// [ [ 1, "Marc", true ], [ 2, "Garry", false ], [ 3, "Peter", true ] <...> ]
///
impl Serialize for BongoResponse {
    fn serialize(&self) -> String {
        match self {
            BongoResponse::Success(result_optional) => {
                let serialized_data;

                match result_optional {
                    None => {
                        serialized_data = String::from("none");
                    }
                    Some(data) => {
                        serialized_data = data.serialize();
                    }
                }

                Self::assemble_serialized_response(
                    true,
                    "",
                    &serialized_data,
                )
            }
            BongoResponse::Error(err) => {
                return Self::assemble_serialized_response(
                    false,
                    &format!("{:?}", err),
                    "");
            }
        }
    }
}

impl BongoResponse {
    fn assemble_serialized_response(is_successful: bool, error_message: &str, serialized_data: &str) -> String {
        format!(r#"{{ "successful": {}, "error": "{}", "data": {} }}"#,
                is_successful as i32, // convert to 0, 1, to get the desired string representation
                error_message,
                serialized_data)
    }
}
