use duplicate::duplicate;


pub trait Serialize {
    fn serialize(&self) -> String;
}

///
/// `BongoDataType` represents all data types of BongoDB.
///
/// Each variant contains data that represents an instance of this datatype in Rust.
///
pub enum BongoDataType {
    Int(i64),
    Bool(bool),
    Varchar(String, usize),
}

impl Serialize for BongoDataType {
    fn serialize(&self) -> String {
        return match self {
            BongoDataType::Int(val) => { val.to_string() }
            BongoDataType::Bool(val) => { val.to_string() }
            BongoDataType::Varchar(val, _size) => { format!(r#""{}""#, val) }
        };
    }
}

///
/// `Row` represents one row that is returned in a `BongoResponse::Success` variant.
///
type Row = Vec<BongoDataType>;


///
/// Implementation of Serialize for `Row` and `Vec<Row>` using duplicate macro because both
/// serialize to a json array and therefore share the same code.
#[duplicate(
data_type; [ Row ]; [ Vec < Row > ];)]
impl Serialize for data_type {
    fn serialize(&self) -> String {
        std::iter::once(String::from("[ ")).chain(
            self.into_iter()
                .map(|d_type| { d_type.serialize() })
                .intersperse_with(|| { String::from(", ") })
        )
            .chain(std::iter::once(String::from(" ]")))
            .collect::<String>()
    }
}

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
    Error(String),
}


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
            BongoResponse::Error(message) => {
                return Self::assemble_serialized_response(
                    false,
                    message,
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
