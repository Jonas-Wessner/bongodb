use duplicate::duplicate;

use crate::bongo_server::serialize::Serialize;

///
/// `BongoDataType` represents all data types of BongoDB.
///
/// Each variant contains data that represents an instance of this datatype in Rust.
///
#[derive(Debug)]
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


#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: BongoDataType,
}


///
/// `Row` represents one row that is returned in a `BongoResponse::Success` variant.
///
pub(crate) type Row = Vec<BongoDataType>;


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