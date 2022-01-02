use duplicate::duplicate;

use crate::serialize::Serialize;

///
/// `BongoDataType` represents all data types of BongoDB.
///
/// Each variant contains data that represents an instance of this datatype in Rust.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoLiteral {
    Int(i64),
    Bool(bool),
    Varchar(String, usize),
    Null
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoDataType {
    Int,
    Bool,
    Varchar(usize),
}


impl Serialize for BongoLiteral {
    fn serialize(&self) -> String {
        return match self {
            BongoLiteral::Int(val) => { val.to_string() }
            BongoLiteral::Bool(val) => { val.to_string() }
            BongoLiteral::Varchar(val, _size) => { format!(r#""{}""#, val) }
            _ => {"NULL".to_string()}
        };
    }
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) data_type: BongoDataType,
}


///
/// `Row` represents one row that is returned in a `BongoResponse::Success` variant.
///
pub(crate) type Row = Vec<BongoLiteral>;


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