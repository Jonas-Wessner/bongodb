use duplicate::duplicate;

use std::convert::TryFrom;
use sqlparser::ast::{ColumnDef as SqlParserColDef, DataType};

use crate::serialize::Serialize;
use sqlparser::parser::ParserError;

#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoError {
    SqlSyntaxError(String),
    UnsupportedFeatureError(String),
    InternalError(String),
}

impl From<ParserError> for BongoError {
    // Assuming the parser library is correct, all resulting error must be syntax errors
    fn from(err: ParserError) -> Self {
        match err {
            ParserError::TokenizerError(msg) |
            ParserError::ParserError(msg) => { BongoError::SqlSyntaxError(msg) }
        }
    }
}

///
/// `BongoLiteral` represents all literals supported by BongoDB.
///
/// Each variant contains data that represents an instance of this datatype in Rust.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoLiteral {
    Int(i64),
    Bool(bool),
    Varchar(String, usize),
    Null,
}

impl Serialize for BongoLiteral {
    fn serialize(&self) -> String {
        return match self {
            BongoLiteral::Int(val) => { val.to_string() }
            BongoLiteral::Bool(val) => { val.to_string() }
            BongoLiteral::Varchar(val, _size) => { format!(r#""{}""#, val) }
            _ => { "NULL".to_string() }
        };
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoDataType {
    Int,
    Bool,
    Varchar(usize),
}

impl TryFrom<&DataType> for BongoDataType {
    type Error = BongoError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        return match value {
            DataType::Varchar(opt_size) => {
                return match opt_size {
                    None => { Err(BongoError::UnsupportedFeatureError(String::from("VARCHARs must have a size in BongoDB."))) }
                    Some(size) => { Ok(BongoDataType::Varchar(*size as usize)) }
                };
            }
            DataType::TinyInt(_) |
            DataType::SmallInt(_) |
            DataType::Int(_) => { Ok(BongoDataType::Int) }
            DataType::Boolean => { Ok(BongoDataType::Bool) }
            _ => {
                Err(BongoError::UnsupportedFeatureError(String::from("BongoDB only supports the datatypes INT, VARCHAR(n) and BOOLEAN.")))
            }
        };
    }
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct ColumnDef {
    pub(crate) name: String,
    pub(crate) data_type: BongoDataType,
}

impl TryFrom<&SqlParserColDef> for ColumnDef {
    type Error = BongoError;

    fn try_from(value: &SqlParserColDef) -> Result<Self, Self::Error> {
        Ok(
            ColumnDef {
                name: String::from(&value.name.value),
                data_type: BongoDataType::try_from(&value.data_type)?,
            })
    }
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