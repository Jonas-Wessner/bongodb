use duplicate::duplicate;

use std::convert::TryFrom;
use sqlparser::ast::{ColumnDef as SqlParserColDef, DataType};

use crate::serialize::Serialize;
use sqlparser::parser::ParserError;

///
/// `BongoError` is the Error class used by the `BongoDB` server.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoError {
    SqlSyntaxError(String),
    EmptySqlStatementError,
    UnsupportedFeatureError(String),
    // an error that represents a bug in BongoDB
    InternalError(String),
    // an error related to the webserver of BongoDB
    WebServerError(String),
}

///
/// Converts a `ParserError` of the used `sqlparser`-library into a `BongoErr`
///
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

///
/// Defines how a BongoLiteral is serialized in the web communication between `BongoServer` an a client.
///
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

///
/// `BongoDataType` represents all data types supported by BongoDB.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum BongoDataType {
    Int,
    Bool,
    // VARCHARs are required to have a fixed size
    Varchar(usize),
}

///
/// Tries to convert an `&DataType` of the used `sqlparser`-library into an object of the custom
/// `BongoDataType` type paying attention to what features are supported by BongoDB.
///
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

///
/// `ColumnDef` represents the definition of a column in an SQL CREATE TABLE statement.
///
/// # Examples
///
/// In the statement `CREATE TABLE table_1 (col_1 INT, col_2 BOOLEAN);`
/// two `ColumnDef`s are specified:
/// `BongoColDef { name: "col_1".to_string(), data_type: BongoDataType::Int },`
/// `BongoColDef { name: "col_2".to_string(), data_type: BongoDataType::Bool },`
///
#[derive(Debug)]
#[derive(PartialEq)]
pub struct ColumnDef {
    pub(crate) name: String,
    pub(crate) data_type: BongoDataType,
}

///
/// Tries to convert an `&SqlParserColDef` of the used `sqlparser`-library into an object of the custom
/// `ColumnDef` type paying attention to what features are supported by BongoDB.
///
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
/// `Row` is a type definition that is used in two scenarios:
/// 1. Inside the `BongoResponse::Success` variant representing the result of an SQL SELECT statement.
/// 2. Inside the `Statement::Insert` variant representing a row to be inserted into a table.
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