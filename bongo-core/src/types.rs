use std::cmp::Ordering;
use std::convert::TryFrom;

use object::read::ReadRef;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef as SqlParserColDef, DataType};
use sqlparser::parser::ParserError;

use crate::bytes_on_disc::{AsDiscBytes, FromDiscBytes};
use crate::conversions::TryConvertAllExt;
use crate::types::BongoError::InternalError;

///
/// Implementers of this trait allow to extract column names from themselves.
///
pub trait GetColNamesExt {
    ///
    /// Returns the names of all contained columns.
    ///
    fn get_col_names(&self) -> Vec<String>;
}

pub trait GetDTypesExt<'a> {
    fn get_d_types(&'a self) -> Vec<&'a BongoDataType>;
}

///
/// `BongoError` is the Error class used by the `BongoDB` server.
///
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum BongoError {
    SqlSyntaxError(String),
    SqlRuntimeError(String),
    EmptySqlStatementError,
    UnsupportedFeatureError(String),
    // an error that represents a bug in BongoDB
    InternalError(String),
    // an error related to the webserver of BongoDB
    WebServerError(String),
    // Database directory does not exist or is not a directory. The contained string is the location
    // where was searched
    DatabaseNotFoundError(String),
    ReadFileError(String),
    WriteFileError(String),
    // Error when serialising the messages passed between BongoServer and client
    DeserializerError,
    InvalidArgumentError(String),
}

///
/// Converts a `ParserError` of the used `sqlparser`-library into a `BongoErr`
///
impl From<ParserError> for BongoError {
    // Assuming the parser library is correct, all resulting error must be syntax errors
    fn from(err: ParserError) -> Self {
        match err {
            ParserError::TokenizerError(msg) | ParserError::ParserError(msg) => {
                BongoError::SqlSyntaxError(msg)
            }
        }
    }
}

///
/// `BongoLiteral` represents all literals supported by BongoDB.
///
/// Each variant contains data that represents an instance of this datatype in Rust.
///
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Hash, Clone)]
pub enum BongoLiteral {
    Int(i64),
    Bool(bool),
    // Varchar can only fit in the BongoDataType::Varchar(size) if String::len() <= size
    Varchar(String),
    Null,
}

impl BongoLiteral {
    ///
    /// Converts a `BongoLiteral` to a boolean value if possible.
    /// This is useful when recursively evaluating an `Expr`.
    ///
    pub fn as_bool(&self) -> Result<bool, BongoError> {
        if let BongoLiteral::Bool(val) = self {
            return Ok(*val);
        }

        Err(BongoError::SqlRuntimeError(format!(
            "Cannot convert '{:?}' to boolean value.",
            self
        )))
    }
}

impl PartialOrd for BongoLiteral {
    ///
    /// Types of BongoLiteral can only be compared, if they come from the same variant
    ///
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            BongoLiteral::Int(l) => {
                if let BongoLiteral::Int(r) = other {
                    return l.partial_cmp(r);
                }
            }
            BongoLiteral::Bool(l) => {
                if let BongoLiteral::Bool(r) = other {
                    return l.partial_cmp(r);
                }
            }
            BongoLiteral::Varchar(l) => {
                if let BongoLiteral::Varchar(r) = other {
                    return l.partial_cmp(r);
                }
            }
            BongoLiteral::Null => {
                if let BongoLiteral::Null = other {
                    return Some(Ordering::Equal);
                }
                return None;
            }
        }
        return None;
    }
}

impl<D: AsRef<BongoDataType>> AsDiscBytes<&D> for BongoLiteral {
    fn as_disc_bytes(&self, def: &D) -> Result<Vec<u8>, BongoError> {
        if !def.as_ref().can_store(self) {
            return Err(BongoError::InternalError(
                "Datatype mismatch on conversion to bytes.".to_string(),
            ));
        }

        match self {
            BongoLiteral::Int(val) => {
                let mut bytes = vec![true as u8];
                bytes.append(&mut val.to_be_bytes().to_vec());

                Ok(bytes)
            }
            BongoLiteral::Bool(val) => Ok(vec![true as u8, *val as u8]),
            BongoLiteral::Null => {
                let size = def.as_ref().disc_size();

                let mut bytes = Vec::with_capacity(size);
                bytes.push(false as u8); // indicator for NULL value
                unsafe {
                    bytes.set_len(size);
                } // safe because size is pre-allocated

                Ok(bytes)
            }
            BongoLiteral::Varchar(val) => {
                let disc_size = def.as_ref().disc_size();

                let mut bytes = Vec::with_capacity(disc_size);
                bytes.push(true as u8);
                bytes.append(&mut val.as_bytes().to_vec());
                bytes.push(0xFFu8);

                unsafe {
                    bytes.set_len(disc_size);
                } // safe because size is pre-allocated

                Ok(bytes)
            }
        }
    }
}

impl<D: AsRef<BongoDataType>> FromDiscBytes<D> for BongoLiteral {
    fn from_disc_bytes(bytes: &[u8], def: D) -> Result<Self, BongoError> {
        if bytes.len() != def.as_ref().disc_size() {
            return Err(BongoError::InternalError(
                "Cannot read literal from bytes due to wrong size of byte array.".to_string(),
            ));
        }

        if bytes[0] == 0 {
            return Ok(BongoLiteral::Null);
        }

        let payload = &bytes[1..];

        return match def.as_ref() {
            BongoDataType::Int => {
                // safe because we know the size of the slice because of checks before
                Ok(BongoLiteral::Int(i64::from_be_bytes(
                    payload.try_into().unwrap(),
                )))
            }
            BongoDataType::Bool => Ok(BongoLiteral::Bool(payload[0] != 0)), // convert to bool
            BongoDataType::Varchar(_) => match payload.iter().position(|b| b == &0xFFu8) {
                None => Err(BongoError::InternalError(
                    "Cannot read literal from bytes due to corrupted format.".to_string(),
                )),
                Some(delim) => {
                    match String::from_utf8(payload.to_vec().into_iter().take(delim).collect()) {
                        Ok(content) => Ok(BongoLiteral::Varchar(content)),
                        Err(_) => Err(BongoError::InternalError(
                            "Cannot read literal from bytes due to corrupted format.".to_string(),
                        )),
                    }
                }
            },
        };
    }
}

impl TryFrom<BongoLiteral> for i64 {
    type Error = BongoError;
    fn try_from(literal: BongoLiteral) -> Result<Self, Self::Error> {
        if let BongoLiteral::Int(v) = literal {
            Ok(v)
        } else {
            Err(BongoError::InternalError(
                "Could not convert BongoLiteral to i64".to_string(),
            ))
        }
    }
}

impl TryFrom<BongoLiteral> for String {
    type Error = BongoError;
    fn try_from(literal: BongoLiteral) -> Result<Self, Self::Error> {
        if let BongoLiteral::Varchar(v) = literal {
            Ok(v)
        } else {
            Err(BongoError::InternalError(
                "Could not convert BongoLiteral to String".to_string(),
            ))
        }
    }
}

impl TryFrom<BongoLiteral> for bool {
    type Error = BongoError;
    fn try_from(literal: BongoLiteral) -> Result<Self, Self::Error> {
        if let BongoLiteral::Bool(v) = literal {
            Ok(v)
        } else {
            Err(BongoError::InternalError(
                "Could not convert BongoLiteral to bool".to_string(),
            ))
        }
    }
}

impl TryFrom<BongoLiteral> for Option<i64> {
    type Error = BongoError;
    fn try_from(literal: BongoLiteral) -> Result<Self, Self::Error> {
        match literal {
            BongoLiteral::Int(v) => Ok(Some(v)),
            BongoLiteral::Null => Ok(None),
            _ => Err(BongoError::InternalError(
                "Could not convert BongoLiteral to Option<i64>".to_string(),
            )),
        }
    }
}

impl TryFrom<BongoLiteral> for Option<String> {
    type Error = BongoError;
    fn try_from(literal: BongoLiteral) -> Result<Self, Self::Error> {
        match literal {
            BongoLiteral::Varchar(v) => Ok(Some(v)),
            BongoLiteral::Null => Ok(None),
            _ => Err(BongoError::InternalError(
                "Could not convert BongoLiteral to Option<String>".to_string(),
            )),
        }
    }
}

impl TryFrom<BongoLiteral> for Option<bool> {
    type Error = BongoError;
    fn try_from(literal: BongoLiteral) -> Result<Self, Self::Error> {
        match literal {
            BongoLiteral::Bool(v) => Ok(Some(v)),
            BongoLiteral::Null => Ok(None),
            _ => Err(BongoError::InternalError(
                "Could not convert BongoLiteral to Option<bool>".to_string(),
            )),
        }
    }
}

///
/// `BongoDataType` represents all data types supported by BongoDB.
///
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum BongoDataType {
    Int,
    Bool,
    // VARCHARs are required to have a fixed size
    Varchar(usize),
}

impl AsRef<BongoDataType> for BongoDataType {
    fn as_ref(&self) -> &Self {
        return &self;
    }
}

impl BongoDataType {
    pub fn can_store(&self, lit: &BongoLiteral) -> bool {
        return match lit {
            BongoLiteral::Int(_) => {
                matches!(self, BongoDataType::Int)
            }
            BongoLiteral::Bool(_) => {
                matches!(self, BongoDataType::Bool)
            }
            BongoLiteral::Varchar(s) => match self {
                BongoDataType::Varchar(cap) => &s.len() <= cap,
                _ => false,
            },
            BongoLiteral::Null => true,
        };
    }

    pub fn disc_size(&self) -> usize {
        // items are saved with one extra byte. The first byte is the information whether it is a null value
        match self {
            BongoDataType::Int => 8 + 1,
            BongoDataType::Bool => 1 + 1,
            BongoDataType::Varchar(size) => size + 1 + 1, // one 0xFF at the end as terminator
        }
    }
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
                    None => Err(BongoError::UnsupportedFeatureError(String::from(
                        "VARCHARs must have a size in BongoDB.",
                    ))),
                    Some(size) => Ok(BongoDataType::Varchar(*size as usize)),
                };
            }
            DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) => {
                Ok(BongoDataType::Int)
            }
            DataType::Boolean => Ok(BongoDataType::Bool),
            _ => Err(BongoError::UnsupportedFeatureError(String::from(
                "BongoDB only supports the datatypes INT, VARCHAR(n) and BOOLEAN.",
            ))),
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: BongoDataType,
}

impl<T: AsRef<[ColumnDef]>> GetColNamesExt for T {
    fn get_col_names(&self) -> Vec<String> {
        self.as_ref()
            .iter()
            .map(|col_def| -> String { col_def.name.clone() })
            .collect()
    }
}

impl<'a, T: AsRef<[ColumnDef]>> GetDTypesExt<'a> for T {
    fn get_d_types(&'a self) -> Vec<&'a BongoDataType> {
        self.as_ref()
            .iter()
            .map(|col_def| -> &BongoDataType { &col_def.data_type })
            .collect()
    }
}

impl AsRef<BongoDataType> for ColumnDef {
    ///
    /// Allows that a `ColumnDef` can also be passed as a `BongoDataType`. The `name` field is then simple ignored.
    ///
    fn as_ref(&self) -> &BongoDataType {
        &self.data_type
    }
}

///
/// Tries to convert an `&SqlParserColDef` of the used `sqlparser`-library into an object of the custom
/// `ColumnDef` type paying attention to what features are supported by BongoDB.
///
impl TryFrom<&SqlParserColDef> for ColumnDef {
    type Error = BongoError;

    fn try_from(value: &SqlParserColDef) -> Result<Self, Self::Error> {
        Ok(ColumnDef {
            name: String::from(&value.name.value),
            data_type: BongoDataType::try_from(&value.data_type)?,
        })
    }
}

///
/// `Row` is a type definition that is used in two scenarios:
/// 1. Inside the `BongoResult::Ok` variant representing the result of an SQL SELECT statement.
/// 2. Inside the `Statement::Insert` variant representing a row to be inserted into a table.
///
pub type Row = Vec<BongoLiteral>;

impl<D: AsRef<BongoDataType>> AsDiscBytes<&[D]> for Row {
    fn as_disc_bytes(&self, def: &[D]) -> Result<Vec<u8>, BongoError> {
        if self.len() != def.len() {
            return Err(InternalError("Row to big for definition and therefore cannot be converted to byte representation.".to_string()));
        }

        Ok(self
            .iter()
            .enumerate()
            .map(|(i, lit)| lit.as_disc_bytes(&def[i]))
            .collect::<Vec<Result<Vec<u8>, BongoError>>>()
            .try_convert_all(|item| item)?
            .concat())
    }
}

impl<D: AsRef<BongoDataType>> FromDiscBytes<&[D]> for Row {
    fn from_disc_bytes(bytes: &[u8], def: &[D]) -> Result<Self, BongoError> {
        let mut offset = 0;
        def.iter()
            .map(
                |d_type| match bytes.read_bytes(&mut offset, d_type.as_ref().disc_size() as u64) {
                    Ok(bytes) => BongoLiteral::from_disc_bytes(&bytes, &d_type),
                    Err(_) => Err(BongoError::InternalError(
                        "Reading row from bytes not successful.".to_string(),
                    )),
                },
            )
            .collect::<Vec<Result<BongoLiteral, BongoError>>>()
            .try_convert_all(|x| x) // bubble up error
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_on_disc::{AsDiscBytes, FromDiscBytes};
    use crate::types::{BongoDataType, BongoLiteral};

    #[test]
    fn datatype_can_store() {
        assert!(BongoDataType::Int.can_store(&BongoLiteral::Int(5)));
        assert!(BongoDataType::Int.can_store(&BongoLiteral::Null));
        assert!(!BongoDataType::Int.can_store(&BongoLiteral::Bool(true)));
        assert!(!BongoDataType::Int.can_store(&BongoLiteral::Varchar("Test".to_string())));

        assert!(BongoDataType::Bool.can_store(&BongoLiteral::Bool(true)));
        assert!(BongoDataType::Bool.can_store(&BongoLiteral::Null));
        assert!(!BongoDataType::Bool.can_store(&BongoLiteral::Int(5)));
        assert!(!BongoDataType::Bool.can_store(&BongoLiteral::Varchar("Test".to_string())));

        assert!(BongoDataType::Varchar(5).can_store(&BongoLiteral::Varchar("Test".to_string())));
        assert!(BongoDataType::Varchar(5).can_store(&BongoLiteral::Null));
        assert!(!BongoDataType::Varchar(5).can_store(&BongoLiteral::Int(5)));
        assert!(!BongoDataType::Varchar(5).can_store(&BongoLiteral::Bool(true)));
        assert!(!BongoDataType::Varchar(5)
            .can_store(&BongoLiteral::Varchar("More than size 5".to_string())));
    }

    #[test]
    fn bongo_lit_as_and_from_disc_bytes_null() {
        // note: the content of null values is ignored except the first byte
        assert_eq!(
            BongoLiteral::Null
                .as_disc_bytes(&BongoDataType::Int)
                .unwrap()[0],
            0
        );
        assert_eq!(
            BongoLiteral::Null
                .as_disc_bytes(&BongoDataType::Int)
                .unwrap()
                .len(),
            9
        );

        assert_eq!(
            BongoLiteral::Null
                .as_disc_bytes(&BongoDataType::Bool)
                .unwrap()[0],
            0
        );
        assert_eq!(
            BongoLiteral::Null
                .as_disc_bytes(&BongoDataType::Bool)
                .unwrap()
                .len(),
            2
        );

        assert_eq!(
            BongoLiteral::Null
                .as_disc_bytes(&BongoDataType::Varchar(10))
                .unwrap()[0],
            0
        );
        assert_eq!(
            BongoLiteral::Null
                .as_disc_bytes(&BongoDataType::Varchar(10))
                .unwrap()
                .len(),
            12
        );

        let original = BongoLiteral::Null;
        let bytes = original.as_disc_bytes(&BongoDataType::Varchar(32)).unwrap();
        let literal = BongoLiteral::from_disc_bytes(&bytes, &BongoDataType::Varchar(32)).unwrap();
        assert_eq!(original, literal);
    }

    #[test]
    fn bongo_lit_as_and_from_disc_bytes_int() {
        assert_eq!(
            BongoLiteral::Int(42)
                .as_disc_bytes(&BongoDataType::Int)
                .unwrap()[0],
            1
        );
        assert_eq!(
            BongoLiteral::Int(42)
                .as_disc_bytes(&BongoDataType::Int)
                .unwrap()
                .len(),
            9
        );
        assert!(BongoLiteral::Int(42)
            .as_disc_bytes(&BongoDataType::Bool)
            .is_err());
        assert!(BongoLiteral::Int(42)
            .as_disc_bytes(&BongoDataType::Varchar(10))
            .is_err());

        let original = BongoLiteral::Int(42);
        let bytes = original.as_disc_bytes(&BongoDataType::Int).unwrap();
        let literal = BongoLiteral::from_disc_bytes(&bytes, &BongoDataType::Int).unwrap();

        assert_eq!(original, literal)
    }

    #[test]
    fn bongo_lit_as_and_from_disc_bytes_bool() {
        assert_eq!(
            BongoLiteral::Bool(true)
                .as_disc_bytes(&BongoDataType::Bool)
                .unwrap()[0],
            1
        );
        assert_eq!(
            BongoLiteral::Bool(true)
                .as_disc_bytes(&BongoDataType::Bool)
                .unwrap()
                .len(),
            2
        );
        assert!(BongoLiteral::Bool(true)
            .as_disc_bytes(&BongoDataType::Int)
            .is_err());
        assert!(BongoLiteral::Bool(true)
            .as_disc_bytes(&BongoDataType::Varchar(10))
            .is_err());

        let original = BongoLiteral::Bool(false);
        let bytes = original.as_disc_bytes(&BongoDataType::Bool).unwrap();
        let literal = BongoLiteral::from_disc_bytes(&bytes, &BongoDataType::Bool).unwrap();

        assert_eq!(original, literal)
    }

    #[test]
    fn bongo_lit_as_and_from_disc_bytes_varchar() {
        let m = "rust".to_string();
        let size = m.len();
        assert_eq!(
            BongoLiteral::Varchar(m.clone())
                .as_disc_bytes(&BongoDataType::Varchar(size))
                .unwrap()[0],
            1
        );
        assert_eq!(
            BongoLiteral::Varchar(m.clone())
                .as_disc_bytes(&BongoDataType::Varchar(size))
                .unwrap()
                .len(),
            size + 2
        );
        assert_eq!(
            BongoLiteral::Varchar(m.clone())
                .as_disc_bytes(&BongoDataType::Varchar(200))
                .unwrap()
                .len(),
            200 + 2
        );
        assert!(BongoLiteral::Varchar(m.clone())
            .as_disc_bytes(&BongoDataType::Varchar(2))
            .is_err());
        assert!(BongoLiteral::Varchar(m.clone())
            .as_disc_bytes(&BongoDataType::Bool)
            .is_err());
        assert!(BongoLiteral::Varchar(m.clone())
            .as_disc_bytes(&BongoDataType::Int)
            .is_err());

        // varchar with exact size
        let original = BongoLiteral::Varchar(m.clone());
        let bytes = original
            .as_disc_bytes(&BongoDataType::Varchar(size))
            .unwrap();
        let literal = BongoLiteral::from_disc_bytes(&bytes, &BongoDataType::Varchar(size)).unwrap();
        assert_eq!(original, literal);

        // varchar in bigger container
        let container_size = size + 10;
        let original = BongoLiteral::Varchar(m.clone());
        let bytes = original
            .as_disc_bytes(&BongoDataType::Varchar(container_size))
            .unwrap();
        let literal =
            BongoLiteral::from_disc_bytes(&bytes, &BongoDataType::Varchar(container_size)).unwrap();
        assert_eq!(original, literal);
    }
}
