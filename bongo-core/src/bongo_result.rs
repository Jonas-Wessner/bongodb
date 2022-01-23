use crate::types::{BongoError, Row};

///
/// `BongoResult` is the result of the execution of a `BongoRequest`
///
/// `Ok` of `Option<Vec<Row>>` represents the successful execution of a `BongoRequest`.
/// It contains an `Optional` containing the result of the execution.
/// Only select statements return a result which is a `Vec<Row>`, all other statements
/// return the `None` variant.
///
/// By this declaration all `Row`s in the vector could have different sizes and data types,
/// however this makes no sense in the context of BongoDB. Therefore the implementation must
/// guarantee that every Row has the same size and data types so that this behaviour can be
/// assumed by the consumer of the `BongoResult::Ok` type.
///
/// Note that there is a semantic difference between `BongoResult::Ok` containing
/// `Some` of an empty `Vec` and containing `None`. `None` means that there was no result,
/// an empty `Vec` means that there was a result containing zero rows.
///
///
/// `Err` of `BongoError` represents that the execution of a `BongoRequest` was not successful.
/// It contains the error message as a `String`.
///
pub type BongoResult = Result<Option<Vec<Row>>, BongoError>;

pub trait ToJson {
    fn to_json(&self) -> String;
}

impl ToJson for BongoResult {
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

pub trait TryFromJson<E> {
    fn try_from_json(json: &str) -> Result<Self, E>
    where
        Self: Sized;
}

impl TryFromJson<BongoError> for BongoResult {
    fn try_from_json(json: &str) -> Result<Self, BongoError> {
        match serde_json::from_str(json) {
            Ok(res) => Ok(res),
            Err(_) => Err(BongoError::DeserializerError),
        }
    }
}

#[cfg(test)]
mod tests {
    mod serialize {
        use crate::bongo_result::{BongoResult, ToJson};
        use crate::types::{BongoError, BongoLiteral};

        #[test]
        fn multiple_rows() {
            let r: BongoResult = Ok(Some(vec![
                vec![
                    BongoLiteral::Int(1),
                    BongoLiteral::Varchar("Günter".to_string()),
                    BongoLiteral::Bool(true),
                ],
                vec![
                    BongoLiteral::Int(2),
                    BongoLiteral::Varchar("Peter".to_string()),
                    BongoLiteral::Bool(false),
                ],
            ]));
            let serialized = r.to_json();

            assert_eq!(
                serialized,
                r#"{"Ok":[[{"Int":1},{"Varchar":"Günter"},{"Bool":true}],[{"Int":2},{"Varchar":"Peter"},{"Bool":false}]]}"#
            );
        }

        #[test]
        fn empty_rows() {
            let r: BongoResult = Ok(Some(vec![]));
            let serialized = serde_json::to_string(&r).unwrap();

            assert_eq!(serialized, r#"{"Ok":[]}"#);
        }

        #[test]
        fn none_rows() {
            let r: BongoResult = Ok(None);
            let serialized = r.to_json();

            assert_eq!(serialized, r#"{"Ok":null}"#);
        }

        #[test]
        fn error() {
            let r1: BongoResult = Err(BongoError::InternalError("Some example error".to_string()));
            let r2: BongoResult = Err(BongoError::EmptySqlStatementError);
            let serialized1 = r1.to_json();
            let serialized2 = r2.to_json();

            assert_eq!(
                serialized1,
                r#"{"Err":{"InternalError":"Some example error"}}"#
            );
            assert_eq!(serialized2, r#"{"Err":"EmptySqlStatementError"}"#);
        }
    }

    mod deserialize {
        use crate::bongo_result::{BongoResult, TryFromJson};
        use crate::types::{BongoError, BongoLiteral};

        #[test]
        fn multiple_rows() {
            let expected: BongoResult = Ok(Some(vec![
                vec![
                    BongoLiteral::Int(1),
                    BongoLiteral::Varchar("Günter".to_string()),
                    BongoLiteral::Bool(true),
                ],
                vec![
                    BongoLiteral::Int(2),
                    BongoLiteral::Varchar("Peter".to_string()),
                    BongoLiteral::Bool(false),
                ],
            ]));

            // this also tests if whitespaces are handled as expected
            let serialized = r#"
            {
            "Ok":
                [
                    [ { "Int": 1 }, { "Varchar": "Günter" }, {"Bool": true} ],
                    [ { "Int": 2 }, { "Varchar": "Peter" }, {"Bool":false} ]
                ]
            }
            "#;

            let result = BongoResult::try_from_json(serialized).unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn empty_rows() {
            let expected: BongoResult = Ok(Some(vec![]));
            let serialized = r#"{ "Ok": [] }"#;
            let result = BongoResult::try_from_json(serialized).unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn none_rows() {
            let expected: BongoResult = Ok(None);
            let serialized = r#"{ "Ok": null }"#;
            let result = BongoResult::try_from_json(serialized).unwrap();

            assert_eq!(result, expected);
        }

        #[test]
        fn error() {
            let expected1: BongoResult =
                Err(BongoError::InternalError("Some example error".to_string()));
            let expected2: BongoResult = Err(BongoError::EmptySqlStatementError);

            let serialized1 = r#"{"Err":{"InternalError":"Some example error"}}"#;
            let serialized2 = r#"{"Err":"EmptySqlStatementError"}"#;

            let result1 = BongoResult::try_from_json(serialized1).unwrap();
            let result2 = BongoResult::try_from_json(serialized2).unwrap();

            assert_eq!(result1, expected1);
            assert_eq!(result2, expected2);
        }
    }
}
