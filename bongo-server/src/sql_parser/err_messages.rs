use bongo_core::types::BongoError;

///
/// This module contains a couple of helper functions that are used to reduce code duplication
/// regarding returning `Err(BongoError)` types.
///

pub fn syntax_error<T>(err_message: &str) -> Result<T, BongoError> {
    Err(BongoError::SqlSyntaxError(String::from(err_message)))
}

pub fn internal_error<T>(err_message: &str) -> Result<T, BongoError> {
    Err(BongoError::InternalError(String::from(err_message)))
}

pub fn unsupported_feature_err<T>(err_message: &str) -> Result<T, BongoError> {
    Err(BongoError::UnsupportedFeatureError(String::from(
        err_message,
    )))
}

pub fn only_single_table_from_err<T>() -> Result<T, BongoError> {
    unsupported_feature_err(
        "Only single identifiers are supported in a list of tables \
    i.e. multiple tables are not supported. Example 1: Select col_1 FROM table_1; Example 2: \
    UPDATE table_name SET col_1 = 5;",
    )
}

pub fn order_by_only_one_column_err<T>() -> Result<T, BongoError> {
    unsupported_feature_err(
        "ORDER BY is only supported with exactly one argument which must be a column name.",
    )
}

pub fn insert_list_only_literals<T>() -> Result<T, BongoError> {
    syntax_error("Only literals can appear in VALUES lists of insert statements")
}

pub fn generic_write_error<T>() -> Result<T, BongoError> {
    Err(BongoError::WriteFileError("Failure modifying file.".to_string()))
}
