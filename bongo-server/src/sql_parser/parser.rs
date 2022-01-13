use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement as Ast, Query, SetExpr, Expr, TableFactor, TableWithJoins, OrderByExpr, ObjectName, Ident, ColumnDef, ObjectType};
use std::convert::TryFrom;
use bongo_core::types::{BongoError, ColumnDef as BongoColDef, Row};

use crate::statement::{Statement, SelectItem as BongoSelectItem, Order, Expr as BongoExpr, Assignment as BongoAssignment};
use crate::sql_parser::err_messages::{*};
use crate::util::conversions::{TryConvertAllExt, TryConvertOption};

///
/// `SqlParser` can parse sql into objects of type `Statement`.
///
/// As `Statement` is a simplification of the language features that SQL offers,
/// `SqlParser` does also not support all SQL features. Currently a library is used to parse the
/// SQL into an abstract syntax tree. To be able to work with that more easily `SqlParser` then
/// eliminates all the unnecessary complexity by converting the abstract syntax tree into the simpler
/// `Statement`.
///
pub struct SqlParser {}

impl SqlParser {
    ///
    /// Parses an SQL string into a `Statement` if possible.
    ///
    /// Possible reasons of errors when parsing are defined trough the `BongoError` enum.
    ///
    pub fn parse(sql: &str) -> Result<Statement, BongoError> {
        let dialect = GenericDialect {};

        let parse_result: Result<Vec<Ast>, ParserError> = Parser::parse_sql(&dialect, sql);

        return match parse_result {
            Ok(mut statements) => {
                if statements.is_empty() {
                    return Err(BongoError::EmptySqlStatementError);
                }
                Self::ast_to_statement(statements.remove(0))
            }
            Err(err) => {
                Err(BongoError::from(err))
            }
        };
    }

    fn ast_to_statement(ast: Ast) -> Result<Statement, BongoError> {
        return match ast {
            Ast::Query(query) => { Self::query_to_statement(*query) }
            Ast::Insert { .. } => { Self::insert_to_statement(ast) }
            Ast::Update { .. } => { Self::update_to_statement(ast) }
            Ast::Delete { .. } => { Self::delete_to_statement(ast) }
            Ast::CreateDatabase { mut db_name, .. } => { Self::obj_name_to_create_db(&mut db_name) }
            Ast::CreateTable { .. } => { Self::create_table_to_statement(ast) }
            Ast::Drop { .. } => { Self::drop_to_statement(ast) }
            _ => {
                unsupported_feature_err("Only the following statements are supported \
            by BongoDB: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE DATABASE, DROP TABLE, \
            DROP DATABASE.")
            }
        };
    }

    fn query_to_statement(query: Query) -> Result<Statement, BongoError> {
        return match query.body {
            SetExpr::Select(select) => {
                Ok(
                    Statement::Select {
                        cols: select.projection.try_convert_all(|item| { BongoSelectItem::try_from(item) })?,
                        table: Self::select_extract_table(&select.from)?,
                        order: Self::select_extract_order(query.order_by)?,
                        condition: select.selection.try_convert_option(|expr| { BongoExpr::try_from(expr) })?,
                    })
            }
            _ => {
                unsupported_feature_err("This query syntax is not supported.")
            }
        };
    }

    fn select_extract_table(tables_with_joins: &Vec<TableWithJoins>) -> Result<String, BongoError> {
        if tables_with_joins.len() != 1 {
            return only_single_table_from_err();
        }

        Self::table_name_from_table_with_joins(&tables_with_joins[0])
    }

    fn select_extract_order(mut order_by_exprs: Vec<OrderByExpr>) -> Result<Option<Order>, BongoError> {
        if order_by_exprs.len() == 0 {
            return Ok(None);
        }
        if order_by_exprs.len() > 1 {
            return order_by_only_one_column_err();
        }

        return Ok(Some(Order::try_from(order_by_exprs.remove(0))?));
    }

    fn insert_to_statement(insert: Ast) -> Result<Statement, BongoError> {
        return match insert {
            Ast::Insert { table_name, columns, source, .. } => {
                Ok(
                    Statement::Insert {
                        table: Self::insert_extract_table(table_name)?,
                        cols: Self::insert_extract_cols(columns)?,
                        rows: Self::insert_extract_rows(*source)?,
                    })
            }
            _ => { internal_error("insert_to_statement should only be called with the Insert variant.") }
        };
    }

    fn insert_extract_table(obj_name: ObjectName) -> Result<String, BongoError> {
        if obj_name.0.len() == 0 {
            return syntax_error("Specifying no one table on an INSERT statement is not valid.");
        }
        if obj_name.0.len() > 1 {
            return syntax_error("Specifying more than one table on an INSERT statement is not valid.");
        }

        return Ok(String::from(&obj_name.0[0].value));
    }

    fn insert_extract_cols(idents: Vec<Ident>) -> Result<Vec<String>, BongoError> {
        if idents.is_empty() {
            return unsupported_feature_err("BongoDB does only support INSERT statements \
            with explicit column lists. Example: INSERT INTO table_1 (col_1, col_2) ..");
        }
        Ok(
            idents.iter().map(|ident| {
                String::from(&ident.value)
            }).collect()
        )
    }

    fn insert_extract_rows(query: Query) -> Result<Vec<Row>, BongoError> {
        return match query.body {
            SetExpr::Values(values) => {
                values.0.into_iter()
                    .map(|exprs: Vec<Expr>| {
                        exprs.into_iter()
                            .map(|expr: Expr| {
                                let bongo_expr = BongoExpr::try_from(expr)?;
                                match bongo_expr {
                                    BongoExpr::Value(data) => { Ok(data) }
                                    _ => { insert_list_only_literals() }
                                }
                            }).collect()
                    }).collect()
            }
            _ => {
                unsupported_feature_err("In INSERT statements only value lists \
            are supported by BongoDB. Example: INSERT INTO table_1 (col_1, col_2) VALUES \
            (1, 'a'),\
            (2, 'b');")
            }
        };
    }

    fn update_to_statement(update: Ast) -> Result<Statement, BongoError> {
        return match update {
            Ast::Update { table, selection, assignments } => {
                Ok(Statement::Update {
                    table: Self::table_name_from_table_with_joins(&table)?,
                    assignments: assignments.try_convert_all(|assignment| { BongoAssignment::try_from(assignment) })?,
                    condition: selection.try_convert_option(|expr| { BongoExpr::try_from(expr) })?,
                })
            }
            _ => { internal_error("update_to_statement should only be called with the Update variant.") }
        };
    }

    fn table_name_from_table_with_joins(table_with_joins: &TableWithJoins) -> Result<String, BongoError> {
        let table_factor: &TableFactor = &table_with_joins.relation;

        return match &table_factor {
            TableFactor::Table { name, .. } => {
                if name.0.len() != 1 {
                    return only_single_table_from_err();
                }

                Ok(String::from(&name.0[0].value))
            }
            _ => {
                only_single_table_from_err()
            }
        };
    }

    fn string_from_obj_name(obj_name: &mut ObjectName) -> Result<String, BongoError> {
        if obj_name.0.len() != 1 {
            return unsupported_feature_err("BongoDB does not support whitespaces in \
            identifier names such as table names or column names");
        }

        Ok(String::from(obj_name.0.remove(0).value))
    }

    fn delete_to_statement(delete: Ast) -> Result<Statement, BongoError> {
        return match delete {
            Ast::Delete { selection, mut table_name } => {
                Ok(
                    Statement::Delete {
                        table: Self::string_from_obj_name(&mut table_name)?,
                        condition: selection.try_convert_option(|expr| { BongoExpr::try_from(expr) })?,
                    })
            }
            _ => { internal_error("delete_to_statement should only be called with the Delete variant.") }
        };
    }

    fn obj_name_to_create_db(_obj_name: &mut ObjectName) -> Result<Statement, BongoError> {
        unsupported_feature_err("BongoDB does not support CREATE DATABASE statements, because only one database is supported.")
        // Ok(Statement::CreateDB { table: Self::string_from_obj_name(obj_name)? })
    }

    fn create_table_to_statement(create_table: Ast) -> Result<Statement, BongoError> {
        return match create_table {
            Ast::CreateTable { mut name, columns, .. } => {
                Ok(Statement::CreateTable {
                    table: Self::string_from_obj_name(&mut name)?,
                    cols: columns.try_convert_all(|col_def: ColumnDef| { BongoColDef::try_from(&col_def) })?,
                    // cols: Self::vec_col_def_from_vec_col_def(&mut columns)?,
                })
            }
            _ => { internal_error("create_table_to_statement should only be called with the CreateTable variant.") }
        };
    }

    fn drop_to_statement(drop: Ast) -> Result<Statement, BongoError> {
        return match drop {
            Ast::Drop { object_type, names, .. } => {
                match object_type {
                    ObjectType::Table => {
                        Ok(Statement::DropTable {
                            names: names.try_convert_all(|mut obj_name| { Self::string_from_obj_name(&mut obj_name) })?
                            // names: Self::vec_string_from_vec_obj_names(names)?
                        })
                    }
                    _ => { unsupported_feature_err("BongoDB only supports DROP statements for TABLEs.") }
                }
            }
            _ => { internal_error("drop_to_statement should only be called with the Drop variant.") }
        };
    }
}

#[cfg(test)]
mod tests {
    mod select {
        use bongo_core::types::BongoLiteral;
        use crate::sql_parser::parser::SqlParser;
        use crate::statement::{Statement, SelectItem, Order, Expr as BongoExpr, BinOp as BongoBinOp};

        #[test]
        fn all_features_together() {
            let sql = "SELECT *, col_1, col_2 \
           FROM table_1 \
           WHERE a > b AND b <= 100 \
           ORDER BY col_1 ASC";

            let expected_statement = Statement::Select {
                cols: vec![
                    SelectItem::Wildcard,
                    SelectItem::ColumnName(String::from("col_1")),
                    SelectItem::ColumnName(String::from("col_2"))
                ],
                table: String::from("table_1"),
                order: Some(Order::Asc(String::from("col_1"))),
                condition: Some(BongoExpr::BinaryExpr {
                    left: Box::new(BongoExpr::BinaryExpr {
                        left: Box::new(BongoExpr::Identifier(String::from("a"))),
                        op: BongoBinOp::Gt,
                        right: Box::new(BongoExpr::Identifier(String::from("b"))),
                    }),
                    op: BongoBinOp::And,
                    right: Box::new(BongoExpr::BinaryExpr {
                        left: Box::new(BongoExpr::Identifier(String::from("b"))),
                        op: BongoBinOp::LtEq,
                        right: Box::new(BongoExpr::Value(BongoLiteral::Int(100))),
                    }),
                }),
            };

            let statement = SqlParser::parse(sql);

            assert_eq!(statement, Ok(expected_statement));
        }

        #[test]
        fn no_condition() {
            let sql = "SELECT col_1, col_2 \
           FROM table_1";

            let expected_statement = Statement::Select {
                cols: vec![
                    SelectItem::ColumnName(String::from("col_1")),
                    SelectItem::ColumnName(String::from("col_2"))
                ],
                table: String::from("table_1"),
                order: None,
                condition: None,
            };

            let statement = SqlParser::parse(sql);

            assert_eq!(statement, Ok(expected_statement));
        }

        #[test]
        fn asc_as_default() {
            let sql = "SELECT *\
            FROM table_1 \
            ORDER BY col_1;";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Select {
                cols: vec![
                    SelectItem::Wildcard
                ],
                table: String::from("table_1"),
                condition: None,
                order: Some(Order::Asc("col_1".to_string())),
            };

            assert_eq!(statement, Ok(expected_statement));
        }

        #[test]
        fn desc() {
            let sql = "SELECT *\
            FROM table_1 \
            ORDER BY col_1 DESC;";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Select {
                cols: vec![
                    SelectItem::Wildcard
                ],
                table: String::from("table_1"),
                condition: None,
                order: Some(Order::Desc("col_1".to_string())),
            };

            assert_eq!(statement, Ok(expected_statement));
        }

        #[test]
        fn simple_wildcard() {
            let sql = "SELECT * FROM table_1";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Select {
                cols: vec![
                    SelectItem::Wildcard
                ],
                table: String::from("table_1"),
                condition: None,
                order: None,
            };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod insert {
        use bongo_core::types::BongoLiteral;
        use super::super::SqlParser;
        use crate::statement::Statement;

        #[test]
        fn mutilple_rows() {
            let sql = r#"INSERT INTO table_1 (col_1, col_2, col_3) VALUES
                              (1, 'a', true),
                              (2, 'b', false),
                              (3, 'c', Null);"#;

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Insert {
                table: "table_1".to_string(),
                cols: vec!["col_1".to_string(), "col_2".to_string(), "col_3".to_string()],
                rows: vec![
                    vec![BongoLiteral::Int(1), BongoLiteral::Varchar("a".to_string(), "a".len()), BongoLiteral::Bool(true)],
                    vec![BongoLiteral::Int(2), BongoLiteral::Varchar("b".to_string(), "b".len()), BongoLiteral::Bool(false)],
                    vec![BongoLiteral::Int(3), BongoLiteral::Varchar("c".to_string(), "a".len()), BongoLiteral::Null]
                ],
            };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod update {
        use bongo_core::types::BongoLiteral;
        use super::super::SqlParser;
        use crate::statement::{Statement, Assignment};

        #[test]
        fn multiple_set_expr() {
            let sql = r#"UPDATE table_1
            SET col_1 = 2,
                col_2 = 'new_value';"#;

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Update {
                table: "table_1".to_string(),
                assignments: vec![
                    Assignment {
                        col_name: "col_1".to_string(),
                        val: BongoLiteral::Int(2),
                    },
                    Assignment {
                        col_name: "col_2".to_string(),
                        val: BongoLiteral::Varchar("new_value".to_string(), "new_value".len()),
                    }
                ],
                condition: None,
            };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod delete {
        use bongo_core::types::BongoLiteral;
        use super::super::SqlParser;
        use crate::statement::{Statement, Expr as BongoExpr, BinOp as BongoBinOp};

        #[test]
        fn nested_condition() {
            let sql = r#"DELETE FROM table_1
           WHERE a != b OR c = false"#;

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Delete {
                table: "table_1".to_string(),
                condition: Some(BongoExpr::BinaryExpr {
                    left: Box::new(BongoExpr::BinaryExpr {
                        left: Box::new(BongoExpr::Identifier(String::from("a"))),
                        op: BongoBinOp::NotEq,
                        right: Box::new(BongoExpr::Identifier(String::from("b"))),
                    }),
                    op: BongoBinOp::Or,
                    right: Box::new(BongoExpr::BinaryExpr {
                        left: Box::new(BongoExpr::Identifier(String::from("c"))),
                        op: BongoBinOp::Eq,
                        right: Box::new(BongoExpr::Value(BongoLiteral::Bool(false))),
                    }),
                }),
            };

            assert_eq!(statement, Ok(expected_statement));
        }

        #[test]
        fn no_condition() {
            let sql = "DELETE FROM table_1";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Delete {
                table: "table_1".to_string(),
                condition: None,
            };

            assert_eq!(statement, Ok(expected_statement));
        }
    }


    mod create_table {
        use super::super::SqlParser;
        use crate::statement::Statement;
        use bongo_core::types::{ColumnDef as BongoColDef, BongoDataType};

        #[test]
        fn all_data_types_once() {
            let sql = "CREATE TABLE table_1 \
                                ( \
                                    col_1 INT, \
                                    col_2 BOOLEAN, \
                                    col_3 VARCHAR(256), \
                                ); ";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::CreateTable {
                table: "table_1".to_string(),
                cols: vec![
                    BongoColDef { name: "col_1".to_string(), data_type: BongoDataType::Int },
                    BongoColDef { name: "col_2".to_string(), data_type: BongoDataType::Bool },
                    BongoColDef { name: "col_3".to_string(), data_type: BongoDataType::Varchar(256) },
                ],
            };

            assert_eq!(statement, Ok(expected_statement));
        }
    }


    mod drop_table {
        use super::super::SqlParser;
        use crate::statement::Statement;

        #[test]
        fn drop_table() {
            let sql = "DROP TABLE table_1";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::DropTable { names: vec!["table_1".to_string()] };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod drop_db {
        use super::super::SqlParser;
        use crate::statement::Statement;

        ///
        /// CREATE DB seems to not be properly processed by the used 3rd party library,
        /// furthermore we assume in BongoDB that we only have one database which makes this
        /// statement obsolete.
        ///
        #[ignore]
        #[test]
        fn drop_db() {
            let sql = r#"DROP DATABASE db_1;"#;

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::DropDB { database: "db_1".to_string() };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod create_db {
        use crate::statement::Statement;
        use super::super::SqlParser;

        ///
        /// CREATE DB seems to not be properly processed by the used 3rd party library,
        /// furthermore we assume in BongoDB that we only have one database which makes this
        /// statement obsolete.
        ///
        #[test]
        #[ignore]
        fn create_db() {
            let sql = "CREATE DATABASE db_1;";

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::CreateDB { name: "db_1".to_string() };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod diverse {
        use bongo_core::types::BongoError::EmptySqlStatementError;
        use crate::sql_parser::parser::SqlParser;

        #[test]
        fn empty_statement(){
            let sql = "";

            let response = SqlParser::parse(sql);

            let expected_response = Err(EmptySqlStatementError);

            assert_eq!(response, expected_response);
        }
    }
}
