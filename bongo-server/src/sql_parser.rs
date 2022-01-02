pub struct SqlParser {}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement as Ast, Query, SetExpr, SelectItem, Expr, Select, TableFactor, TableWithJoins, BinaryOperator, OrderByExpr, ObjectName, Ident, Assignment};
use crate::statement::{Statement, SelectItem as BongoSelectItem, Order, Expr as BongoExpr, BinOp as BongoBinOp, Assignment as BongoAssignment};
use std::fmt;
use std::ascii::escape_default;
use std::convert::TryFrom;
use crate::types::{Row, BongoDataType};
use crate::statement::Expr::Value;

fn syntax_error<T>(err_message: &str) -> Result<T, String> {
    Err(String::from("Syntax Error: ") + err_message)
}

fn internal_error<T>(err_message: &str) -> Result<T, String> {
    Err(String::from("Internal Error: ") + err_message)
}


fn unsupported_feature_err<T>(err_message: &str) -> Result<T, String> {
    Err(String::from("Unsupported Feature: ") + err_message)
}


fn only_single_table_from_err<T>() -> Result<T, String> {
    unsupported_feature_err("Only single identifiers are supported in a list of tables \
    i.e. multiple tables are not supported. Example 1: Select col_1 FROM table_1; Example 2: \
    UPDATE table_name SET col_1 = 5;")
}

fn order_by_only_one_column_err<T>() -> Result<T, String> {
    unsupported_feature_err("ORDER BY is only supported with exactly one argument which must be a column name.")
}

fn insert_list_only_literals<T>() -> Result<T, String> {
    syntax_error("Only literals can appear in VALUES lists of insert statements")
}

// TODO: return appropriate errors on all unsafe accesses such as absolute vector indexers

impl SqlParser {
    pub fn parse(sql: &str) -> Result<Statement, String> {
        let dialect = GenericDialect {};

        let mut parse_result: Result<Vec<Ast>, ParserError> = Parser::parse_sql(&dialect, sql);

        return match parse_result {
            Ok(mut stmts) => {
                // move element out of vector for later use, as the vector is not used anyways
                Self::ast_to_statement(stmts.remove(0))
            }
            Err(err) => {
                match err {
                    ParserError::TokenizerError(m) | ParserError::ParserError(m) => {
                        Err(String::from(m))
                    }
                }
            }
        };
    }

    fn ast_to_statement(ast: Ast) -> Result<Statement, String> {
        return match ast {
            Ast::Query(query) => { Self::query_to_statement(*query) }
            Ast::Insert { .. } => { Self::insert_to_statement(ast) }
            Ast::Update { .. } => { Err(format!("{:?}", ast)) }
            Ast::Delete { .. } => { Err(String::from("not yet implemented")) }
            Ast::CreateTable { .. } => { Err(String::from("not yet implemented")) }
            Ast::Drop { .. } => { Err(String::from("not yet implemented")) }
            _ => { unsupported_feature_err("Only the following statements are supported by BongoDB: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE DATABASE, DROP TABLE, DROP DATABASE.") }
        };
    }

    fn query_to_statement(query: Query) -> Result<Statement, String> {
        return match query.body {
            SetExpr::Select(select) => {
                Ok(
                    Statement::Select {
                        cols: Self::select_extract_cols(&select)?,
                        table: Self::select_extract_table(&select)?,
                        order: Self::select_extract_order(&query.order_by)?,
                        condition: Self::opt_bongo_expr_from_opt_expr(select.selection)?,
                    })
            }
            _ => {
                unsupported_feature_err("This query syntax is not supported.")
            }
        };
    }

    fn select_extract_cols(select: &Select) -> Result<Vec<BongoSelectItem>, String> {
        let items: Vec<Result<BongoSelectItem, String>> = select.projection.iter()
            .map(|item: &SelectItem| -> Result<BongoSelectItem, String> {
                return match item {
                    SelectItem::UnnamedExpr(expr) => {
                        match expr {
                            Expr::Identifier(ident) => {
                                Ok(BongoSelectItem::ColumnName(String::from(&ident.value)))
                            }
                            _ => {
                                unsupported_feature_err("Only identifiers are supported in a projection inside a SELECT.")
                            }
                        }
                    }
                    SelectItem::Wildcard => {
                        Ok(BongoSelectItem::Wildcard)
                    }
                    _ => {
                        unsupported_feature_err("Only unqualified Wildcards are supported. Example: `SELECT * FROM ...`")
                    }
                };
            }).collect::<Vec<Result<BongoSelectItem, String>>>();

        let errors = items.iter()
            .filter_map(|result| -> Option<String> {
                return match result {
                    Ok(_) => {
                        None
                    }
                    Err(error) => {
                        Some(String::from(error))
                    }
                };
            }).collect::<Vec<String>>();

        return if !errors.is_empty() {
            // return first error if errors exist
            Err(String::from(&errors[0]))
        } else {
            Ok(items.into_iter()
                .map(|item| -> BongoSelectItem {
                    item.unwrap()
                }).collect::<Vec<BongoSelectItem>>())
        };
    }

    fn select_extract_table(select: &Select) -> Result<String, String> {
        let tables_with_joins: &Vec<TableWithJoins> = &select.from;
        if tables_with_joins.len() != 1 {
            return only_single_table_from_err();
        }

        Self::table_name_from_table_with_joins(&tables_with_joins[0])
    }

    fn select_extract_order(order_by_exprs: &Vec<OrderByExpr>) -> Result<Option<Order>, String> {
        if order_by_exprs.len() == 0 {
            return Ok(None);
        }
        if order_by_exprs.len() > 1 {
            return order_by_only_one_column_err();
        }

        let order_by_expr = &order_by_exprs[0];

        return match &order_by_expr.expr {
            Expr::Identifier(ident) => {
                let column = String::from(&ident.value);

                return Ok(Some(
                    match order_by_expr {
                        OrderByExpr { asc, .. } => {
                            match asc {
                                None => { Order::Asc(column) }
                                Some(is_asc) => {
                                    match is_asc {
                                        true => { Order::Asc(column) }
                                        false => { Order::Desc(column) }
                                    }
                                }
                            }
                        }
                    }
                ));
            }
            _ => {
                order_by_only_one_column_err()
            }
        };
    }

    fn insert_to_statement(insert: Ast) -> Result<Statement, String> {
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

    fn insert_extract_table(obj_name: ObjectName) -> Result<String, String> {
        if obj_name.0.len() == 0 {
            return Err("Specifying more than one table on an INSERT statement is not valid.".to_string());
        }
        if obj_name.0.len() > 1 {
            return Err("Specifying more than one table on an INSERT statement is not valid.".to_string());
        }

        return Ok(String::from(&obj_name.0[0].value));
    }

    fn insert_extract_cols(idents: Vec<Ident>) -> Result<Vec<String>, String> {
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

    fn insert_extract_rows(query: Query) -> Result<Vec<Row>, String> {
        return match query.body {
            SetExpr::Values(values) => {
                values.0.into_iter()
                    .map(|exprs: Vec<Expr>| {
                        exprs.into_iter()
                            .map(|expr: Expr| {
                                return
                                    match BongoExpr::try_from(expr) {
                                        Ok(bongo_expr) => {
                                            match bongo_expr {
                                                BongoExpr::Value(data) => { Ok(data) }
                                                _ => { insert_list_only_literals() }
                                            }
                                        }
                                        Err(_) => { insert_list_only_literals() }
                                    };
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

    fn update_to_statement(update: Ast) -> Result<Statement, String> {
        return match update {
            Ast::Update { table, selection, assignments } => {
                Ok(Statement::Update {
                    table: Self::table_name_from_table_with_joins(&table)?,
                    assignments: Self::bongo_assignment_from_assignments(assignments)?,
                    condition: Self::opt_bongo_expr_from_opt_expr(selection)?,
                })
            }
            _ => { internal_error("update_to_statement should only be called with the Update variant.") }
        };
    }

    fn table_name_from_table_with_joins(table_with_joins: &TableWithJoins) -> Result<String, String> {
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

    fn opt_bongo_expr_from_opt_expr(expr: Option<Expr>) -> Result<Option<BongoExpr>, String> {
        return match expr {
            Some(cond) => {
                match BongoExpr::try_from(cond) {
                    Ok(bongo_expr) => { Ok(Some(bongo_expr)) }
                    Err(_) => { unsupported_feature_err("This type of expression is not supported in a WHERE clause by BongoDB.") }
                }
            }
            None => {
                Ok(None)
            }
        };
    }

    fn bongo_assignment_from_assignments(assignments: Vec<Assignment>) -> Result<Vec<BongoAssignment>, String> {
        let mut ok = false;
        let bongo_assignments = assignments.into_iter()
            .map(|assignment| {
                return match BongoAssignment::try_from(assignment) {
                    Ok(bongo_assignment) => { bongo_assignment }
                    Err(_) => {
                        // set flag to false and return placeholder for syntactic correctness
                        ok = false;
                        BongoAssignment { varname: "".to_string(), val: BongoDataType::Null }
                    }
                };
            }).collect();

        return if ok {
            Ok(bongo_assignments)
        } else {
            unsupported_feature_err("This type of assignment is not supported by BongoDB. \
            BongoDB only supports assignments where the left hand side operand is a column name and \
            the right hand side operator is a literal.")
        };
    }
}

#[cfg(test)]
mod tests {
    mod select {
        use sqlparser::ast::{Expr, Ident, BinaryOperator};
        use sqlparser::ast::Expr::{BinaryOp, Identifier, Value};
        use sqlparser::tokenizer::Token::Number;
        use sqlparser::ast::Value as ValueEnum;

        use crate::sql_parser::SqlParser;
        use crate::statement::{Statement, SelectItem, Order, Expr as BongoExpr, BinOp as BongoBinOp};
        use crate::statement::SelectItem::Wildcard;
        use crate::types::BongoDataType;

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
                        right: Box::new((BongoExpr::Identifier(String::from("b")))),
                    }),
                    op: BongoBinOp::And,
                    right: Box::new(BongoExpr::BinaryExpr {
                        left: Box::new(BongoExpr::Identifier(String::from("b"))),
                        op: BongoBinOp::LtEq,
                        right: Box::new(BongoExpr::Value(BongoDataType::Int(100))),
                    }),
                }),
            };

            let statement = SqlParser::parse(sql);

            assert_eq!(statement, Ok(expected_statement));
        }

        #[test]
        fn simple_wildcard() {
            let sql = "SELECT * FROM table_1;";

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
        use crate::sql_parser::SqlParser;
        use crate::statement::Statement;
        use crate::types::BongoDataType;

        #[test]
        fn insert_multiple() {
            let sql = r#"INSERT INTO table_1 (col_1, col_2, col_3) VALUES
                              (1, 'a', true),
                              (2, 'b', false),
                              (3, 'c', Null);"#;

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Insert {
                table: "table_1".to_string(),
                cols: vec!["col_1".to_string(), "col_2".to_string(), "col_3".to_string()],
                rows: vec![
                    vec![BongoDataType::Int(1), BongoDataType::Varchar("a".to_string(), "a".len()), BongoDataType::Bool(true)],
                    vec![BongoDataType::Int(2), BongoDataType::Varchar("b".to_string(), "b".len()), BongoDataType::Bool(false)],
                    vec![BongoDataType::Int(3), BongoDataType::Varchar("c".to_string(), "a".len()), BongoDataType::Null]
                ],
            };

            assert_eq!(statement, Ok(expected_statement));
        }
    }

    mod update {
        use crate::sql_parser::SqlParser;
        use crate::statement::{Statement, Assignment};
        use crate::types::BongoDataType;

        #[test]
        fn multiple_set_expr() {
            let sql = r#"UPDATE table_1
            SET col_1 = 2
                col_2 = 'new_value;'"#;

            let statement = SqlParser::parse(sql);

            let expected_statement = Statement::Update {
                table: "table_1".to_string(),
                assignments: vec![
                    Assignment {
                        varname: "col_1".to_string(),
                        val: BongoDataType::Int(2),
                    },
                    Assignment {
                        varname: "col_2".to_string(),
                        val: BongoDataType::Varchar("new_value".to_string(), "new_value".len()),
                    }
                ],
                condition: None,
            };
        }
    }
}




