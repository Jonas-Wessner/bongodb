pub struct SqlParser {}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement as Ast, Query, SetExpr, SelectItem, Expr, Select, TableFactor, TableWithJoins, BinaryOperator, OrderByExpr};
use crate::statement::{Statement, SelectItem as BongoSelectItem, Order, Expr as BongoExpr, BinOp as BongoBinOp};
use std::fmt;
use std::ascii::escape_default;
use std::convert::TryFrom;

fn unsupported_feature_err<T>(err_message: &str) -> Result<T, String> {
    Err(String::from("Unsupported Feature: ") + err_message)
}

fn only_single_table_from_err<T>() -> Result<T, String> {
    unsupported_feature_err("Only single identifiers are supported in the FROM clause. Example: Select col_1 FROM table_1")
}

fn order_by_only_one_column_err<T>() -> Result<T, String> {
    unsupported_feature_err("ORDER BY is only supported with exactly one argument which must be a column name.")
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
            Ast::Insert { .. } => { Err(String::from("not yet implemented")) }
            Ast::Update { .. } => { Err(String::from("not yet implemented")) }
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
                        cols: Self::extract_select_cols(&select)?,
                        table: Self::extract_select_table(&select)?,
                        order: Self::extract_select_order(&query.order_by)?,
                        condition: Self::extract_select_condition(*select)?,
                    })
            }
            _ => {
                unsupported_feature_err("This query syntax is not supported.")
            }
        };
    }

    fn extract_select_cols(select: &Select) -> Result<Vec<BongoSelectItem>, String> {
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

    fn extract_select_table(select: &Select) -> Result<String, String> {
        let tables_with_joins: &Vec<TableWithJoins> = &select.from;

        if tables_with_joins.len() != 1 {
            return only_single_table_from_err();
        }
        let table_factor: &TableFactor = &tables_with_joins[0].relation;

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

    fn extract_select_condition(select: Select) -> Result<Option<BongoExpr>, String> {
        return match select.selection {
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

    fn extract_select_order(order_by_exprs: &Vec<OrderByExpr>) -> Result<Option<Order>, String> {
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

    fn expr_is_supported_order_by(expr: &Expr) -> bool {
        // so far only ordering by one column is supported -> only identifiers are valid
        return match expr {
            Expr::Identifier(_) => { true }
            _ => { false }
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

        use super::super::SqlParser;
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
}




