pub struct SqlParser {}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement as Ast, Query, SetExpr, SelectItem, Expr, Select, TableFactor, TableWithJoins};
use crate::statement::{Statement, SelectItem as BongoSelectItem, Ordering};
use std::fmt;

fn unsupported_feature_err<T>(err_message: &str) -> Result<T, String> {
    Err(String::from("Unsupported Feature: ") + err_message)
}

fn only_single_table_from_err<T>() -> Result<T, String> {
    unsupported_feature_err("Only single identifiers are supported in the FROM clause. Example: Select col_1 FROM table_1")
}

// TODO: return appropriate errors on all unsafe accesses such as absolute vector indexers

impl SqlParser {
    pub fn parse(sql: &str) -> Result<Statement, String> {
        let dialect = GenericDialect {};

        let sql = "SELECT *, col_1, col_2 \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY col_1";

        let parse_result = Parser::parse_sql(&dialect, sql);

        let ast;
        match &parse_result {
            Ok(stmts) => {
                ast = &stmts[0];
            }
            Err(err) => {
                return match err {
                    ParserError::TokenizerError(m) | ParserError::ParserError(m) => {
                        Err(String::from(m))
                    }
                };
            }
        }

        return Self::ast_to_statement(&ast);
    }

    fn ast_to_statement(ast: &Ast) -> Result<Statement, String> {
        return match ast {
            Ast::Query(query) => { Self::query_to_statement(query) }
            Ast::Insert { .. } => { Err(String::from("not yet implemented")) }
            Ast::Update { .. } => { Err(String::from("not yet implemented")) }
            Ast::Delete { .. } => { Err(String::from("not yet implemented")) }
            Ast::CreateTable { .. } => { Err(String::from("not yet implemented")) }
            Ast::Drop { .. } => { Err(String::from("not yet implemented")) }
            _ => { unsupported_feature_err("Only the following statements are supported by BongoDB: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE DATABASE, DROP TABLE, DROP DATABASE.") }
        };
    }

    fn query_to_statement(query: &Query) -> Result<Statement, String> {
        println!("query: {:?}", query);
        return match &query.body {
            SetExpr::Select(select) => {
                println!("select: {:?}", select);


                Ok(Statement::Select {
                    cols: Self::extract_select_cols(&select)?,
                    table: Self::extract_table(&select)?,
                    condition: None,
                    // TODO: implement condition parsing
                    ordering: Ordering::Asc("ordering parsing not implemented".to_string()),
                    // TODO: implement ordering parsing
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

    fn extract_table(select: &Select) -> Result<String, String> {
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
}