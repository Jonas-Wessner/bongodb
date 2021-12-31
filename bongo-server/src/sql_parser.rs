pub struct SqlParser {}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement as Ast, Query, SetExpr, SelectItem, Expr, Select};
use crate::statement::{Statement, SelectItem as BongoSelectItem, Ordering};
use std::fmt;

impl SqlParser {
    pub fn parse(sql: &str) -> Result<Statement, String> {
        let dialect = GenericDialect {};

        let sql = "SELECT *, col_1, col_2 \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

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
        match ast {
            Ast::Query(query) => {
                return Self::query_to_statement(query);
            }
            Ast::Insert { .. } => {}
            Ast::Update { .. } => {}
            Ast::Delete { .. } => {}
            Ast::CreateTable { .. } => {}
            Ast::Drop { .. } => {}
            _ => {}
        }
        return Err(String::from("Not yet implemented"));
    }

    fn query_to_statement(query: &Query) -> Result<Statement, String> {
        println!("query: {:?}", query);
        return match &query.body {
            SetExpr::Select(select) => {
                println!("select: {:?}", select);

                let cols = Self::extract_select_cols(&select)?;

                Ok(Statement::Select {
                    cols,
                    // TODO: implement table parsing
                    table: "table parsing not implemented".to_string(),
                    condition: None,
                    // TODO: implement condition parsing
                    ordering: Ordering::Asc("ordering parsing not implemented".to_string()),
                    // TODO: implement ordering parsing
                })
            }
            _ => {
                Err(String::from("Unsupported Feature: This query syntax is not supported."))
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
                                Err(String::from("Unsupported Feature: only identifiers are supported to be selected."))
                            }
                        }
                    }
                    SelectItem::Wildcard => {
                        Ok(BongoSelectItem::Wildcard)
                    }
                    _ => {
                        Err(String::from("Unsupported Feature: Only unqualified Wildcards are supported."))
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
}