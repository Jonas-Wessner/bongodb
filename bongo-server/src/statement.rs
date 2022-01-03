use crate::types::{BongoLiteral, ColumnDef, Row, BongoError};
use std::convert::TryFrom;
use sqlparser::ast::{Expr as SqlParserExpr, Value, BinaryOperator as SqlParserBinOp, BinaryOperator, Assignment as SqlParserAssignment, SelectItem as SqlParserSelectItem, OrderByExpr};
use std::num::ParseIntError;
use crate::statement::Expr::BinaryExpr;

#[derive(Debug)]
#[derive(PartialEq)]
pub struct Assignment {
    pub varname: String,
    pub val: BongoLiteral,
}

impl TryFrom<SqlParserAssignment> for Assignment {
    type Error = BongoError;

    fn try_from(parser_assignment: SqlParserAssignment) -> Result<Self, Self::Error> {
        if parser_assignment.id.len() != 1 {
            return Err(BongoError::UnsupportedFeatureError("Only single identifiers are supported \
            in assignments by BongoDB.".to_string()));
        }

        let expr = Expr::try_from(parser_assignment.value)?;

        return match expr {
            Expr::Value(val) => {
                Ok(
                    Self {
                        varname: String::from(&parser_assignment.id[0].value),
                        val,
                    })
            }
            _ => { Err(BongoError::UnsupportedFeatureError("Only values are supported in assignments".to_string())) }
        };
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum Order {
    Asc(String),
    Desc(String),
}

impl TryFrom<OrderByExpr> for Order {
    type Error = BongoError;

    fn try_from(order_expr: OrderByExpr) -> Result<Self, Self::Error> {
        return match order_expr.expr {
            SqlParserExpr::Identifier(ident) => {
                let column = String::from(&ident.value);

                Ok(
                    match order_expr.asc {
                        None => { Order::Asc(column) }
                        Some(is_asc) => {
                            match is_asc {
                                true => { Order::Asc(column) }
                                false => { Order::Desc(column) }
                            }
                        }
                    })
            }
            _ => {
                Err(BongoError::UnsupportedFeatureError("ORDER BY is only supported with exactly \
                one argument which must be a column name.".to_string()))
            }
        };
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum SelectItem {
    ColumnName(String),
    Wildcard,
}

impl TryFrom<SqlParserSelectItem> for SelectItem {
    type Error = BongoError;

    fn try_from(item: SqlParserSelectItem) -> Result<Self, Self::Error> {
        let error = Err(BongoError::UnsupportedFeatureError("Only identifiers and unqualified wildcards \
            are supported as select items by BongoDB.".to_string()));
        match item {
            SqlParserSelectItem::UnnamedExpr(expr) => {
                match expr {
                    SqlParserExpr::Identifier(ident) => { Ok(SelectItem::ColumnName(String::from(&ident.value))) }
                    _ => { error }
                }
            }
            SqlParserSelectItem::Wildcard => { Ok(SelectItem::Wildcard) }
            _ => { error }
        }
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum BinOp {
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
}

impl TryFrom<SqlParserBinOp> for BinOp {
    type Error = BongoError;

    fn try_from(value: SqlParserBinOp) -> Result<Self, Self::Error> {
        return match value {
            BinaryOperator::Gt => { Ok(BinOp::Gt) }
            BinaryOperator::Lt => { Ok(BinOp::Lt) }
            BinaryOperator::GtEq => { Ok(BinOp::GtEq) }
            BinaryOperator::LtEq => { Ok(BinOp::LtEq) }
            BinaryOperator::Eq => { Ok(BinOp::Eq) }
            BinaryOperator::NotEq => { Ok(BinOp::NotEq) }
            BinaryOperator::And => { Ok(BinOp::And) }
            BinaryOperator::Or => { Ok(BinOp::Or) }
            _ => {
                Err(BongoError::UnsupportedFeatureError("Only the Operators >, <, >=, <=, =, !=, \
                AND, OR are supported by BongoDB".to_string()))
            }
        };
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum Expr {
    BinaryExpr {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Identifier(String),
    Value(BongoLiteral),
}

impl TryFrom<SqlParserExpr> for Expr {
    type Error = BongoError;

    fn try_from(expr: SqlParserExpr) -> Result<Self, Self::Error> {
        return match expr {
            SqlParserExpr::Identifier(ident) => { Ok(Expr::Identifier(ident.value)) }
            SqlParserExpr::Value(value) => {
                return match value {
                    Value::Number(lit, ..) => {
                        match str::parse::<i64>(&lit) {
                            Ok(val) => { Ok(Expr::Value(BongoLiteral::Int(val))) }
                            Err(_) => { Err(BongoError::InternalError("Failed to parse int value.".to_string())) }
                        }
                    }
                    Value::SingleQuotedString(lit) | Value::DoubleQuotedString(lit) =>
                        { Ok(Expr::Value(BongoLiteral::Varchar(String::from(&lit), lit.len()))) }
                    Value::Boolean(val) => { Ok(Expr::Value(BongoLiteral::Bool(val))) }
                    Value::Null => { Ok(Expr::Value(BongoLiteral::Null)) }
                    _ => {
                        Err(BongoError::UnsupportedFeatureError("Only integers, single quoted strings \
                    booleans and NULL values are supported as literals by BongoDB.".to_string()))
                    }
                };
            }
            SqlParserExpr::BinaryOp { left, op, right } => {
                // Each operand of a binary operation must also itself be a supported expression
                // and the operation of a binary expression must be convertible.
                // Recursively try the conversion and bubble up errors
                Ok(Expr::BinaryExpr {
                    left: Box::new(Expr::try_from(*left)?),
                    op: BinOp::try_from(op)?,
                    right: Box::new(Expr::try_from(*right)?),
                })
            }
            _ => {
                Err(BongoError::UnsupportedFeatureError("Only identifiers, values, and binary \
            operations are supported as expressions by BongoDB.".to_string()))
            }
        };
    }
}

///
/// `Statement` is the type of statement that the `SqlParser` of `BongoServer` uses.
/// This is a simplified view of the statement and does not support all sql features.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum Statement {
    Select {
        cols: Vec<SelectItem>,
        table: String,
        condition: Option<Expr>,
        order: Option<Order>,
    },
    Insert {
        table: String,
        cols: Vec<String>,
        rows: Vec<Row>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        condition: Option<Expr>,
    },
    Delete {
        table: String,
        condition: Option<Expr>,
    },
    CreateTable {
        table: String,
        cols: Vec<ColumnDef>,
    },
    DropTable { names: Vec<String> },
    // NOTE: currently not supported, because BongoDB asserts having only exactly one DB
    CreateDB { name: String },
    // NOTE: currently not supported, because BongoDB asserts having only exactly one DB
    DropDB { database: String },
}