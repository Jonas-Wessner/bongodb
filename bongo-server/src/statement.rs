use crate::types::{BongoDataType, Column, Row};
use std::convert::TryFrom;
use sqlparser::ast::{Expr as SqlParserExpr, Value, BinaryOperator as SqlParserBinOp, BinaryOperator};
use std::num::ParseIntError;
use crate::statement::Expr::BinaryExpr;

#[derive(Debug)]
#[derive(PartialEq)]
pub struct Assignment {
    var: String,
    val: BongoDataType,
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum Order {
    Asc(String),
    Desc(String),
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum SelectItem {
    ColumnName(String),
    Wildcard,
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
    type Error = ();

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
                // other operators are not supported yet
                Err(())
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
    Value(BongoDataType),
}


impl TryFrom<SqlParserExpr> for Expr {
    type Error = ();

    fn try_from(expr: SqlParserExpr) -> Result<Self, Self::Error> {
        return match expr {
            SqlParserExpr::Identifier(ident) => { Ok(Expr::Identifier(ident.value)) }
            SqlParserExpr::Value(value) => {
                return match value {
                    Value::Number(lit, ..) => {
                        match str::parse::<i64>(&lit) {
                            Ok(val) => { Ok(Expr::Value(BongoDataType::Int(val))) }
                            Err(_) => { Err(()) }
                        }
                    }
                    Value::SingleQuotedString(lit) | Value::DoubleQuotedString(lit) =>
                        { Ok(Expr::Value(BongoDataType::Varchar(String::from(&lit), lit.len()))) }
                    Value::Boolean(val) => { Ok(Expr::Value(BongoDataType::Bool(val))) }
                    Value::Null => { Ok(Expr::Value(BongoDataType::Null)) }
                    _ => { Err(()) }
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
            _ => { Err(()) }
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
    CreateDB { table: String },
    CreateTable {
        table: String,
        cols: Vec<Column>,
    },
    DropTable { table: String },
    DropDB { database: String },
}