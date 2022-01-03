use crate::types::{BongoLiteral, ColumnDef, Row};
use std::convert::TryFrom;
use sqlparser::ast::{Expr as SqlParserExpr, Value, BinaryOperator as SqlParserBinOp, BinaryOperator, Assignment as SqlParserAssignment};
use std::num::ParseIntError;
use crate::statement::Expr::BinaryExpr;

#[derive(Debug)]
#[derive(PartialEq)]
pub struct Assignment {
    pub varname: String,
    pub val: BongoLiteral,
}

impl TryFrom<SqlParserAssignment> for Assignment {
    type Error = ();

    fn try_from(parser_assignment: SqlParserAssignment) -> Result<Self, Self::Error> {
        if parser_assignment.id.len() != 1 {
            // only single identifiers supported
            return Err(());
        }

        let expr = Expr::try_from(parser_assignment.value)?;

        return match expr {
            // only values are supported in assignments
            Expr::Value(val) => {
                Ok(
                    Self {
                        varname: String::from(&parser_assignment.id[0].value),
                        val,
                    })
            }
            _ => { Err(()) }
        };
    }
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
    Value(BongoLiteral),
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
                            Ok(val) => { Ok(Expr::Value(BongoLiteral::Int(val))) }
                            Err(_) => { Err(()) }
                        }
                    }
                    Value::SingleQuotedString(lit) | Value::DoubleQuotedString(lit) =>
                        { Ok(Expr::Value(BongoLiteral::Varchar(String::from(&lit), lit.len()))) }
                    Value::Boolean(val) => { Ok(Expr::Value(BongoLiteral::Bool(val))) }
                    Value::Null => { Ok(Expr::Value(BongoLiteral::Null)) }
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