use std::convert::TryFrom;
use sqlparser::ast::{Expr as SqlParserExpr, Value, BinaryOperator as SqlParserBinOp, BinaryOperator, Assignment as SqlParserAssignment, SelectItem as SqlParserSelectItem, OrderByExpr};
use bongo_core::types::{BongoError, BongoLiteral, ColumnDef, Row};

///
/// `Assignment` is a structure that represents an assignment of a value to a column.
/// So far assignments are only supported and used in SQL UPDATE statements.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub struct Assignment {
    pub col_name: String,
    pub val: BongoLiteral,
}

///
/// Tries to convert an `Assignment` of the used `sqlparser`-library into an object of the custom
/// `Assignment` type paying attention to what features are supported by BongoDB.
///
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
                        col_name: String::from(&parser_assignment.id[0].value),
                        val,
                    })
            }
            _ => { Err(BongoError::UnsupportedFeatureError("Only values are supported in assignments".to_string())) }
        };
    }
}

///
/// `Order` represents the semantics of an SQL ORDER BY clause.
/// However, in SQL ORDER BY clauses can be specified with multiple orders while `BongoDB` so far
/// only supports ordering by exactly one column.
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum Order {
    Asc(String),
    Desc(String),
}

///
/// Tries to convert an `OrderByExpr` of the used `sqlparser`-library into an object of the custom
/// `Order` type paying attention to what features are supported by BongoDB.
///
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

///
/// `SelectItem` represents an item in a projection in SQL.
///
/// Although SQL is more powerful, `BongoDB` so far only supports column names and unqualified
/// wildcard (asterisks *) as `SelectItem`s.
///
/// # Examples
/// In the statement `SELECT col_1, col_2 FROM table_1`
/// `col_1` and `col_2` are select items.
///
///
#[derive(Debug)]
#[derive(PartialEq)]
pub enum SelectItem {
    ColumnName(String),
    Wildcard,
}

///
/// Tries to convert a `SelectItem` of the used `sqlparser`-library into an object of the custom
/// `SelectItem` type paying attention to what features are supported by BongoDB.
///
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

///
/// `BinOp` represents a binary operator which can appear inside an expression.
/// `BongoDB` does not support all binary operators that exist in SQL
///
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

///
/// Tries to convert a `BinaryOperator` of the used `sqlparser`-library into an object of the custom
/// `BinOp` type paying attention to what features are supported by BongoDB.
///
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

///
/// `Expr` represents an expression in SQL.
///
/// `Expr` is recursively defined, such that a binary expression contains operand which each are
/// expressions. The recursion stops at either the `Identifier(String)` variant or the
/// `Value(BongoLiteral)` variant. Using a recursive definition allows to evaluating the expression
/// in a natural way starting from its root.
///
/// # Examples
///
/// In the statement `SELECT * FROM table_1 WHERE (a < b) AND (c = 5)`
/// `(a < b) AND (c = 5)` is the variant `BinaryExpr` where each operand each is a `BinaryExpr`
/// variant. The operands of these Expressions then are `Identifier`s ore `Value`s
///
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

///
/// Tries to convert an `Expr` of the used `sqlparser`-library into an object of the custom
/// `Expr` type paying attention to what features are supported by BongoDB.
///
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
/// This is a simplified view of the statement and does not support all SQL language features.
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