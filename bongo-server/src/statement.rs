use sqlparser::ast::{Expr};
use crate::types::{BongoDataType, Column, Row};

#[derive(Debug)]
#[derive(PartialEq)]
pub struct AssignmentExpr {
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
        assignments: Vec<AssignmentExpr>,
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