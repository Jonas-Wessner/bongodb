use sqlparser::ast::{Expr};
use crate::types::{BongoDataType, Column, Row};

#[derive(Debug)]
pub struct AssignmentExpr {
    var: String,
    val: BongoDataType,
}

#[derive(Debug)]
pub enum Order {
    Asc(String),
    Desc(String),
}

#[derive(Debug)]
pub enum SelectItem {
    ColumnName(String),
    Wildcard,
}

///
/// `Statement` is the type of statement that the `SqlParser` of `BongoServer` uses.
/// This is a simplified view of the statement and does not support all sql features.
///
#[derive(Debug)]
pub enum Statement {
    Select {
        cols: Vec<SelectItem>,
        table: String,
        condition: Option<Expr>,
        order: Order,
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