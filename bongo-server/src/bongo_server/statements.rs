use sqlparser::ast::{Expr};
use crate::bongo_server::types::{BongoDataType, Column, Row};

pub struct AssignmentExpr {
    var: String,
    val: BongoDataType,
}

pub enum Ordering {
    Asc(String),
    Desc(String),
}

///
/// `Statement` is the type of statement that the `SqlParser` of `BongoServer` uses.
/// This is a simplified view of the statement and does not support all sql features.
///
pub enum Statement {
    Select {
        cols: Vec<String>,
        table: String,
        condition: Option<Expr>,
        ordering: Ordering,
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