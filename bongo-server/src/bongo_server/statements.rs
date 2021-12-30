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