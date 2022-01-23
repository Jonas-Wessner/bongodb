use bongo_core::types::{BongoError, BongoLiteral, ColumnDef, Row};
use sqlparser::ast::{
    Assignment as SqlParserAssignment, BinaryOperator as SqlParserBinOp, BinaryOperator,
    Expr as SqlParserExpr, OrderByExpr, SelectItem as SqlParserSelectItem, Value,
};
use std::convert::TryFrom;
use std::mem;

// TODO: docs
pub trait ApplyAssignments {
    fn apply_assignments(self, assignments: &[Assignment], cols: &[&str])
                         -> Result<Self, BongoError> where Self: Sized;
}

impl ApplyAssignments for Row {
    fn apply_assignments(mut self, assignments: &[Assignment], cols: &[&str]) -> Result<Self, BongoError> {
        if self.len() != cols.len() {
            return Err(BongoError::InternalError("Cannot assign to row because column definition has a different size than row.".to_string()));
        }
        for a in assignments {
            let index = cols.iter().position(|c| c == &a.col_name.as_str());
            if index.is_none() {
                return Err(BongoError::SqlRuntimeError(format!(
                    "Cannot apply assignment, because column '{}' does not exist",
                    a.col_name
                )));
            }

            self[index.unwrap()] = a.val.clone();
        }

        Ok(self)
    }
}

///
/// `Assignment` is a structure that represents an assignment of a value to a column.
/// So far assignments are only supported and used in SQL UPDATE statements.
///
#[derive(Debug, PartialEq)]
pub struct Assignment {
    pub col_name: String,
    pub val: BongoLiteral,
}

///
/// Implementers of this trait allow to extract column names from themselves.
/// NOTE: unfortunately, this trait must be defined in types and in server, because only traits in the same
/// crate can be implemented for arbitrary types (in this case for slices)
///
pub trait GetColNamesExt<'a> {
    ///
    /// Returns the names of all contained columns.
    ///
    fn get_col_names(&'a self) -> Vec<&'a str>;
}

impl<'a, T: AsRef<[Assignment]>> GetColNamesExt<'a> for T {
    fn get_col_names(&'a self) -> Vec<&'a str> {
        self.as_ref().iter()
            .map(|a| -> &str { a.col_name.as_str() })
            .collect()
    }
}

///
/// Tries to convert an `Assignment` of the used `sqlparser`-library into an object of the custom
/// `Assignment` type paying attention to what features are supported by BongoDB.
///
impl TryFrom<SqlParserAssignment> for Assignment {
    type Error = BongoError;

    fn try_from(parser_assignment: SqlParserAssignment) -> Result<Self, Self::Error> {
        if parser_assignment.id.len() != 1 {
            return Err(BongoError::UnsupportedFeatureError(
                "Only single identifiers are supported \
            in assignments by BongoDB."
                    .to_string(),
            ));
        }

        let expr = Expr::try_from(parser_assignment.value)?;

        match expr {
            Expr::Value(val) => Ok(Self {
                col_name: String::from(&parser_assignment.id[0].value),
                val,
            }),
            _ => Err(BongoError::UnsupportedFeatureError(
                "Only values are supported in assignments".to_string(),
            )),
        }
    }
}

///
/// `Order` represents the semantics of an SQL ORDER BY clause.
/// However, in SQL ORDER BY clauses can be specified with multiple orders while `BongoDB` so far
/// only supports ordering by exactly one column.
///
#[derive(Debug, PartialEq)]
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
        match order_expr.expr {
            SqlParserExpr::Identifier(ident) => {
                let column = String::from(&ident.value);

                Ok(match order_expr.asc {
                    None => Order::Asc(column),
                    Some(is_asc) => match is_asc {
                        true => Order::Asc(column),
                        false => Order::Desc(column),
                    },
                })
            }
            _ => Err(BongoError::UnsupportedFeatureError(
                "ORDER BY is only supported with exactly \
                one argument which must be a column name."
                    .to_string(),
            )),
        }
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
#[derive(Debug, PartialEq)]
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
        let error = Err(BongoError::UnsupportedFeatureError(
            "Only identifiers and unqualified wildcards \
            are supported as select items by BongoDB."
                .to_string(),
        ));
        match item {
            SqlParserSelectItem::UnnamedExpr(SqlParserExpr::Identifier(ident)) =>
                { Ok(SelectItem::ColumnName(String::from(&ident.value))) }

            SqlParserSelectItem::Wildcard => Ok(SelectItem::Wildcard),
            _ => error
        }
    }
}

///
/// `BinOp` represents a binary operator which can appear inside an expression.
/// `BongoDB` does not support all binary operators that exist in SQL
///
#[derive(Debug, PartialEq)]
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

impl BinOp {
    // TODO: docs
    pub fn apply(&self, left: &BongoLiteral, right: &BongoLiteral) -> Result<BongoLiteral, BongoError> {
        // only equal discriminants can be compared.
        // The exception is Null, which can always be compared as if it was false
        let null = mem::discriminant(&BongoLiteral::Null);

        // create let binding to potentially override to BongoLiteral::Bool(false) if null
        let mut left = left;
        let mut right = right;

        // Null values in Logical operations shall evaluate to false
        if mem::discriminant(left) == null {
            left = &BongoLiteral::Bool(false);
        }
        if mem::discriminant(right) == null {
            right = &BongoLiteral::Bool(false);
        }

        if mem::discriminant(left) != mem::discriminant(right) {
            return Err(BongoError::SqlRuntimeError(
                format!("Cannot compare '{:?}' and '{:?}'. Can only compare instances of the same datatype.",
                        left, right)));
        }

        match self {
            BinOp::Lt => { return Ok(BongoLiteral::Bool(left < right)); }
            BinOp::Gt => { return Ok(BongoLiteral::Bool(left > right)); }
            BinOp::GtEq => { return Ok(BongoLiteral::Bool(left >= right)); }
            BinOp::LtEq => { return Ok(BongoLiteral::Bool(left <= right)); }
            BinOp::Eq => { return Ok(BongoLiteral::Bool(left == right)); }
            BinOp::NotEq => { return Ok(BongoLiteral::Bool(left != right)); }
            _ => {}
        }

        if let BongoLiteral::Bool(val_left) = left {
            // right is implicitly the same as left we tested in the beginning of the method -> no else needed
            if let BongoLiteral::Bool(val_right) = right {
                match self {
                    BinOp::And => { return Ok(BongoLiteral::Bool(*val_left && *val_right)); }
                    BinOp::Or => { return Ok(BongoLiteral::Bool(*val_left || *val_right)); }
                    _ => { /* in these cases we would have already returned */ }
                }
            }
        }

        return Err(BongoError::SqlRuntimeError(
            format!("Cannot compare '{:?}' and '{:?}'. Can only compare instances of the same datatype.",
                    left, right)));
    }
}

///
/// Tries to convert a `BinaryOperator` of the used `sqlparser`-library into an object of the custom
/// `BinOp` type paying attention to what features are supported by BongoDB.
///
impl TryFrom<SqlParserBinOp> for BinOp {
    type Error = BongoError;

    fn try_from(value: SqlParserBinOp) -> Result<Self, Self::Error> {
        match value {
            BinaryOperator::Gt => Ok(BinOp::Gt),
            BinaryOperator::Lt => Ok(BinOp::Lt),
            BinaryOperator::GtEq => Ok(BinOp::GtEq),
            BinaryOperator::LtEq => Ok(BinOp::LtEq),
            BinaryOperator::Eq => Ok(BinOp::Eq),
            BinaryOperator::NotEq => Ok(BinOp::NotEq),
            BinaryOperator::And => Ok(BinOp::And),
            BinaryOperator::Or => Ok(BinOp::Or),
            _ => Err(BongoError::UnsupportedFeatureError(
                "Only the Operators >, <, >=, <=, =, !=, \
                AND, OR are supported by BongoDB"
                    .to_string(),
            )),
        }
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
#[derive(Debug, PartialEq)]
pub enum Expr {
    BinaryExpr {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Identifier(String),
    Value(BongoLiteral),
}

impl Expr {
    // TODO: enhance docs
    ///
    /// Evaluates the expression recursively for a specific row.
    ///
    pub fn eval(&self, row: &Row, cols: &[&str]) -> Result<bool, BongoError> {
        if row.len() != cols.len() {
            return Err(BongoError::InternalError("Column size and row size are different".to_string()));
        }

        self.eval_helper(row, cols)?.as_bool()
    }
    fn eval_helper(&self, row: &Row, cols: &[&str]) -> Result<BongoLiteral, BongoError> {
        match self {
            Expr::BinaryExpr { left, op, right } => {
                let left_val = left.eval_helper(row, cols)?;
                let right_val = right.eval_helper(row, cols)?;

                op.apply(&left_val, &right_val)
            }
            Expr::Identifier(name) => {
                let pos = cols.iter().position(|n| { n == &name });
                if pos.is_none() {
                    return Err(BongoError::SqlRuntimeError(format!("Column '{}' does not exist.", name)));
                }
                // return value in this column
                Ok(row[pos.unwrap()].clone())
            }
            Expr::Value(val) => { Ok(val.clone()) }
        }
    }
}

///
/// Tries to convert an `Expr` of the used `sqlparser`-library into an object of the custom
/// `Expr` type paying attention to what features are supported by BongoDB.
///
impl TryFrom<SqlParserExpr> for Expr {
    type Error = BongoError;

    fn try_from(expr: SqlParserExpr) -> Result<Self, Self::Error> {
        match expr {
            SqlParserExpr::Identifier(ident) => Ok(Expr::Identifier(ident.value)),
            SqlParserExpr::Value(value) => {
                match value {
                    Value::Number(lit, ..) => match str::parse::<i64>(&lit) {
                        Ok(val) => Ok(Expr::Value(BongoLiteral::Int(val))),
                        Err(_) => Err(BongoError::InternalError(
                            "Failed to parse int value.".to_string(),
                        )),
                    },
                    Value::SingleQuotedString(lit) | Value::DoubleQuotedString(lit) => Ok(
                        Expr::Value(BongoLiteral::Varchar(lit.clone())),
                    ),
                    Value::Boolean(val) => Ok(Expr::Value(BongoLiteral::Bool(val))),
                    Value::Null => Ok(Expr::Value(BongoLiteral::Null)),
                    _ => Err(BongoError::UnsupportedFeatureError(
                        "Only integers, single quoted strings \
                    booleans and NULL values are supported as literals by BongoDB."
                            .to_string(),
                    )),
                }
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
            _ => Err(BongoError::UnsupportedFeatureError(
                "Only identifiers, values, and binary \
            operations are supported as expressions by BongoDB."
                    .to_string(),
            )),
        }
    }
}

///
/// `Statement` is the type of statement that the `SqlParser` of `BongoServer` uses.
/// This is a simplified view of the statement and does not support all SQL language features.
///
/// Creating a struct for every contained type allows cleaner passing to functions.
/// If we had anonymous contained structs, we would have to pass the containing enum and in the function
/// match on the enum type although we might already know the variant of the enum.
///
#[derive(Debug, PartialEq)]
pub enum Statement {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    DropTable(DropTable),
    // Forces BongoDB to write all information that is currently kept in the cache (RAM) to disk
    Flush,
    // NOTE: currently not supported, because BongoDB asserts having only exactly one DB
    CreateDB(CreateDB),
    // NOTE: currently not supported, because BongoDB asserts having only exactly one DB
    DropDB(DropDB),
}

#[derive(Debug, PartialEq)]
pub struct Select {
    pub cols: Vec<SelectItem>,
    pub table: String,
    pub condition: Option<Expr>,
    pub order: Option<Order>,
}

#[derive(Debug, PartialEq)]
pub struct Insert {
    pub table: String,
    pub cols: Vec<String>,
    pub rows: Vec<Row>,
}

#[derive(Debug, PartialEq)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub condition: Option<Expr>,
}

#[derive(Debug, PartialEq)]
pub struct Delete {
    pub table: String,
    pub condition: Option<Expr>,
}

#[derive(Debug, PartialEq)]
pub struct CreateTable {
    pub table: String,
    pub cols: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq)]
pub struct DropTable {
    ///
    /// multiple tables could be dropped here
    ///
    pub names: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct CreateDB {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct DropDB {
    pub database: String,
}

// TODO: NULL values should be able to compared to anything, because we never know if a column might be null at some point
#[cfg(test)]
mod tests {
    mod bin_op {
        use bongo_core::types::BongoLiteral;
        use crate::statement::BinOp;

        #[test]
        fn apply_lt() {
            let op = BinOp::Lt;
            assert_eq!(op.apply(&BongoLiteral::Int(3), &BongoLiteral::Int(5)).unwrap(), BongoLiteral::Bool(true));
            assert_eq!(op.apply(&BongoLiteral::Int(5), &BongoLiteral::Int(3)).unwrap(), BongoLiteral::Bool(false));
            // equal
            assert_eq!(op.apply(&BongoLiteral::Int(3), &BongoLiteral::Int(3)).unwrap(), BongoLiteral::Bool(false));

            // alphabetic order
            assert_eq!(op.apply(&BongoLiteral::Varchar("a".to_string()), &BongoLiteral::Varchar("b".to_string())).unwrap(), BongoLiteral::Bool(true));
            assert_eq!(op.apply(&BongoLiteral::Varchar("b".to_string()), &BongoLiteral::Varchar("a".to_string())).unwrap(), BongoLiteral::Bool(false));
            // equal
            assert_eq!(op.apply(&BongoLiteral::Varchar("a".to_string()), &BongoLiteral::Varchar("a".to_string())).unwrap(), BongoLiteral::Bool(false));

            assert_eq!(op.apply(&BongoLiteral::Bool(false), &BongoLiteral::Bool(true)).unwrap(), BongoLiteral::Bool(true));
            assert_eq!(op.apply(&BongoLiteral::Bool(true), &BongoLiteral::Bool(false)).unwrap(), BongoLiteral::Bool(false));
            // equal
            assert_eq!(op.apply(&BongoLiteral::Bool(true), &BongoLiteral::Bool(true)).unwrap(), BongoLiteral::Bool(false));

            // Null shall return false on any comparison unless the other operand is also Null
            assert_eq!(op.apply(&BongoLiteral::Null, &BongoLiteral::Null).unwrap(), BongoLiteral::Bool(false));

            // types that are not equal should not be comparable:
            assert!(op.apply(&BongoLiteral::Int(3), &BongoLiteral::Bool(true)).is_err());
            assert!(op.apply(&BongoLiteral::Int(3), &BongoLiteral::Varchar("oh no!".to_string())).is_err());
        }

        #[test]
        fn apply_and() {
            let op = BinOp::And;
            assert_eq!(op.apply(&BongoLiteral::Bool(false), &BongoLiteral::Bool(false)).unwrap(), BongoLiteral::Bool(false));
            assert_eq!(op.apply(&BongoLiteral::Bool(false), &BongoLiteral::Bool(true)).unwrap(), BongoLiteral::Bool(false));
            assert_eq!(op.apply(&BongoLiteral::Bool(true), &BongoLiteral::Bool(false)).unwrap(), BongoLiteral::Bool(false));
            assert_eq!(op.apply(&BongoLiteral::Bool(true), &BongoLiteral::Bool(true)).unwrap(), BongoLiteral::Bool(true));

            // null should evaluate to false in logical expressions
            assert_eq!(op.apply(&BongoLiteral::Null, &BongoLiteral::Null).unwrap(), BongoLiteral::Bool(false));
            assert_eq!(op.apply(&BongoLiteral::Null, &BongoLiteral::Bool(true)).unwrap(), BongoLiteral::Bool(false));


            // other types should not have logical operators
            assert!(op.apply(&BongoLiteral::Int(1), &BongoLiteral::Int(1)).is_err());
            assert!(op.apply(&BongoLiteral::Varchar("a".to_string()), &BongoLiteral::Varchar("b".to_string())).is_err());
        }
    }

    mod expr {
        use bongo_core::types::BongoLiteral;
        use crate::statement::{BinOp, Expr};

        #[test]
        fn eval_err() {
            assert!(Expr::Value(BongoLiteral::Int(1)).eval(&vec![], &[]).is_err());
            assert!(Expr::Value(BongoLiteral::Varchar("oh no!".to_string())).eval(&vec![], &[]).is_err());
            // literals for row and the definition of columns have different sizes
            assert!(Expr::Value(BongoLiteral::Bool(true)).eval(
                &vec![BongoLiteral::Bool(true), BongoLiteral::Bool(false)],
                &["col_1"]).is_err());
            // identifier is not a column
            assert!(Expr::Identifier("col_1".to_string()).eval(
                &vec![BongoLiteral::Bool(true)],
                &["col_2"]).is_err());
        }

        #[test]
        fn eval_simple_valid_expr() {
            assert!(Expr::Value(BongoLiteral::Bool(true)).eval(&vec![], &[]).unwrap());
            assert!(!Expr::Value(BongoLiteral::Bool(false)).eval(&vec![], &[]).unwrap());
            // with some columns given, but those are ignored, because expression does not contains identifier
            assert!(Expr::Value(BongoLiteral::Bool(true)).eval(
                &vec![BongoLiteral::Bool(true), BongoLiteral::Bool(false)],
                &["col_1", "col_2"]).unwrap());
            // identifier that evaluates to true
            assert!(Expr::Identifier("col_1".to_string()).eval(
                &vec![BongoLiteral::Bool(true)],
                &["col_1"]).unwrap());
        }

        #[test]
        fn eval_complex_valid_expr() {
            let expr = Expr::BinaryExpr {
                left: Box::new(Expr::BinaryExpr {
                    left: Box::new(Expr::Identifier(String::from("a"))),
                    op: BinOp::NotEq,
                    right: Box::new(Expr::Identifier(String::from("b"))),
                }),
                op: BinOp::Or,
                right: Box::new(Expr::BinaryExpr {
                    left: Box::new(Expr::Identifier(String::from("c"))),
                    op: BinOp::Eq,
                    right: Box::new(Expr::Value(BongoLiteral::Bool(false))),
                }),
            };

            let cols = &["a", "b", "c", "d"];

            let row_true_1 = &vec![
                BongoLiteral::Varchar("❤".to_string()),
                BongoLiteral::Varchar("😍".to_string()),
                BongoLiteral::Bool(true),
                BongoLiteral::Null, // last column should never be evaluated
            ];

            let row_true_2 = &vec![
                BongoLiteral::Varchar("❤".to_string()),
                BongoLiteral::Varchar("❤".to_string()),
                BongoLiteral::Bool(false),
                BongoLiteral::Null, // last column should never be evaluated
            ];

            let row_false = &vec![
                BongoLiteral::Varchar("❤".to_string()),
                BongoLiteral::Varchar("❤".to_string()),
                BongoLiteral::Bool(true),
                BongoLiteral::Null, // last column should never be evaluated
            ];

            assert!(expr.eval(row_true_1, cols).unwrap());
            assert!(expr.eval(row_true_2, cols).unwrap());
            assert!(!expr.eval(row_false, cols).unwrap());
        }
    }
}