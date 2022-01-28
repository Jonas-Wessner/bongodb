pub use bongo_core::types::Row;

pub trait SelectPrimaryQuery<T> {
    fn select_primary_query(primary: T) -> String;
}

pub trait SelectQuery {
    fn select_all_query() -> String;
    fn select_where_query(where_clause: &str) -> String;
}

pub trait CreateDropTableQuery {
    fn create_table_query() -> String;
    fn drop_table_query() -> String;
}

pub trait InsertQuery {
    fn insert_query_head() -> String;
    fn insert_query_values(&self) -> String;
}

pub trait FromRow {
    fn from_row(row: Row) -> Self;
}

impl FromRow for Row {
    fn from_row(row: Row) -> Self {
        row
    }
}
