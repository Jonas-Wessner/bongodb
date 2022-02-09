use bongo_core::types::BongoError;
use bongo_core::types::Row;

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

impl<T> InsertQuery for &[T]
where
    T: InsertQuery,
{
    fn insert_query_head() -> String {
        T::insert_query_head()
    }

    fn insert_query_values(&self) -> String {
        let mut query = String::new();

        for type_to_insert in self.iter() {
            query.push_str(format!("{}, ", type_to_insert.insert_query_values()).as_str());
        }

        query[..query.len() - 2].to_string()
    }
}

pub trait FromRow<T> {
    fn from_row(row: Row) -> Result<T, BongoError>;
}

impl FromRow<Row> for Row {
    fn from_row(row: Row) -> Result<Row, BongoError> {
        Ok(row)
    }
}
