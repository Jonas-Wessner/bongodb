use crate::traits::{CreateDropTableQuery, FromRow, InsertQuery, SelectPrimaryQuery, SelectQuery};
use crate::types::BongoError;

use bongo_core::bongo_request::BongoRequest;
use bongo_core::bongo_result::{BongoResult, TryFromJson};

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::str;

pub type ExecuteResult = Result<(), BongoError>;
pub type QueryResult<T> = Result<Vec<T>, BongoError>;
pub type QueryResultSingle<T> = Result<T, BongoError>;

/// The connection struct is the main interface to a `BongoDB server`.
/// It provides a set of functions that allow exectution of SQL statements on the server.
pub struct Connection {
    connection: TcpStream,
}

impl Connection {
    /// The connect function tries to connect to the given url and returns a Result.
    /// If the connection was successfully established, then a `Connection` will be returned.
    /// Otherwise it returns a `BongoError`
    pub fn connect(url: &str) -> Result<Connection, BongoError> {
        let connection =
            TcpStream::connect(&url).map_err(|e| BongoError::InternalError(e.to_string()))?;

        Ok(Self { connection })
    }

    /// The disconnect function tries to disconnect from the server and returns a Result.
    /// If the disconnection was successfull, nothing will be returned.
    /// Otherwise it returns a `BongoError`
    pub fn disconnect(&mut self) -> Result<(), BongoError> {
        self.connection
            .shutdown(Shutdown::Both)
            .map_err(|e| BongoError::InternalError(e.to_string()))
    }

    /// The execute function can exectue a sql statement on a server.
    /// Returns a result containing either nothing when successfull or a BongoError.
    /// The sql statement may not be a SELECT statement.
    pub fn execute(&mut self, sql: &str) -> ExecuteResult {
        if sql.to_lowercase().starts_with("select") {
            return ExecuteResult::Err(BongoError::UnsupportedFeatureError(
                "You must not use a select statement in the execute function".to_string(),
            ));
        }
        self.query_raw(sql)?;
        Ok(())
    }

    /// The query function can exectue a sql select statement on a server.
    /// Returns a result containing either the rows matching the sql statement when successfull or a BongoError.
    /// The sql statement may only be a SELECT statement.
    pub fn query<T>(&mut self, select_sql: &str) -> QueryResult<T>
    where
        T: FromRow<T>,
    {
        if !select_sql.to_lowercase().starts_with("select") {
            return QueryResult::Err(BongoError::UnsupportedFeatureError(
                "You may only use a select statement in the query function".to_string(),
            ));
        }

        QueryResult::Ok(
            self.query_raw(select_sql)?
                .ok_or_else(|| {
                    BongoError::InternalError(
                        "The database did not provide data to select".to_string(),
                    )
                })?
                .into_iter()
                .map(|r| T::from_row(r))
                .collect::<Result<Vec<T>, BongoError>>()?,
        )
    }

    fn query_raw(&mut self, sql: &str) -> BongoResult {
        let request = serde_json::to_string(&BongoRequest::new(sql))
            .map_err(|e| BongoError::InternalError(e.to_string()))?;
        let message_bytes = request.as_bytes();
        let length: u32 = message_bytes.len() as u32;

        let length_as_bytes = &length.to_be_bytes();
        let message_bytes = [length_as_bytes, message_bytes].concat();

        self.connection
            .write_all(&message_bytes)
            .map_err(|e| BongoError::InternalError(e.to_string()))?;

        let mut buf: [u8; 1024] = [0; 1024];
        let mut total_bytes_read = self
            .connection
            .read(&mut buf)
            .map_err(|e| BongoError::InternalError(e.to_string()))?;

        let response_length = u32::from_be_bytes(buf[..4].try_into().map_err(|_| {
            BongoError::InternalError("Failed to parse 4 byte header.".to_string())
        })?);

        let mut response: Vec<u8> = Vec::from(&buf[4..total_bytes_read]);

        while (total_bytes_read as u32) < response_length {
            let bytes_read = self.connection.read(&mut buf).map_err(|_| {
                BongoError::InternalError("Unable to read from database server.".to_string())
            })?;

            total_bytes_read += bytes_read;
            response.append(&mut Vec::from(&buf[..bytes_read]));
        }

        let json_string =
            String::from_utf8(response).map_err(|e| BongoError::InternalError(e.to_string()))?;

        BongoResult::try_from_json(&json_string)?
    }

    /// The select_primary function can return a struct that implements the `SelectPrimary` trait, based on its primary key.
    /// Returns a result containing either the struct or a BongoError.
    pub fn select_primary<T, U>(&mut self, primary: U) -> QueryResultSingle<T>
    where
        T: SelectPrimaryQuery<U>,
        T: FromRow<T>,
        T: Clone,
    {
        let res = self.query::<T>(T::select_primary_query(primary).as_str());
        match res {
            Ok(res) => {
                if res.is_empty() {
                    Err(BongoError::SqlRuntimeError(
                        "No entry for that primary key".to_string(),
                    ))
                } else {
                    Ok(res[0].to_owned())
                }
            }
            Err(e) => Err(e),
        }
    }

    /// The select_all function can return a vec of structs that implements the `Select` trait.
    /// Returns a result containing either a list of structs or a BongoError.
    pub fn select_all<T>(&mut self) -> QueryResult<T>
    where
        T: SelectQuery,
        T: FromRow<T>,
    {
        self.query::<T>(T::select_all_query().as_str())
    }

    /// The select_where function can return a vec of structs which fulfill the given where clause.
    /// The struct has to implement the `Select` trait.
    /// Returns a result containing either a list of structs or a BongoError.
    pub fn select_where<T>(&mut self, where_clause: &str) -> QueryResult<T>
    where
        T: SelectQuery,
        T: FromRow<T>,
    {
        self.query::<T>(T::select_where_query(where_clause).as_str())
    }

    /// The create_table function can create a table on the database, based on the members of a struct.
    /// The struct has to implement the `CreateDropTable` trait.
    /// Returns a result containing either nothing when successfull or a BongoError.
    pub fn create_table<T>(&mut self) -> ExecuteResult
    where
        T: CreateDropTableQuery,
    {
        self.execute(T::create_table_query().as_str())
    }

    /// The drop_table function can drop a table on the database, based on a struct.
    /// The struct has to implement the `CreateDropTable` trait.
    /// Returns a result containing either nothing when successfull or a BongoError.
    pub fn drop_table<T>(&mut self) -> ExecuteResult
    where
        T: CreateDropTableQuery,
    {
        self.execute(T::drop_table_query().as_str())
    }

    /// The insert function can a struct or a slice of structs on the database.
    /// The struct has to implement the `Insert` trait.
    /// Returns a result containing either nothing when successfull or a BongoError.
    pub fn insert<T>(&mut self, insert: T) -> ExecuteResult
    where
        T: InsertQuery,
    {
        self.execute(
            format!(
                "{} {};",
                T::insert_query_head(),
                insert.insert_query_values()
            )
            .as_str(),
        )
    }
}
