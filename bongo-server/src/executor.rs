use std::cell::RefCell;
use std::cmp::Ordering::Equal;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use bongo_core::bongo_request::BongoRequest;
use bongo_core::bongo_result::BongoResult;
use bongo_core::bytes_on_disc::{AsDiscBytes, FromDiscBytes};
use bongo_core::conversions::TryConvertAllExt;
use bongo_core::types::{BongoError, BongoLiteral, ColumnDef, GetColNamesExt as GetColNamesExtCore, GetDTypesExt, Row};
use bongo_core::types::BongoError::{InternalError, ReadFileError};
use serde::{Deserialize, Serialize};

use crate::sql_parser::err_messages::generic_write_error;
use crate::sql_parser::parser::SqlParser;
use crate::statement::{ApplyAssignments, BinOp, CreateTable, Delete, DropTable, Expr, GetColNamesExt as GetColNamesExtServer, Insert, Order, Select, SelectItem, Statement, Update};

///
/// `TableMetaData` stores all information about a table in the current state except the actual user data
/// i.e. the rows of the tables. The user data is stored in a file on disk.
///
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct TableMetaData {
    ///
    /// `cols` is the definition of columns in the table given by the user in the CREATE TABLE statement.
    ///
    pub cols: Vec<ColumnDef>,
    ///
    /// `idx` is a tuple of `String` and `HashMap<BongoLiteral, Vec<u64>>`.
    /// The `idx.0` specifies the name of the indexed column.
    /// the `idx.1` maps a BongoLiteral, which is the content of an entry for a column, to indices of
    /// the start of rows on disc on disc. That means if we read exactly as many bytes as a row in
    /// that table is long at one of those indices, we will get back exactly one row.
    /// If the the Vec has length > 1 it means that there was a hash collision which requires a linear search.
    /// The Vec always has length >= 1 because the HashMap entry will be removed if it has length == 0.
    ///
    // TODO: LOW_PRIO: allow multiple indexes and set them with CREATE INDEX statement
    pub idx: (String, HashMap<BongoLiteral, Vec<u64>>),
    ///
    /// `ghosts` is a list of row indices marked as unused. Unused rows result from DELETE operations.
    ///
    pub ghosts: Vec<u64>,
    ///
    /// The size in bytes which one row occupies.
    ///
    pub row_size: usize,
    ///
    /// The amount of rows in the table
    ///
    pub row_count: usize,
}

impl TableMetaData {
    ///
    /// returns whether a specified `Row` can be stored in this table.
    /// To be able to store the `Row` the datatypes and order must match.
    ///
    pub fn can_store(&self, row: &Row) -> bool {
        if self.cols.len() != row.len() {
            return false;
        }
        for (i, item) in row.iter().enumerate() {
            if !self.cols[i].data_type.can_store(item) {
                return false;
            }
        }

        true
    }
}

///
/// `Table` is used to store the name of a table together with its meta data.
/// This structure is used to easily be inserted into a HashMap
///
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Table((String, TableMetaData));

///
/// An `Executor` can execute a `BongoRequest`
///
pub struct Executor {
    ///
    /// `db_dir` is the root directory of the database storage.
    ///
    db_root: PathBuf,
    ///
    /// `tables` maps table names to their meta data.
    ///
    /// We use a 2-level-RwLock-system allowing for the maximum possible degree of parallelism while
    /// still keeping thread-safety.
    ///
    /// 1st level RwLock (outer lock):
    /// - A read lock on this means that we have immutable access to the HashMap which means we
    ///     cannot remove or add any tables. However, we still have mutable access to the tables themselves
    ///     as those are wrapped inside a RefCell.
    ///     The execution of the following statements requires acquiring a read lock on the first level:
    ///         SELECT, INSERT, UPDATE, DELETE
    /// - A write lock on this means that we have mutable access on the entire HashMap allowing us to
    ///     add and remove tables. This also means we guarantee that no other statement is executed at
    ///     this time.
    ///     The execution of the following statements requires acquiring a write lock on the first level:
    ///         CREATE TABLE, DROP TABLE, FLUSH
    ///
    /// 2nd level RwLock (inner lock):
    /// - A read lock on this means that we have immutable access to the tables which means we cannot
    ///     modify anything at all.
    ///     The execution of the following statements requires acquiring a read lock on the second level:
    ///         SELECT
    /// - A write lock on this means that we have mutable access to exactly ONE table and that no other thread
    ///     currently has access to this table in any way.
    ///     The execution of the following statements requires acquiring a write lock on the second level:
    ///         INSERT, UPDATE, DELETE
    ///
    /// Lets compare this to using a single RwLock system:
    /// Single-level RwLock:
    /// - Enables multiple parallel SELECTs OR
    ///     - block any other statement except SELECT no matter the table
    /// - exactly one  CREATE TABLE, DROP TABLE, FLUSH, INSERT, UPDATE, DELETE
    ///     - block any other statement
    ///
    /// 2-level RwLock (used here):
    /// - Enables multiple parallel SELECTs
    ///     - blocks only INSERTs, UPDATEs, DELETEs on this specific and CREATE TABLE, DROP TABLE, FLUSH
    /// - Enables multiple parallel INSERTs, UPDATEs, DELETEs as long as they are on different tables
    ///     - blocks only any other statement on this specific table
    /// - exclusive access required for CREATE TABLE, DROP TABLE, FLUSH
    ///     - blocks any other statements
    ///
    /// So over all we get more parallelism while keeping thread safety, becasue we only lock what really
    /// has to be locked.
    tables: RwLock<HashMap<String, RefCell<RwLock<TableMetaData>>>>,
    ///
    /// `auto_flush` == true means that a flush shall be triggered after every other command except flush itself.
    ///
    auto_flush: bool,
}

// Executor internally ensures by its logic and by using RwLock that it is safe to use from different threads.
unsafe impl Send for Executor {}

// Executor internally ensures by its logic and by using RwLock that it is safe to use from different threads.
unsafe impl Sync for Executor {}

impl Drop for Executor {
    fn drop(&mut self) {
        // there is nothing we can do for the user if this final flush does not work but panic.
        self.flush().unwrap();
    }
}

impl Executor {
    pub fn new<P>(db_root: &P, create_db: bool, auto_flush: bool) -> Result<Self, BongoError>
        where P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr> + ?Sized {
        let path_buf = Self::get_db_root_dir(db_root, create_db)?;
        Ok(Self {
            tables: RwLock::new(Self::load_tables_from_disc(&path_buf)?),
            db_root: path_buf,
            auto_flush,
        })
    }

    fn get_db_root_dir<P>(root_dir: &P, create_db: bool) -> Result<PathBuf, BongoError>
        where P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr> + ?Sized {
        let path = AsRef::<Path>::as_ref(root_dir);
        if !path.exists() {
            if create_db {
                if fs::create_dir_all(path).is_err() {
                    return Err(BongoError::WriteFileError("could not create root directory for DB".to_string()));
                }
            } else {
                // unwrap is safe, because BongoServer makes sure only utf8 directories are passed in
                return Err(BongoError::DatabaseNotFoundError(format!(
                    "location: '{}'",
                    location = path.to_str().unwrap()
                )));
            }
        }

        // safe because canonicalize will be valid if root_dir exists is valid, which we assure
        Ok(PathBuf::from(path).canonicalize().unwrap())
    }

    fn load_tables_from_disc(root_dir: &Path) -> Result<HashMap<String, RefCell<RwLock<TableMetaData>>>, BongoError> {
        //
        // 1. if root_dir it does not exists and create_db is true create the directory
        // 2. load files from disc for each directory entry into a HashMap
        //
        match root_dir.read_dir() {
            Ok(entries) => {
                Ok(entries
                    .map(|entry_res| {
                        match entry_res {
                            Ok(entry) => Self::load_table_from_disc(entry.path()),
                            Err(_) => Err(BongoError::ReadFileError(
                                "Cannot read file inside DB root".to_string(),
                            )),
                        }
                    })
                    .collect::<Vec<Result<(String, RefCell<RwLock<TableMetaData>>), BongoError>>>()
                    // fail fast unwrap of all contained elements and in error case bubble up error
                    .try_convert_all(|t| t)?
                    // back to iterator in order to convert vec of tuples to map
                    .into_iter()
                    .collect())
            }
            Err(_) => Err(BongoError::ReadFileError(
                "Cannot read files inside DB root".to_string(),
            )),
        }
    }

    ///
    /// Loads a  key value pair of table name and `TableMetaData` from a file in a given directory.
    ///
    fn load_table_from_disc(mut table_dir: PathBuf) -> Result<(String, RefCell<RwLock<TableMetaData>>), BongoError> {
        //
        // 1. read meta.bongo file content to end
        // 2. deserialize content into a tuple `String, RwLock<TableMetaData)`
        //
        let mut data = Vec::new();
        table_dir.push("meta.bongo");

        match File::open(&table_dir) {
            Ok(mut file) => match file.read_to_end(&mut data) {
                Ok(_) => match bincode::deserialize(&data[..]) {
                    Ok(table) => Ok(table),
                    Err(_) => Err(BongoError::InternalError(format!(
                        "Meta table file corrupted and cannot be deserialized at '{}'",
                        table_dir.to_str().unwrap()
                    ))),
                },
                Err(_) => Err(BongoError::ReadFileError(format!(
                    "Could not read meta table information from file at '{}'",
                    table_dir.to_str().unwrap()
                ))),
            },
            Err(_) => Err(BongoError::ReadFileError(format!(
                "Could not open meta table file for reading at '{}'",
                table_dir.to_str().unwrap()
            ))),
        }
    }

    ///
    /// Executes a `BongoRequest` by first parsing it and then executing its contents.
    /// returns a `BongoResult` representing the result of execution.
    ///
    pub fn execute(&mut self, request: &BongoRequest) -> BongoResult {
        let statement = SqlParser::parse(&request.sql)?;

        println!(
            "sql has been parsed with the following resulting statement:\n{:?}",
            statement
        );

        let must_flush = self.auto_flush && !matches!(&statement, Statement::Select(_) | Statement::Flush);

        let result = match statement {
            Statement::Select(select) => self.select(select),
            Statement::Insert(insert) => self.insert(insert),
            Statement::Update(update) => self.update(update),
            Statement::Delete(delete) => self.delete(delete),
            Statement::CreateTable(create_table) => self.create_table(create_table),
            Statement::DropTable(drop_table) => self.drop_table(drop_table),
            Statement::Flush => self.flush(),
            Statement::CreateDB { .. } | Statement::DropDB { .. } => Self::create_drop_db(),
        };

        if must_flush { self.flush()?; }

        result
    }

    ///
    /// A `Flush` statement is executed as follows.
    ///
    /// for each table:
    ///  1. serialize table
    ///  2. check that directory for table exists
    ///  3. check if meta data file already exists and delete if so (effectively overwrite it)
    ///  4. (re-)create meta data file
    ///  5. write table to file
    ///
    fn flush(&mut self) -> BongoResult {
        // this function requires write access on the whole hash map which means there can not be
        // any other concurrent statement running. This is because this function will modify the disc
        // directly which must be synchronized.

        let db_root = self.db_root.clone(); // access here before mut ref access to self is required

        for table in self.tables_write_access()?.iter() {
            let encoded = bincode::serialize(&table);
            if encoded.is_err() { return Err(BongoError::InternalError("Could not write cashed state to disc.".to_string())); }
            let encoded = encoded.unwrap();

            let mut location = db_root.clone();
            location.push(table.0);

            if !location.is_dir() {
                return Err(BongoError::InternalError("Table to flush has no directory on disc".to_string()));
            }

            location.push("meta.bongo");

            if location.exists() {
                if location.is_file() {
                    // File exists from last flush
                    if fs::remove_file(&location).is_err() {
                        return Err(BongoError::WriteFileError("Could not delete outdated meta data file on disc.".to_string()));
                    }
                } else {
                    return Err(BongoError::InternalError("Directory of table to flush contains invalid files.".to_string()));
                }
            }

            let file = File::create(&location);
            if file.is_err() { return Err(BongoError::WriteFileError("Could not create meta table file on disc.".to_string())); }
            let mut file = file.unwrap();

            if file.write_all(&encoded).is_err() {
                return Err(BongoError::WriteFileError("Could not save current of meta data to file".to_string()));
            }
        }

        Ok(None)
    }

    ///
    /// A `Select` statement is executed as follows.
    ///
    /// 1. check if table exists in cache
    /// 2. check if table exists on disc
    /// 3. check if all columns exist in table
    /// 4. get all indices that must be loaded from disc as a `DiscIdx`.
    /// 5. Load rows from disc and if applicably check if condition is true for them.
    /// 6. remove the not selected rows.
    /// 7. check if order is given, if so sort accordingly.
    /// 8. return result.
    ///
    fn select(&self, select: Select) -> BongoResult {
        let mut path = self.get_table_dir_if_exists(&select.table)?;
        let tables = self.tables_read_access()?;
        // unwrap safe, because we have checked the entry exists before
        let cell = tables.get(&select.table).unwrap();
        let table_lock = cell.borrow();
        let table = table_lock.read();

        if table.is_err() {
            return Err(BongoError::InternalError("Could not acquire read access to cached meta data.".to_string()));
        }
        let table = table.unwrap();

        // get indices of the selected columns in the structure of the table
        let mut selected_col_indices = vec![];
        for item in select.cols {
            match item {
                SelectItem::ColumnName(name) => {
                    let loc = table.cols.iter().position(|col_def| { col_def.name == name });
                    if loc.is_none() {
                        return Err(BongoError::SqlRuntimeError(format!("The column '{name}' does not exist.")));
                    }
                    selected_col_indices.push(loc.unwrap());
                }
                SelectItem::Wildcard => {
                    // in this case the for loop will only run once. This is ensured by the value we get from the parser
                    selected_col_indices = (0..table.cols.len()).into_iter().collect();
                }
            }
        }

        let selected_d_types = table.cols.get_d_types();

        path.push("data.bongo");
        let file = File::open(&path);
        if file.is_err() {
            return Err(BongoError::ReadFileError("Could not get read access to file on disc".to_string()));
        }
        let mut file = file.unwrap();

        let mut row_buffer = Vec::with_capacity(table.row_size);
        unsafe { row_buffer.set_len(table.row_size) } // expand buffer to avoid useless initialization

        let indexer = DiscIndexer::from_opt_expr(&table.idx, select.condition);
        let mut rows = match &indexer.expr {
            None => {
                // when all indices are used, better already allocate
                Vec::with_capacity(indexer.indices.len() * table.row_size)
            }
            Some(_) => {
                // if there is an expression we do not know yet how much to allocate
                vec![]
            }
        };

        for i in indexer.indices {
            if file.seek(SeekFrom::Start(i)).is_err() {
                return Err(ReadFileError("Could not jump to correct position in file".to_string()));
            }
            if file.read_exact(&mut row_buffer).is_err() {
                return Err(BongoError::ReadFileError("Could not read row from disc".to_string()));
            }
            let row = Row::from_disc_bytes(&row_buffer, &selected_d_types)?;

            // if no condition exists or the existing condition evaluates to true this row shall be returned
            if indexer.expr.is_none() ||
                indexer.expr.as_ref().unwrap().eval(&row, &table.cols.get_col_names())? {
                rows.push(row);
            }
        }

        // apply order before removing unselected indices, because we allow ordering by non-selected columns
        if let Some(order) = select.order {
            let col_idx = match &order {
                Order::Asc(col) | Order::Desc(col) => {
                    let idx = table.cols.iter().position(|c| { &c.name == col });
                    if idx.is_none() {
                        return Err(BongoError::SqlRuntimeError(format!("Cannot order, because column '{}' does not exist.",
                                                                       col)));
                    }
                    idx.unwrap()
                }
            };

            let mut asc = true;

            if let Order::Desc(_) = order {
                asc = false;
            }

            rows.sort_by(|r1, r2| {
                if asc {
                    r1[col_idx].partial_cmp(&r2[col_idx]).unwrap_or(Equal)
                } else {
                    r2[col_idx].partial_cmp(&r1[col_idx]).unwrap_or(Equal)
                }
            });
        }

        // remove non-selected indices
        rows = rows.into_iter().map(|row| {
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| { selected_col_indices.contains(i) })
                .map(|(_, item)| { item }) // remove indices again
                .collect()
        }).collect();

        Ok(Some(rows))
    }

    ///
    /// An `Insert` statement is executed as follows:
    ///
    /// 1. check if table exists in cache
    /// 2. check if the table exists on disc
    /// 3. check if specified columns are correct for the specified table
    /// 4. check if specifies Rows have the correct datatypes for each element
    /// 5. write rows to disc
    ///      4.1 if ghosts exists use ghosts first
    ///      4.2 if no ghosts exist write to end of file
    /// 6. update index
    /// 7. update row_count
    ///
    fn insert(&mut self, mut insert: Insert) -> BongoResult {
        let mut location = self.get_table_dir_if_exists(&insert.table)?;

        location.push("data.bongo");

        let tables = self.tables_read_access()?;
        let cell = tables.get(&insert.table).unwrap();
        let table_lock = cell.borrow_mut();
        let table = table_lock.write();

        if table.is_err() {
            return Err(InternalError("Concurrency Error.".to_string()));
        }
        let mut table = table.unwrap();

        // check if all columns are correct
        if table.cols.get_col_names() !=
            insert.cols {
            let col_names = table.cols
                .iter()
                .map(|col_def| { format!("{:?}", col_def) })
                .intersperse_with(|| { " ".to_string() })
                .collect::<String>();

            return Err(BongoError::SqlRuntimeError(format!(
                "Specified columns do not match columns of the table.\
                table '{}' has the following columns: '{}'",
                &insert.table,
                col_names
            )));
        }
        for row in &insert.rows {
            if !table.can_store(row) {
                return Err(BongoError::SqlRuntimeError(format!(
                    "The row '{:?}' cannot be inserted into the table '{}', because not all elements have the correct type",
                    row,
                    insert.table
                )));
            }
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(location);
        if file.is_err() {
            return Err(BongoError::WriteFileError("Could not open file on disc for writing.".to_string()));
        }

        let mut writer = BufWriter::new(file.unwrap());

        for row in &mut insert.rows {
            let mut pos = SeekFrom::End(0);

            if let Some(loc) = table.ghosts.pop() {
                pos = SeekFrom::Start(loc as u64);
            }

            if writer.seek(pos).is_err() {
                return generic_write_error();
            }

            // safe because we checked that seeking to this position did return an error
            let item_pos = writer.stream_position().unwrap();

            // write to specified position
            if writer.write_all(&row.as_disc_bytes(&table.cols)?).is_err() {
                return generic_write_error();
            }

            // update index
            match table.idx.1.get_mut(&row[0]) {
                None => {
                    // index does not exist for this key yet.
                    // row.remove(0) allows to avoid copying as this element is not needed anymore anyways.
                    table.idx.1.insert(row.remove(0), vec![item_pos]);
                }
                Some(indices) => {
                    // hash collision -> append new row index to list
                    indices.push(item_pos);
                }
            }
        }

        if writer.flush().is_err() {
            return generic_write_error();
        }

        table.row_count += insert.rows.len();

        Ok(None)
    }

    ///
    /// An `Update` statement is executed as follows:
    ///
    /// 1. check if table exists in cache
    /// 2. check if the table exists on disc
    /// 3. check if specified columns in set expression exist in the specified table
    /// 4. get indices of the relevant columns via DicsIndexer
    /// 5. iterate over row-indices and for each row-index:
    ///     - load row data from disc
    ///     - check if condition applies (if there is a condition) and if so:
    ///         + apply the assignments to the row+
    ///         + write row back to disc
    ///         + if the assignments also included the indexed column, update the index key for that row
    /// 6. return Ok(None)
    ///
    fn update(&mut self, update: Update) -> BongoResult {
        let mut path = self.get_table_dir_if_exists(&update.table)?;
        let tables = self.tables_read_access()?;
        // unwrap safe, because we have checked the entry exists before
        let cell = tables.get(&update.table).unwrap();
        let table_lock = cell.borrow_mut();
        let table = table_lock.write();

        if table.is_err() {
            return Err(BongoError::InternalError("Could not acquire read access to cached meta data.".to_string()));
        }
        let mut table = table.unwrap();

        // error if there is at least one column that does not exists
        if update.assignments.get_col_names().iter().any(|name| {
            !table.cols.get_col_names().contains(name)
        }) {
            return Err(BongoError::SqlRuntimeError(format!(
                "There were columns in the SET expressions that are not columns of the table '{}'",
                &update.table
            )));
        }

        let indexer = DiscIndexer::from_opt_expr(&table.idx, update.condition);
        let col_names = table.cols.get_col_names();

        path.push("data.bongo");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path);
        if file.is_err() {
            return Err(BongoError::ReadFileError("Could not get read access to file on disc".to_string()));
        }
        let mut file = file.unwrap();

        let mut row_buffer = Vec::with_capacity(table.row_size);
        unsafe { row_buffer.set_len(table.row_size) } // expand buffer to avoid useless initialization

        for i in indexer.indices {
            if file.seek(SeekFrom::Start(i)).is_err() {
                return Err(ReadFileError("Could not jump to correct position in file".to_string()));
            }
            if file.read_exact(&mut row_buffer).is_err() {
                return Err(BongoError::ReadFileError("Could not read row from disc".to_string()));
            }
            let row = Row::from_disc_bytes(&row_buffer, &table.cols.get_d_types())?;
            // TODO: LOW_PRIO: look for index dynamically in the future, as for now the first column is the indexed column
            let old_idx_val = row[0].clone();

            // if no condition exists or the existing condition evaluates to true this row shall be returned
            if indexer.expr.is_none() ||
                indexer.expr.as_ref().unwrap().eval(&row, &col_names)? {
                // modify row according to SET expressions
                let mut row = row.apply_assignments(&update.assignments, &col_names)?;

                // write modified row to disc
                if file.seek(SeekFrom::Start(i)).is_err() { // reset position
                    return Err(ReadFileError("Could not jump to correct position in file".to_string()));
                }
                if file.write_all(&row.as_disc_bytes(&table.cols.get_d_types())?).is_err() {
                    return Err(BongoError::WriteFileError("Could not update row in file on disc.".to_string()));
                }

                // update index
                if update.assignments.get_col_names().contains(&table.idx.0) {
                    // remove old row index
                    let indices = table.idx.1.get_mut(&old_idx_val);
                    if indices.is_none() {
                        return Err(BongoError::InternalError("Index not correct.".to_string()));
                    }
                    let indices = indices.unwrap();
                    if indices.len() == 1 {
                        // remove whole item if the thing we want to remove is the only index belonging to this value
                        table.idx.1.remove(&old_idx_val).unwrap(); // unwrap safe, because we know for sure that the value existed
                    } else {
                        // if there are more than this element, only remove the element
                        indices.remove(i as usize); // remove from index
                    }

                    // insert new row index
                    // use remove to avoid copying. row is not needed anymore, therefore mutation is ok
                    let new_val = row.remove(0);

                    match table.idx.1.get_mut(&new_val) {
                        None => { // insert pair if this value does not exist yet
                            table.idx.1.insert(new_val, vec![i]);
                        }
                        Some(indices) => { // append index if already another index is mapped to this value
                            indices.push(i);
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    ///
    /// A `Delete` statement is executed as follows.
    ///
    /// 1. Check if table exists in cache and on disc.
    /// 2. Obtain relevant indices that might have to be deleted
    /// 3. Iterate over all those indices and remove indices that do not fulfill condition
    ///    (in case a condition exists) from list of relevant indices
    /// 4. remove the indices from the cached index `self.tables`
    /// 4. Mark all removed indices as ghosts
    /// 5. Update row_count
    /// 6. Return Ok
    ///
    fn delete(&mut self, delete: Delete) -> BongoResult {
        let table_dir = self.get_table_dir_if_exists(&delete.table)?;
        let mut data_loc = table_dir.clone();
        data_loc.push("data.bongo");

        let tables = self.tables_read_access()?;
        let cell = tables.get(&delete.table).unwrap();
        let table_lock = cell.borrow_mut();
        let table = table_lock.write();

        if table.is_err() {
            return Err(InternalError("Concurrency Error.".to_string()));
        }
        let mut table = table.unwrap();

        let indexer = DiscIndexer::from_opt_expr(&table.idx, delete.condition);

        let mut indices_to_delete = match indexer.expr {
            None => {
                // in this case we do not need to load anything from disc, because no expression means all
                // remaining indices shall be deleted.
                // -> return all indices in indexer
                indexer.indices
            }
            Some(expr) => {
                // in case there is an expression we have to assemble a new list of indices
                let mut indices = vec![];

                let file = File::open(&data_loc);
                if file.is_err() {
                    return Err(BongoError::ReadFileError("Could not get read access to file on disc".to_string()));
                }
                let mut file = file.unwrap();

                let mut row_buffer = Vec::with_capacity(table.row_size);
                unsafe { row_buffer.set_len(table.row_size) } // expand buffer to avoid useless initialization

                // additional for loop to fail early. Slower but safer
                for i in indexer.indices {
                    if file.seek(SeekFrom::Start(i)).is_err() {
                        return Err(BongoError::ReadFileError("Could not jump to correct position in file.".to_string()));
                    }
                    if file.read_exact(&mut row_buffer).is_err() {
                        return Err(BongoError::ReadFileError("Cannot read indexed row from file.".to_string()));
                    }
                    let row = Row::from_disc_bytes(&row_buffer, &table.cols.get_d_types())?;

                    if expr.eval(&row, &table.cols.get_col_names())? {
                        indices.push(i);
                    }
                }

                indices
            }
        };

        // remove indices from cached index
        table.idx.1 = table.idx.1.clone().into_iter()
            .map(|(lit, ids)| -> (BongoLiteral, Vec<u64>){
                let new_ids = ids.into_iter().filter(|i| {
                    !indices_to_delete.contains(i) // remove i if it shall be deleted
                }).collect();
                (lit, new_ids)
            }).filter(|(_lit, ids)| {
            !ids.is_empty() // remove entries that have no associated indices anymore
        }).collect();

        table.row_count -= indices_to_delete.len();

        // mark removed rows as ghosts
        table.ghosts.append(&mut indices_to_delete);

        Ok(None)
    }

    ///
    /// A `CreateTable` statement is executed as follows.
    ///
    /// 1. check that table does not exist
    /// 2. check that folder does not exist
    /// 3. create folder
    /// 4. create empty data.bongo file
    /// 5. update self.tables
    ///
    fn create_table(&mut self, create_table: CreateTable) -> BongoResult {
        let mut location = self.get_table_dir_on_disc(&create_table.table);

        let mut tables = self.tables_write_access()?;
        if tables.contains_key(&create_table.table) {
            return Err(BongoError::SqlRuntimeError(format!("Table '{}' already exists", &create_table.table)));
        }

        if location.exists() {
            return Err(BongoError::InternalError("DB root directory contains invalid elements.".to_string()));
        }

        if fs::create_dir(&location).is_err() {
            return Err(BongoError::WriteFileError("Could directory for new table.".to_string()));
        }

        location.push("data.bongo");
        if File::create(location).is_err() {
            return Err(BongoError::WriteFileError("Could not create data.bongo file for new table".to_string()));
        }

        let row_size = create_table.cols.iter()
            .map(|col_def| { col_def.as_ref().disc_size() })
            .sum();

        tables.insert(create_table.table.clone(),
                      RefCell::new(RwLock::new(
                          TableMetaData {
                              idx: (create_table.cols[0].name.clone(), HashMap::new()),
                              cols: create_table.cols,
                              ghosts: vec![],
                              row_size,
                              row_count: 0,
                          })));

        Ok(None)
    }

    ///
    /// A `DropTable` statement is executed as follows.
    ///
    /// for each table name:
    ///  1. check if table exists in self.tables
    ///  2. check if table correctly exists on disc
    ///  3. delete directory for the table on disc and delete cache entry (self.tables)
    ///
    fn drop_table(&mut self, drop_table: DropTable) -> BongoResult {
        // extra for loop here to fail early before actual execution begins.
        // slightly less performant but more secure.
        let mut paths_to_delete = HashMap::new();
        for table_name in drop_table.names {
            let path = self.get_table_dir_if_exists(&table_name)?;
            paths_to_delete.insert(table_name, path);
        }

        let mut tables = self.tables_write_access()?;

        for (name, path) in paths_to_delete {
            if fs::remove_dir_all(&path).is_err() {
                return Err(BongoError::WriteFileError(
                    format!("Could not delete directory '{}'",
                            path.to_str().unwrap())));
            }

            // previous conditions in this function assure that this will be safe because the key exists
            tables.remove(&name).unwrap();
        }

        Ok(None)
    }

    fn create_drop_db() -> BongoResult {
        BongoResult::Err(BongoError::UnsupportedFeatureError(
            "'CREATE DATABASE and DROP DATABASE statements are not supported by BongoDB so far. \
            BongoDB assumes there is only one database."
                .to_string(),
        ))
    }

    fn table_exists_in_cache(&self, table_name: &str) -> Result<bool, BongoError> {
        let tables = self.tables_read_access()?;
        Ok(tables.get(table_name).is_some())
    }

    fn get_table_dir_on_disc(&self, table_name: &str) -> PathBuf {
        let mut table_dir = self.db_root.clone();
        table_dir.push(table_name);

        table_dir
    }

    ///
    /// Checks if a table exists in cache and on disc.
    /// If not found returns an Error.
    /// If found returns the path to the directory of the table.
    ///
    fn get_table_dir_if_exists(&self, table_name: &str) -> Result<PathBuf, BongoError> {
        if !self.table_exists_in_cache(&table_name)? {
            return Err(BongoError::SqlRuntimeError(format!(
                "Execution of statement failed because table {} does not exist.",
                &table_name)));
        }

        let path = self.get_table_dir_on_disc(&table_name);

        if !path.is_dir() {
            return Err(BongoError::InternalError(
                "Cache inconsistency detected.".to_string()));
        }

        Ok(path)
    }

    fn tables_read_access(&self) -> Result<RwLockReadGuard<HashMap<String, RefCell<RwLock<TableMetaData>>>>, BongoError> {
        let tables = self.tables.read();
        if tables.is_err() {
            return Err(BongoError::InternalError("Concurrency Error.".to_string()));
        }
        Ok(tables.unwrap())
    }

    fn tables_write_access(&mut self) -> Result<RwLockWriteGuard<HashMap<String, RefCell<RwLock<TableMetaData>>>>, BongoError> {
        let tables = self.tables.write();
        if tables.is_err() {
            return Err(BongoError::InternalError("Concurrency Error.".to_string()));
        }
        Ok(tables.unwrap())
    }
}

///
/// An expression that can trivially indexed.
///

struct TrivialIdxExpr {
    ///
    /// The column to be indexed.
    /// Not used yet, might be useful later.
    ///
    pub _col: String,
    ///
    /// The comparison operator which must be `Eq` or `NotEq` because only those are supported.
    ///
    pub op: IndexBinOp,
    ///
    /// The value that the value of the column is compared to.
    ///
    pub val: BongoLiteral,
}

enum IndexBinOp {
    Eq,
    NotEq,
}

impl TryFrom<&BinOp> for IndexBinOp {
    type Error = ();

    fn try_from(op: &BinOp) -> Result<Self, Self::Error> {
        match op {
            BinOp::Eq => Ok(IndexBinOp::Eq),
            BinOp::NotEq => Ok(IndexBinOp::NotEq),
            _ => Err(())
        }
    }
}

struct DiscIndexer {
    pub indices: Vec<u64>,
    ///
    /// Concrete row indexes are already known due to use of index.
    /// Because `TrivialIdxExpr` only supports non-nested expressions, we can calculate row indices
    /// from the `TrivialIdxExpr` and know that all those row indices will match the condition.
    /// Therefore, In the indexable cases there is no expression to be applied
    ///
    pub expr: Option<Expr>,
}

impl DiscIndexer {
    ///
    /// Returns all rows indices that are applicable after potentially having applied the index and
    /// the expression left to apply to all those indices. In case the index could already be used the
    /// expression is None, and no linear search is needed anymore. In case the index could not be used
    /// all indices are returned as a linear search is needed along with the expression which must be
    /// checked for each of the indices.
    ///
    pub fn from_opt_expr(idx: &(String, HashMap<BongoLiteral, Vec<u64>>), opt_expr: Option<Expr>) -> Self {
        let (name, map) = idx;

        match opt_expr {
            None => {
                // not indexable and no condition -> all indices that are in map + None
                Self {
                    indices: Self::idx_to_indices(map.clone()),
                    expr: None,
                }
            }
            Some(expr) => {
                match TrivialIdxExpr::try_from((name.as_str(), &expr)) {
                    Ok(idx_expr) => {
                        match idx_expr.op {
                            IndexBinOp::Eq => {
                                // indexable with Eq operator -> return the contents of the index at that position + None
                                // because condition is always true for all values at the those indices
                                match map.get(&idx_expr.val) {
                                    None => Self { indices: vec![], expr: None },
                                    Some(indices) => Self { indices: indices.clone(), expr: None }
                                }
                            }
                            IndexBinOp::NotEq => {
                                // indexable with NotEq operator -> return all indexes but the ones that match the index
                                // and none, because expr is true for all values at those indices
                                let mut idx = map.clone();
                                idx.remove(&idx_expr.val);
                                Self { indices: Self::idx_to_indices(idx), expr: None }
                            }
                        }
                    }
                    Err(_) => {
                        // not indexable -> all indices in map + Some(expression)
                        // because the expression must still be evaluated for each value
                        Self { indices: Self::idx_to_indices(map.clone()), expr: Some(expr) }
                    }
                }
            }
        }
    }

    fn idx_to_indices(idx: HashMap<BongoLiteral, Vec<u64>>) -> Vec<u64> {
        idx.into_iter()
            .map(|(_lit, indices)| { indices })
            .collect::<Vec<Vec<u64>>>()
            .concat()
    }
}

impl TryFrom<(&str, &Expr)> for TrivialIdxExpr {
    type Error = ();
    ///
    /// In BongoDB so far if an `Expr` can be converted to a `TrivialIdxExpr` all elements fulfilling the
    /// expression can be calculated using the tables index. If not then the evaluation of the `Expr`
    /// can not trivially be sped up by using an index.
    /// For BongoDB to use indexes where ever possible it would need to be able to recursively
    /// restructure `Expr`s which is unfortunately out of the scope of this project.
    ///
    fn try_from((idx_col, expr): (&str, &Expr)) -> Result<Self, Self::Error> {
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                match IndexBinOp::try_from(op) {
                    Ok(idx_op) => {
                        match &(**left) {
                            Expr::Identifier(name) => {
                                match &(**right) {
                                    Expr::BinaryExpr { .. } => {
                                        // not trivially indexable, requires recursive analysing
                                        Err(())
                                    }
                                    Expr::Identifier(_) => {
                                        // not  indexable, because index requires concrete value lookup
                                        Err(())
                                    }
                                    Expr::Value(val) => {
                                        return if name == idx_col {
                                            Ok(TrivialIdxExpr {
                                                _col: name.to_string(),
                                                op: idx_op,
                                                val: (*val).clone(),
                                            })
                                        } else {
                                            // the identifier is not the indexed column
                                            Err(())
                                        };
                                    }
                                }
                            }
                            Expr::Value(val) => {
                                match &(**right) {
                                    Expr::BinaryExpr { .. } => {
                                        // not trivially indexable, requires recursive analysing
                                        Err(())
                                    }
                                    Expr::Identifier(name) => {
                                        return if name == idx_col {
                                            Ok(TrivialIdxExpr {
                                                _col: name.to_string(),
                                                op: idx_op,
                                                val: (*val).clone(),
                                            })
                                        } else {
                                            // the identifier is not the indexed column
                                            Err(())
                                        };
                                    }
                                    Expr::Value(_) => {
                                        // not  indexable, because this expression does not contain any Identifier (column).
                                        // It can in fact be immediately evaluated, but this does not matter here.
                                        Err(())
                                    }
                                }
                            }
                            Expr::BinaryExpr { .. } => {
                                // not trivially indexable, requires recursive analysing
                                Err(())
                            }
                        }
                    }
                    Err(_) => { Err(()) }
                }
            }
            // not trivially indexable, requires recursive analysing
            _ => Err(())
        }
    }
}

// TODO: LOW_PRIO: write concurrency tests with by using many threads that access the same executor
#[cfg(test)]
mod tests {
    use bongo_core::bongo_request::BongoRequest;
    use bongo_core::bongo_result::BongoResult;
    use bongo_core::types::{BongoLiteral, Row};

    use crate::executor::Executor;

    mod constructor {
        use std::fs;

        use bongo_core::types::BongoError;

        use crate::executor::Executor;

        #[test]
        fn non_existing_path_create_db_true() {
            let path = "test_temp/non_existing_path_create_db_true";
            non_existing_path(path, true);
            // clen up, because this call should create a new DB
            fs::remove_dir(path).unwrap();
        }

        #[test]
        fn non_existing_path_create_db_false() {
            non_existing_path("test_temp/non_existing_path_create_db_false", false);
        }

        fn non_existing_path(path: &str, create_db: bool) {
            let ex = Executor::new(path, create_db, false);
            match ex {
                Ok(_) => {
                    // if we create the db this should return Ok(_)
                    assert!(create_db);
                }
                Err(err) => {
                    // an error should be thrown if we do not create the db
                    assert!(!create_db);
                    assert_eq!(
                        BongoError::DatabaseNotFoundError(format!("location: '{}'", path)),
                        err
                    )
                }
            }
        }

        #[test]
        fn existing_path() {
            let path = "test_temp/test_existing_path";
            fs::create_dir_all(path).unwrap();

            let ex = Executor::new(path, false, false);

            fs::remove_dir(path).unwrap();

            match ex {
                Ok(ex) => {
                    assert!(ex.db_root.is_absolute());
                }
                Err(_) => {
                    panic!("This should not return an error on unix systems")
                }
            }
        }
    }

    mod create_drop_tables {
        use std::fs;
        use std::path::PathBuf;
        use std::str::FromStr;

        use bongo_core::bongo_request::BongoRequest;
        use crate::executor::Executor;
        use crate::executor::tests::create_example_table;

        ///
        /// 1. Creates an `Executor` on en empty DB and creates a table
        /// 2. Destroys `Executor`
        /// 3. Creates new `Executor` and checks if the table can be loaded from disc
        /// 4. Drops Table and ensures that table got successfully dropped from cache and disc worked
        ///
        /// NOTE: uses private functions for test
        ///
        #[test]
        fn create_table_and_load_in_next_session_and_drop() {
            let db_root = PathBuf::from_str("test_temp/create_table_and_load_in_next_session_and_drop").unwrap();

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                // create table
                create_example_table(&mut ex, "table_1");
            }

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();

                match ex.tables.read().unwrap().get("table_1") {
                    None => { panic!("The entry table_1 does not exist in the meta data table") }
                    Some(_data) => {
                        // nice, some data is in there
                        // unfortunately it is a bit hard checking the data is correct as it is behind a RwLock.
                        // But we have loads of other tests that check the data is loaded correctly using the
                        // public select interface of the Executor
                    }
                }

                // drop table again
                let drop_table_req = BongoRequest { sql: "DROP TABLE table_1;".to_string() };
                ex.execute(&drop_table_req).unwrap();

                // table has been removed from cache
                let tables = ex.tables_read_access().unwrap();
                assert!(tables.is_empty());

                // table has been removed from disk
                assert!(db_root.read_dir().unwrap().next().is_none())

                // leaving this scope drops the executor before cleaning up the directory.
                // This way the executors drop method can work like expected.
            }

            // clean up afterwards (CAREFULLY!)
            fs::remove_dir_all(db_root).unwrap();
        }
    }

    mod insert {
        use std::fs;
        use std::fs::File;
        use std::io::Read;
        use std::path::PathBuf;

        use bongo_core::bytes_on_disc::FromDiscBytes;
        use bongo_core::types::{BongoDataType, Row};

        use crate::executor::Executor;
        use crate::executor::tests::{create_example_table, get_example_rows, insert_example_rows};

        #[test]
        fn insert_in_empty_db() {
            let db_root = PathBuf::from("test_temp/insert_in_empty_db");
            let mut loc = db_root.clone();
            let table_name = "table_1";
            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            // read contents from DB directly to buffer
            loc.push(table_name);
            loc.push("data.bongo");
            let mut file = File::open(&loc).unwrap();
            let mut buffer = vec![];
            file.read_to_end(&mut buffer).unwrap();

            let table_def = vec![BongoDataType::Int,
                                 BongoDataType::Varchar(256),
                                 BongoDataType::Bool];

            // convert bytes back to BongoLiterals
            let rows = buffer.chunks(BongoDataType::Int.disc_size() +
                BongoDataType::Varchar(256).disc_size()
                + BongoDataType::Bool.disc_size())
                .map(|row| {
                    Row::from_disc_bytes(row, &table_def).unwrap()
                })
                .collect::<Vec<Row>>();

            let expected_rows = get_example_rows();

            // clean up before assertion
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(rows, expected_rows);
        }
    }

    // NOTE: this also involves create table and insert statement
    mod select {
        use std::fs;
        use std::path::PathBuf;

        use bongo_core::bongo_request::BongoRequest;
        use bongo_core::types::{BongoLiteral, Row};

        use crate::executor::Executor;
        use crate::executor::tests::{create_example_table, get_example_rows, insert_example_rows};

        #[test]
        ///
        /// - specify a subset of columns
        /// - nested where condition
        /// - descending order
        /// - test on two executors to see if persisting cache works correctly
        ///
        fn all_features_together() {
            let db_root = PathBuf::from("test_temp/all_features_together");
            let table_name = "table_1";

            let sql = format!("SELECT col_1, col_3 \
               FROM {table_name} \
               WHERE col_2 = 'c❤' OR col_3 \
               ORDER BY col_1 DESC");
            let request = BongoRequest { sql: sql.to_string() };
            // should return the first two columns in the opposite order and only col_1 and col_3
            let expected = get_example_rows().into_iter()
                .take(2)
                .rev() // example rows are initially sorted ascending by row 1
                .map(|row| {
                    vec![row[0].clone(), row[2].clone()]
                })
                .collect::<Vec<Row>>();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn where_clause_false() {
            let db_root = PathBuf::from("test_temp/where_clause_false");
            let table_name = "table_1";

            let sql = format!("SELECT col_1, col_3 \
               FROM {table_name} \
               WHERE false \
               ORDER BY col_1 DESC");
            let request = BongoRequest { sql: sql.to_string() };
            // should return the first two columns in the opposite order and only col_1 and col_3
            let expected: Vec<Row> = vec![];
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn wildcard_no_where() {
            let db_root = PathBuf::from("test_temp/wildcard_no_where");
            let table_name = "table_1";

            let sql = format!("SELECT * \
               FROM {table_name} \
               ORDER BY col_1 DESC");
            let request = BongoRequest { sql: sql.to_string() };
            // should return all columns in reverse order
            let expected: Vec<Row> = get_example_rows().into_iter().rev().collect();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn order_by_non_selected_col() {
            let db_root = PathBuf::from("test_temp/order_by_non_selected_col");
            let table_name = "table_1";

            let sql = format!("SELECT col_2, col_3 \
               FROM {table_name} \
               ORDER BY col_1 DESC");
            let request = BongoRequest { sql: sql.to_string() };
            // should return columns 2 and 3 in reverse order
            let expected: Vec<Row> = get_example_rows().into_iter()
                .rev() // example rows are initially sorted ascending
                .map(|row| {
                    vec![row[1].clone(), row[2].clone()]
                })
                .collect::<Vec<Row>>();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn order_asc() {
            let db_root = PathBuf::from("test_temp/order_asc");
            let table_name = "table_1";

            let sql = format!("SELECT * \
               FROM {table_name} \
               ORDER BY col_2 ASC"); // col_2 is ordered desc on insert
            let request = BongoRequest { sql: sql.to_string() };
            // should return all columns in reverse order
            let expected: Vec<Row> = get_example_rows().into_iter().rev().collect();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        ///
        /// Tests how the system responds if the where condition is indexable
        ///
        #[test]
        fn eq_indexable_where() {
            let db_root = PathBuf::from("test_temp/eq_indexable_where");
            let table_name = "table_1";

            let sql = format!("SELECT * \
               FROM {table_name} \
               WHERE col_1 = 3");
            let request = BongoRequest { sql: sql.to_string() };
            // should return only 3rd row
            let expected: Vec<Row> = vec![get_example_rows().remove(2)];
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn comparison_with_null() {
            let db_root = PathBuf::from("test_temp/comparison_with_null");
            let table_name = "table_1";

            let sql = format!("SELECT * \
                                     FROM {table_name} \
                                     WHERE col_3 = Null");
            let request = BongoRequest { sql: sql.to_string() };
            // should return only 3rd row
            let expected: Vec<Row> = get_example_rows().into_iter()
                .filter(|r| {
                    match r[2] {
                        BongoLiteral::Null => true,
                        _ => false
                    }
                }).collect();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }
    }

    mod delete {
        use std::{fs, iter};
        use std::path::PathBuf;

        use bongo_core::bongo_request::BongoRequest;
        use bongo_core::types::{BongoLiteral, Row};

        use crate::Executor;
        use crate::executor::tests::{create_example_table, get_example_rows, insert_example_rows};

        #[test]
        fn simple_delete() {
            let db_root = PathBuf::from("test_temp/simple_delete");
            let table_name = "table_1";

            create_table_and_delete_nth_row(table_name, &db_root, 3);

            let select = format!("SELECT * \
                               FROM {table_name} \
                               ORDER BY col_1 ASC;");

            let request = BongoRequest { sql: select.to_string() };

            // shall return all but the 3rd row
            let expected: Vec<Row> = get_example_rows().into_iter()
                .enumerate()
                .filter(|&(i, _)| i != 2)
                .map(|(_, r)| r).collect();
            let result;

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&request).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn delete_and_insert_in_ghosts() {
            let db_root = PathBuf::from("test_temp/delete_and_insert_in_ghosts");
            let table_name = "table_1";

            create_table_and_delete_nth_row(table_name, &db_root, 3);

            let insert = format!("INSERT INTO {table_name} (col_1, col_2, col_3) VALUES \
                          (42, 'x', true);");
            let insert_req = BongoRequest { sql: insert.to_string() };

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                { // new scope to drop the ReadGuard and avoid deadlock
                    // assert we have one ghost after deleting one row
                    assert_eq!(ex.tables.read().unwrap().get("table_1").unwrap().borrow().read().unwrap().ghosts.len(), 1)
                }
                ex.execute(&insert_req).unwrap();
                {
                    // asser the one ghost is now replaced with a value
                    assert!(ex.tables.read().unwrap().get("table_1").unwrap().borrow().read().unwrap().ghosts.is_empty())
                }
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            let select = format!("SELECT * \
                               FROM {table_name} \
                               ORDER BY col_1 ASC;");

            let select_req = BongoRequest { sql: select.to_string() };

            // 3rd row shall be removed and instead the inserted row shall be contained
            let expected: Vec<Row> = get_example_rows().into_iter()
                .enumerate()
                .filter(|&(i, _)| i != 2)
                .map(|(_, r)| r)
                .chain(iter::once(vec![BongoLiteral::Int(42), BongoLiteral::Varchar("x".to_string()), BongoLiteral::Bool(true)]))
                .collect();
            let result;

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                result = ex.execute(&select_req).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        fn create_table_and_delete_nth_row(table_name: &str, db_root: &PathBuf, n: i32) {
            let db_root = PathBuf::from(db_root);

            let delete = format!("DELETE FROM {table_name} \
                               WHERE col_1 = {n};");

            let request = BongoRequest { sql: delete };

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, false).unwrap();
                ex.execute(&request).unwrap();
            } // leaving scope triggers drop and flush on the executor
        }
    }

    mod update {
        use std::fs;
        use std::path::PathBuf;
        use bongo_core::bongo_request::BongoRequest;
        use bongo_core::types::{BongoLiteral, Row};
        use crate::Executor;
        use crate::executor::tests::{create_example_table, get_example_rows, insert_example_rows};

        #[test]
        fn update_including_index() {
            let db_root = PathBuf::from("test_temp/update_including_index");
            let table_name = "table_1";

            let update = format!("UPDATE {table_name} \
                                SET col_1 = 42, col_3 = NULL;");
            let select = "SELECT * FROM table_1 \
                               ORDER BY col_2 DESC ;";
            let update_req = BongoRequest { sql: update.to_string() };
            let select_req = BongoRequest { sql: select.to_string() };

            let expected = get_example_rows().into_iter()
                .map(|mut row| {
                    row[0] = BongoLiteral::Int(42);
                    row[2] = BongoLiteral::Null;
                    row
                })
                .collect::<Vec<Row>>();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, false).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
            } // leaving scope triggers drop and flush on the executor

            {
                let mut ex = Executor::new(&db_root, false, true).unwrap();
                ex.execute(&update_req).unwrap();
                result = ex.execute(&select_req).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }

        #[test]
        fn update_with_wrong_d_type() {
            let db_root = PathBuf::from("test_temp/update_with_wrong_d_type");
            let table_name = "table_1";

            let update = format!("UPDATE {table_name} \
                                SET col_1 = 'col_1 is datatype int...';");
            let request = BongoRequest { sql: update.to_string() };
            let result;

            {
                let mut ex = Executor::new(&db_root, true, true).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
                result = ex.execute(&request);
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert!(result.is_err());
        }

        #[test]
        fn update_only_one() {
            let db_root = PathBuf::from("test_temp/update_only_one");
            let table_name = "table_1";

            let update = format!("UPDATE {table_name} \
                                SET col_2 = 'updated' \
                                WHERE col_1 = 3;");
            let select = format!("SELECT * FROM {table_name} \
                               ORDER BY col_1 ASC ;");
            let update_req = BongoRequest { sql: update };
            let select_req = BongoRequest { sql: select };

            let expected = get_example_rows().into_iter()
                .enumerate()
                .map(|(i, mut row)| {
                    if i == 2 {
                        row[1] = BongoLiteral::Varchar("updated".to_string());
                    }
                    row
                })
                .collect::<Vec<Row>>();
            let result;

            {
                let mut ex = Executor::new(&db_root, true, true).unwrap();
                create_example_table(&mut ex, table_name);
                insert_example_rows(&mut ex, table_name);
                ex.execute(&update_req).unwrap();
                result = ex.execute(&select_req).unwrap().unwrap();
            } // drop executors before cleanup to avoid executor flushing on non existing dir.

            // clean up before assertion in case it panics
            fs::remove_dir_all(&db_root).unwrap();

            assert_eq!(expected, result);
        }
    }


    ///
    /// creates an an example table with three columns
    ///
    fn create_example_table(ex: &mut Executor, table_name: &str) {
        let request = BongoRequest {
            sql: format!("CREATE TABLE {table_name} \
                                ( \
                                    col_1 INT, \
                                    col_2 VARCHAR(256), \
                                    col_3 BOOLEAN, \
                                ); ")
        };

        let result = ex.execute(&request);

        assert_eq!(BongoResult::Ok(None), result);

        // drop(Executor) gets triggered here and the executor writes its cache to disc.
    }

    ///
    /// Inserts some rows into the example table created with `create_example_table`.
    ///
    fn insert_example_rows(ex: &mut Executor, table_name: &str) {
        let sql = format!("INSERT INTO {table_name} (col_1, col_2, col_3) VALUES
                              (1, 'd❤', true),
                              (2, 'c❤', false),
                              (3, 'b❤', Null),
                              (4, 'a❤', false);");
        let request = BongoRequest { sql };

        ex.execute(&request).unwrap();
    }

    ///
    /// Returns the example rows inserted in `insert_example_rows`.
    ///
    fn get_example_rows() -> Vec<Row> {
        return vec![
            vec![BongoLiteral::Int(1), BongoLiteral::Varchar("d❤".to_string()), BongoLiteral::Bool(true)],
            vec![BongoLiteral::Int(2), BongoLiteral::Varchar("c❤".to_string()), BongoLiteral::Bool(false)],
            vec![BongoLiteral::Int(3), BongoLiteral::Varchar("b❤".to_string()), BongoLiteral::Null],
            vec![BongoLiteral::Int(4), BongoLiteral::Varchar("a❤".to_string()), BongoLiteral::Bool(false)],
        ];
    }
}
