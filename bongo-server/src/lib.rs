#![feature(iter_intersperse)]

mod executor;
pub mod sql_parser;
mod statement;
mod unsafe_sync_cell;

use std::path::Path;
use bongo_core::bongo_request::{BongoRequest, BongoRequestParser};
use bongo_core::bongo_result::{ToJson};
use bongo_core::types::{BongoError};
use webserver::Webserver;
use crate::executor::Executor;
use crate::unsafe_sync_cell::UnsafeSyncCell;

pub struct BongoServer {}

impl BongoServer {
    ///
    /// `start_new` Starts a new `BongoServer`
    /// * `address` - Address that the server shall start on e.g. "localhost:8080"
    /// * `db_root` - Root directory of the database.
    /// * `create_db` - Setting this to true will cause BongoServer to create the directory `db_root`
    /// if it does not exist and create a new database in that directory. If set to false `BongoServer`
    /// will return an error if the directory `create_db` does not exist.
    /// * `auto_flush` - Setting this to true causes `BongoServer` to execute a `FLUSH` statement after
    /// each statement automatically.
    ///
    pub async fn start_new<P>(address: &str, db_root: &P, create_db: bool, auto_flush: bool) -> Result<(), BongoError>
        where P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr> + ?Sized {
        if AsRef::<Path>::as_ref(db_root).to_str().is_none() {
            return Err(BongoError::InvalidArgumentError("only paths that are valid unicode are allowed \
            to be used as DB root directory for BongoDB".to_string()));
        }

        println!(
            "Starting BongoServer on {} with database at '{}'",
            address, AsRef::<Path>::as_ref(db_root).to_str().unwrap()
        );

        // NOTE: Executor itself ensures synchronization of accesses by using RwLock where needed.
        let ex = UnsafeSyncCell::new(Executor::new(db_root, create_db, auto_flush)?);

        Err(BongoError::WebServerError(
            Webserver::new(
                address,
                BongoRequestParser::new(),
                move |request: BongoRequest| -> String {
                    let serialized_response = ex.get().execute(&request).to_json();
                    println!("request: '{}'", request.sql);
                    println!("response: '{serialized_response}'");
                    serialized_response
                },
            )
                .start()
                .await,
        ))
    }
}
