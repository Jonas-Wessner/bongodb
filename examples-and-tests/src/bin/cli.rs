///
/// This is a code example on how you may use the bongo-lib to communicate with a BongoServer.
/// Note that this example was created to demonstrate the functionality of BongoDB in the presentation
/// of this project and is not an actual part of the BongoDB project. Therefore code quality might
/// just be sufficient but not be perfect.
///
/// The cli application allows you to quickly test out what BongoDB offers.
/// It allows you to execute arbitrary SQL as well as run functions that perform e.g. insertion of
/// example values for more convenient testing.
/// Note that for running integration tests cargo test should be run instead of using the cli as this
/// is much faster and also tests concurrency.
/// The cli-application is just for demonstration purposes and not meant to be a testing tool.
///
use std::fmt::Debug;
use std::io;

use bongo_lib::connection::Connection;
use bongo_lib::derives::{FromRow, Insert};
use bongo_lib::traits::FromRow;
use bongo_lib::types::Row;

#[derive(Debug, PartialEq, FromRow, Insert)]
struct Person {
    id: i64,
    name: String,
    married: bool,
    grade_in_asp: Option<i64>,
}

fn main() -> io::Result<()> {
    let mut con = Connection::connect("localhost:8080").unwrap();

    let mut input = String::new();
    let stdin = io::stdin();

    loop {
        print_menu();
        input.clear();
        stdin.read_line(&mut input)?;
        input = input.trim().to_lowercase();

        match input.as_str() {
            "1" => create_example_table(&mut con, "Person"),
            "2" => insert_example_rows(&mut con, "Person"),
            "3" => select_all::<Row>(&mut con, "Person"),
            "4" => select_all::<Person>(&mut con, "Person"),
            "5" => {
                println!("Enter Query in one line.");

                let mut sql = String::new();
                stdin.read_line(&mut sql)?;

                query(&mut con, &sql);
            }
            "6" => {
                println!("Enter Statement in one line.");

                let mut sql = String::new();
                stdin.read_line(&mut sql)?;

                execute_sql(&mut con, &sql);
            }
            "7" => drop_table(&mut con, "Person"),
            "x" => break,
            _ => println!("Invalid input."),
        }

        println!()
    }

    con.disconnect().unwrap();

    println!("Disconnected from server. Bye.");

    Ok(())
}

fn print_menu() {
    println!("[1] - Create 'Person' table");
    println!("[2] - Insert example rows into 'Person' table");
    println!("[3] - Select all rows of table");
    println!("[4] - Select all 'Person's in table");
    println!("[5] - Execute some query");
    println!("[6] - Execute some statement");
    println!("[7] - Drop 'Person' table");
    println!("[x] - Disconnect and exit")
}

fn create_example_table(con: &mut Connection, table_name: &str) {
    let sql = format!(
        "CREATE TABLE {table_name}(\
            id              INT,\
            name            VARCHAR(255),\
            married         BOOLEAN,\
            grade_in_asp    INT );"
    );

    execute_sql(con, &sql);
}

fn insert_example_rows(con: &mut Connection, table_name: &str) {
    let sql = format!(
        "INSERT INTO {table_name} (id, name, married, grade_in_asp) VALUES \
            (1, 'James', true, 3),\
            (2, 'Karl', false, NULL),\
            (3, 'Sarah', true, NULL),\
            (4, 'Jonas', false, 1),\
            (5, 'Simon', false, 1),\
            (6, 'David', true, 5),\
            (7, 'Linda', true, 2),\
            (8, 'Pascal', true, NULL);"
    );

    execute_sql(con, &sql);
}

fn select_all<T: FromRow<T> + Debug>(con: &mut Connection, table_name: &str) {
    let sql = format!(
        "SELECT * FROM {table_name} \
         ORDER BY id ASC;"
    );

    match con.query::<T>(&sql) {
        Ok(rows) => {
            for row in rows {
                println!("{:?}", row);
            }
        }
        Err(err) => {
            println!("An error occurred:\n{:?}", err)
        }
    }
}

fn execute_sql(con: &mut Connection, sql: &str) {
    match con.execute(sql) {
        Ok(_) => {
            println!("OK")
        }
        Err(err) => {
            println!("An error occurred:\n{:?}", err)
        }
    }
}

fn query(con: &mut Connection, sql: &str) {
    match con.query::<Row>(sql) {
        Ok(rows) => {
            for row in rows {
                println!("{:?}", row);
            }
        }
        Err(err) => {
            println!("An error occurred:\n{:?}", err)
        }
    }
}

fn drop_table(con: &mut Connection, table_name: &str) {
    let sql = format!("DROP TABLE {table_name};");
    match con.execute(&sql) {
        Ok(_) => {
            println!("OK")
        }
        Err(err) => {
            println!("An error occurred:\n{:?}", err)
        }
    }
}

fn get_example_rows() -> Vec<Person> {
    vec![
        Person {
            id: 1,
            name: "James".to_string(),
            married: true,
            grade_in_asp: Some(3),
        },
        Person {
            id: 2,
            name: "Karl".to_string(),
            married: false,
            grade_in_asp: None,
        },
        Person {
            id: 3,
            name: "Sarah".to_string(),
            married: true,
            grade_in_asp: None,
        },
        Person {
            id: 4,
            name: "Jonas".to_string(),
            married: false,
            grade_in_asp: Some(1),
        },
        Person {
            id: 5,
            name: "Simon".to_string(),
            married: false,
            grade_in_asp: Some(1),
        },
        Person {
            id: 6,
            name: "David".to_string(),
            married: true,
            grade_in_asp: Some(5),
        },
        Person {
            id: 7,
            name: "Linda".to_string(),
            married: true,
            grade_in_asp: Some(2),
        },
        Person {
            id: 8,
            name: "Pascal".to_string(),
            married: true,
            grade_in_asp: None,
        },
    ]
}

///
/// The following tests briefly verify the functionality of the server and clients together (integration test).
/// It requires that a BongoServer is running on localhost:8080.
///
/// Note: as all tests run in parallel with different connections to the same server
/// this also somehow tests concurrency a bit.
///
#[cfg(test)]
mod tests {
    extern crate core;

    use bongo_lib::connection::Connection;
    use crate::{get_example_rows, Person};

    #[ignore]
    #[test]
    fn select_all() {
        let table_name = "select_all"; // make sure parallel tests do not mess each other up
        let mut con = Connection::connect("localhost:8080").unwrap();

        create_table_with_sample_rows(&mut con, table_name);

        let expected = get_example_rows();

        // note  that selection order is non-deterministic if no order by is specified due to usage of hash index
        let sql = format!(
            "SELECT * FROM {table_name} \
                        ORDER BY id ASC;"
        );

        let result: Vec<Person> = con.query(&sql).unwrap();

        // clean up
        con.execute(&format!("DROP TABLE {table_name}")).unwrap();
        con.disconnect().unwrap();

        assert_eq!(expected, result)
    }

    #[ignore]
    #[test]
    fn select_with_complex_where_clause() {
        let table_name = "select_with_complex_features"; // make sure parallel tests do not mess each other up
        let mut con = Connection::connect("localhost:8080").unwrap();

        create_table_with_sample_rows(&mut con, table_name);

        let expected = get_example_rows()
            .into_iter()
            .enumerate()
            .filter(|(_, p)| p.id == 3 || !p.married)
            .map(|(_, p)| p)
            .collect::<Vec<Person>>();

        // note  that selection order is non-deterministic if no order by is specified due to usage of hash index
        let sql = format!(
            "SELECT * FROM {table_name} \
                        WHERE id = 3 OR married = false \
                        ORDER BY id ASC;"
        );

        let result: Vec<Person> = con.query(&sql).unwrap();

        // clean up
        con.execute(&format!("DROP TABLE {table_name}")).unwrap();
        con.disconnect().unwrap();

        assert_eq!(expected, result);
    }

    #[ignore]
    #[test]
    fn update_selection() {
        let table_name = "update_selection"; // make sure parallel tests do not mess each other up
        let mut con = Connection::connect("localhost:8080").unwrap();

        create_table_with_sample_rows(&mut con, table_name);

        let expected = get_example_rows()
            .into_iter()
            .enumerate()
            .map(|(i, mut p)| {
                if (i + 1) == 5 || p.name == "Pascal".to_string() {
                    p.married = true;
                    p.grade_in_asp = Some(42);
                }
                p
            })
            .collect::<Vec<Person>>();

        // note  that selection order is non-deterministic if no order by is specified due to usage of hash index
        let sql_update = format!(
            "UPDATE {table_name} SET \
                                 married = true, \
                                 grade_in_asp = 42 \
                                 WHERE id = 5 OR name = 'Pascal';"
        );

        // note  that selection order is non-deterministic if no order by is specified due to usage of hash index
        let sql_select = format!(
            "SELECT * FROM {table_name} \
                        ORDER BY id ASC;"
        );

        con.execute(&sql_update).unwrap();
        let result: Vec<Person> = con.query(&sql_select).unwrap();

        // clean up
        con.execute(&format!("DROP TABLE {table_name}")).unwrap();
        con.disconnect().unwrap();

        assert_eq!(expected, result);
    }

    #[ignore]
    #[test]
    fn delete_selection() {
        let table_name = "delete_selection"; // make sure parallel tests do not mess each other up
        let mut con = Connection::connect("localhost:8080").unwrap();

        create_table_with_sample_rows(&mut con, table_name);

        // only expect 3rd entry
        let expected: Vec<Person> = get_example_rows().into_iter().skip(2).take(1).collect();

        // note  that selection order is non-deterministic if no order by is specified due to usage of hash index
        let sql_update = format!(
            "DELETE FROM {table_name}
             WHERE id != 3;"
        );

        let sql_select = format!("SELECT * FROM {table_name};");

        con.execute(&sql_update).unwrap();
        let result: Vec<Person> = con.query(&sql_select).unwrap();

        // clean up
        con.execute(&format!("DROP TABLE {table_name}")).unwrap();
        con.disconnect().unwrap();

        assert_eq!(expected, result);
    }

    fn create_table_with_sample_rows(con: &mut Connection, table_name: &str) {
        // create a new table
        create_example_table(con, table_name);

        // insert some rows
        insert_example_rows(con, table_name);
    }

    fn create_example_table(con: &mut Connection, table_name: &str) {
        con.execute(&format!(
            "CREATE TABLE {table_name}\
        ( id INT, name VARCHAR(255),\
         married BOOLEAN,\
          grade_in_asp INT );"
        ))
            .unwrap();

        println!("OK");
    }

    fn insert_example_rows(con: &mut Connection, table_name: &str) {
        con.execute(&format!(
            "INSERT INTO {table_name} (id, name, married, grade_in_asp) VALUES \
                (1, 'James', true, 3),\
                (2, 'Karl', false, NULL),\
                (3, 'Sarah', true, NULL),\
                (4, 'Jonas', false, 1),\
                (5, 'Simon', false, 1),\
                (6, 'David', true, 5),\
                (7, 'Linda', true, 2),\
                (8, 'Pascal', true, NULL);"
        ))
            .unwrap();

        println!("OK");
    }
}
