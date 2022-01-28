use bongo_lib::connection::Connection;
use bongo_lib::derives::FromRow;
use bongo_lib::traits::FromRow;
use bongo_lib::types::Row;

#[derive(Debug, FromRow)]
struct Person {
    age: i64,
    name: String,
    is_alive: bool,
    option: Option<i64>,
}

// TODO: remove this as it was implemented with derive macro

//
// impl FromRow for Person {
//     fn from_row(mut row: Row) -> Self {
//         Self {
//             age: i64::from_bongo_literal(row.remove(0)),
//             name: String::from_bongo_literal(row.remove(0)),
//             is_alive: bool::from_bongo_literal(row.remove(0)),
//             option: Option::from_bongo_literal(row.remove(0)),
//         }
//     }
// }

fn main() {
    let mut conn = Connection::connect("localhost:8080").unwrap();

    conn.execute("CREATE TABLE Person (id INT, name VARCHAR(255), married BOOLEAN, option INT);");

    conn.execute(
        "INSERT INTO Person (id, name, married, option) VALUES \
    (1, 'James', true, NULL),\
    (2, 'Karl', false, NULL),\
    (3, 'Sarah', true, NULL),\
    (4, 'Jonas', false, 5),\
    (5, 'Simon', false, 879),\
    (6, 'David', true, 1),\
    (7, 'Linda', true, NULL),\
    (8, 'Pascal', true, 35);",
    )
    .unwrap();

    let result: Vec<Row> = conn.query("SELECT * FROM Person").unwrap();
    dbg!(&result);

    let result: Vec<Person> = conn.query("SELECT * FROM Person").unwrap();
    dbg!(&result);

    conn.execute("DROP TABLE Person");

    conn.disconnect().unwrap();
}
