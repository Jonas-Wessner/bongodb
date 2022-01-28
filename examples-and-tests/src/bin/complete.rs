use bongo_lib::connection::Connection;
use bongo_lib::derives::FromRow;
use bongo_lib::types::Row;

#[derive(Debug, FromRow)]
struct Person {
    id: i64,
    name: String,
    married: bool,
    grade_in_asp: Option<i64>,
}

fn main() {
    let mut conn = Connection::connect("localhost:8080").unwrap();

    conn.execute(
        "CREATE TABLE Person (id INT, name VARCHAR(255), married BOOLEAN, grade_in_asp INT);",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO Person (id, name, married, grade_in_asp) VALUES \
    (1, 'James', true, NULL),\
    (2, 'Karl', false, NULL),\
    (3, 'Sarah', true, NULL),\
    (4, 'Jonas', false, 1),\
    (5, 'Simon', false, 1),\
    (6, 'David', true, 5),\
    (7, 'Linda', true, 2),\
    (8, 'Pascal', true, NULL);",
    )
    .unwrap();

    let result: Vec<Row> = conn.query("SELECT * FROM Person").unwrap();
    dbg!(&result);

    let result: Vec<Person> = conn.query("SELECT * FROM Person").unwrap();
    dbg!(&result);

    conn.execute("DROP TABLE Person").unwrap();

    conn.disconnect().unwrap();
}