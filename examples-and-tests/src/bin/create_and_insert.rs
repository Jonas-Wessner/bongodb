use bongo_lib::connection::Connection;

fn main() {
    let mut conn = Connection::connect("localhost:8080").unwrap();

    conn.execute("CREATE TABLE Person (id INT, name VARCHAR(255), married BOOLEAN);")
        .unwrap();

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

    conn.disconnect().unwrap();
}
