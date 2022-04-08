# BongoDB

**BongoDB** is an SQL database written in rust.  
We also provide the client library **bongo-lib** which takes care of connecting to the server, conveniently executing queries from a rust program and mapping DB-entries to structs.  
Here you will find a quick guide on how to test out the most basic features.  
*If you would like to dive deeper in, **please check out our [Wiki](./docs/Home.md).***

## Getting the code

Install the rust nightly build channel if you have not already:

```bash
rustup install nightly
```

Clone the repository first:

```bash
git clone git@code.fbi.h-da.de:advanced-systems-programming-ws21/y1/bongodb.git
```

Verify everything works by running the tests:

```bash
make test-all
```

NOTE: you may run the test manually if you do not have make installed.

## Run the BongoDB server locally

```bash
cargo +nightly run --manifest-path bongo-server/Cargo.toml
```

This will start the server with default configuration. If you would like to change that take the server library and use it in your own executable.

## Write a simple client program using bongo-lib

In the following examples you can see how the bongo library can be used to interact with the database server.

```rust
fn main() {
    // connect to a locally running BongoDB server. Make sure you have started one
    let mut conn = Connection::connect("localhost:8080").unwrap();

    // execute a raw SQL statement
    conn.execute(
        "CREATE TABLE Person (id INT, name VARCHAR(255), married BOOLEAN, grade_in_asp INT);",
    )
        .unwrap();

    // execute a raw SQL statement
    conn.execute(
        "INSERT INTO Person (id, name, married, grade_in_asp) VALUES \
        (1, 'James', true, 3),\
        (2, 'Karl', false, NULL),\
        (3, 'Sarah', true, NULL);",
    )
        .unwrap();

    // query some rows
    let result: Vec<Row> = conn
        .query("SELECT name, married FROM Person WHERE id > 1;")
        .unwrap();
    dbg!(&result);

    // drop the table to make this example reproducible
    conn.execute("DROP TABLE Person").unwrap();

    // disconnecting from the BongoDB server automatically as soon as `conn` gets out of scope.
}
```

The full example can be found [here](./examples-and-tests/src/bin/simple-bongo-lib.rs).
Alternatively you can achieve the same by using derive macros. You will have to enable them in the Cargo.toml file.
 `bongo-lib = {path = "../bongo-lib", features=["derive"]}`

```rust
#[derive(Debug, FromRow, Select, CreateDropTable, Insert)]
struct Person {
    id: i64,
    name: String,
    married: bool,
    grade_in_asp: Option<i64>,
}

fn main() {
    // connect to a locally running BongoDB server. Make sure you have started one
    let mut conn = Connection::connect("localhost:8080").unwrap();
    // create the Person table
    conn.create_table::<Person>().unwrap();

    // create a vector of Persons
    let persons = vec![
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
    ];

    // insert all Persons into table
    conn.insert(&persons[..]).unwrap();

    // create a query that is used as result structure.
    // This is for the convenience as we are only interested in name and marriage status in this example
    // Alternatively we could also query as a `Row` or as a `Person`
    #[derive(Debug, Select, FromRow)]
    #[TableName("Person")]
    struct PersonQuery {
        name: String,
        married: bool,
    }
    let result: Vec<PersonQuery> = conn.select_where("id > 1").unwrap();
    dbg!(result);

    // drop the table to make this example reproducible
    conn.drop_table::<Person>().unwrap();

    // disconnecting from the BongoDB server automatically as soon as `conn` gets out of scope.
}
```

The full example can be found [here](./examples-and-tests/src/bin/simple-bongo-lib-derive.rs).
