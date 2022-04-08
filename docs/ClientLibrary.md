# Client Library

# Main library

## Connection struct

The `Connection` struct is the main interface to a BongoDB server. It provides a set of functions that allow the execution of SQL statements on the server.

## Connect function

The `connect` function tries to connect to the given url and returns a `Result` . If the connection was successfully established, then a `Connection` will be returned. Otherwise it returns a `BongoError`

```rust
let conn = Connection::connect("localhost:8080");
```

## Disconnect function

The `disconnect` function tries to disconnect from the server and returns a Result. If the disconnection was successfull, nothing will be returned. Otherwise it returns a `BongoError` .
You do not have to explicitly close the connection yourself. The connection also gets closed when the connection struct goes out of scope, so this function must only be used if the connection shall be closed before its scope ends.

```rust
let result = conn.disconnect();
```

## Execute function

The `execute` function can execute an sql statement on a server. Returns a `Result` containing either nothing when successful or a `BongoError` . The sql statement may not be a SELECT.

```rust
let result = conn.execute("DELETE FROM table1 where name = 'Pascal';");
```

## Query function

The `query` function can exectue an sql select statement on a server. Returns a result containing either the `Row` s matching the sql statement when successful or a `BongoError` . The sql statement may only be a SELECT statement.

```rust
let result = conn.execute("SELECT * FROM table1;");
```

## Select primary function

The `select_primary` function can return a struct that implements the `SelectPrimary` trait, based on its primary key. Returns a `Result` containing either an instance of the struct with this primary key or a `BongoError` .

```rust
struct Sample {
    id: i64
}

let result: Sample = conn.select_primary(1).unwrap;
```

## Select all function

The select_all function can return a vec of structs that implements the `Select` trait. Returns a result containing either a list of structs or a BongoError.

```rust
struct Sample {
    id: i64
}

let result: Vec<Sample> = conn.select_all().unwrap;
```

## Select where function

The select_where function can return a vec of structs which fulfill the given where clause. The struct has to implement the `Select` trait. Returns a result containing either a list of structs or a BongoError.

```rust
struct Sample {
    id: i64
}

let result: Vec<Sample> = conn.select_where("id=1").unwrap;
```

## Create table function

The create_table function can create a table on the database, based on the members of a struct. The struct has to implement the `CreateDropTable` trait. Returns a result containing either nothing when successfull or a BongoError.

```rust
struct Sample {
    id: i64
}

let result = conn.create_table::<Sample>();
```

## Drop table function

The drop_table function can drop a table on the database, based on a struct. The struct has to implement the `CreateDropTable` trait. Returns a result containing either nothing when successfull or a BongoError.

```rust
struct Sample {
    id: i64
}

let result = conn.drop_table::<Sample>();
```

## Insert function

The insert function can a struct or a slice of structs on the database. The struct has to implement the `Insert` trait. Returns a result containing either nothing when successfull or a BongoError.

```rust
struct Sample {
    id: i64
}

let sample = Sample{ id: 1 };

let result = conn.insert(sample);

let samples = vec![
    Sample{ id: 1 },
    Sample{ id: 2 },
    Sample{ id: 3 }
];

let result = conn.insert(&samples[..]);
```

# Derive addons

The derive macros use the name of a struct and its members to create sql statements for the traits of the main library.

## FromRow derive macro

The `FromRow` derive macro can be used on structs to automatically implement the `FromRow` trait. This allows the main library to automatically convert a database entry to a struct.

### Attributes

* Persistent: Marks a member as persistent (members not marked as persistent will be ignored)

```rust
#[derive(FromRow)]
struct Sample {
    id: i64
}
```

or

```rust
#[derive(FromRow)]
struct Sample {
    #[Persistent]
    id: i64,
    sample: u32,
}
```

## Select derive macro

The `Select` derive macro can be used on structs to automatically implement the `Select` trait.
This allows the usage of the `select` and `select_where` function. Requires the `FromRow` trait.

### Attributes

* TableName: Declares the table name for a struct
* Persistent: Marks a member as persistent (members not marked as persistent will be ignored)

```rust
#[derive(Select)]
struct Sample {
    id: i64
}
```

or

```rust
#[derive(Select)]
#[TableName("Sample2")]
struct Sample {
    #[Persistent]
    id: i64
}
```

## Select primary derive macro

The `SelectPrimary` derive macro can be used on structs to automatically implement the `SelectPrimary` trait.
This allows the usage of the `select_primary` function. Requires the `FromRow` trait.

### Attributes

* PrimaryKey: Declares a struct member as the primary key of a database table
* TableName: Declares the table name for a struct
* Persistent: Marks a member as persistent (members not marked as persistent will be ignored)

```rust
#[derive(SelectPrimary)]
struct Sample {
    #[Primarykey]
    id: i64
}
```

or

```rust
#[derive(SelectPrimary)]
#[TableName("Sample2")]
struct Sample {
    #[Primarykey]
    #[Persistent]
    id: i64
}
```

## Create drop table derive macro

The `CreateDropTable` derive macro can be used on structs to automatically implement the `CreateDropTable` trait.
This allows the usage of the `create_table` and `drop_table` function.

### Attributes

* TableName: Declares the table name for a struct
* Persistent: Marks a member as persistent (members not marked as persistent will be ignored)

```rust
#[derive(CreateDropTable)]
struct Sample {
    id: i64
}
```

or

```rust
#[derive(CreateDropTable)]
#[TableName("Sample2")]
struct Sample {
    #[Persistent]
    id: i64
}
```

## Insert derive macro

The `Insert` derive macro can be used on structs to automatically implement the `Insert` trait.
This allows the usage of the `insert` function.

### Attributes

* TableName: Declares the table name for a struct
* Persistent: Marks a member as persistent (members not marked as persistent will be ignored)

```rust
#[derive(Insert)]
struct Sample {
    id: i64
}
```

or

```rust
#[derive(Insert)]
#[TableName("Sample2")]
struct Sample {
    #[Persistent]
    id: i64
}
```
