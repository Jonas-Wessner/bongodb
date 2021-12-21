# Rust Database

# Databaseserver

- webserver
    - wait for tcp connections
    - receive SQL commands
    - data format: JSON
- simple SQL parser
    - extract information from string
        - operation type: insert, update...
        - arguments: vec<vec<String>> → outer vec: rows → inner vec: cols
    - check meta file for correct datatypes
        - convert Strings to correct datatypes
    - commands
        - insert
        - update
        - delete
        - create database
        - create table
        - drop table
- manage files
    - check for concurrent access
    - locks:
        - unlimited select operations → blocks write operations
        - 1 write operations → block everything
        - Simplest solution: block entire file
        - advanced solution: try to only block affected rows
- Data structure
    - meta data file for table information
        - column names
        - datatypes
            - for now:
                - i64
                - bool
                - varchar
            - for later:
                - u64
                - f64
                - etc.
    - binary format
        - fixed size rows
    - binary search for key
        - b-tree
    - hash tables with hash mapped to row
    - duplicate hashes result in linear search after binary search
    - file per table

      [https://www.youtube.com/watch?v=OyBwIjnQLtI](https://www.youtube.com/watch?v=OyBwIjnQLtI)

    tcp connected → new thread → wait for commands → parse sql → check for concurrent access → execute command → return result → send response with result → wait for new commands (back to step 3) → client exit thread

## Data format transmitted via tcp between server and client

- data-format Client -> Server:
  {
  "sql": ""
  }


- data-format Server -> Client:
- The Structure of the return data is implicitly known, because the client sends the select statement and therefore
  knows which table shall be queried.
- Only select returns an array of "data". Other statements return an empty array of "data"
  For select statements:
- As every transmitted information is encoded in json, the webserver knows, when a message is fully transmitted over tcp. Therefore another transmitting protocol like HTTP, which would cause too much overhead, can be omitted.

```json
  {
  "success_code": ENUM_SUCCESS_CODE,
  "data": [
    [
      1,
      "Peter",
      35,
      175,
      0
    ],
    [
      2,
      "Güther",
      33,
      180,
      2000
    ],
    ...
  ]
}
```

ENUM_SUCCESS_CODE:

- 0 -> Sucessful
- 1 -> Error invalid statement
- 2 -> error correct statement, but cannot be executed
- possibly other codes

## Supported SQL statements:
- Only simple inserts without default values:
```sql
INSERT INTO table_name (column_list)
VALUES
    (value_list_1),
    (value_list_2),
    ...
    (value_list_n);
```

```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition; 
```

```sql
DELETE FROM table_name WHERE condition;
```

```sql
CREATE DATABASE testDB;
```

```sql
CREATE TABLE table_name (
                            column1 datatype,
                            column2 datatype,
                            column3 datatype,
    ....
); 
```

```sql
DROP TABLE table_name; 
```

```sql
DROP DATABASE databasename; 
```

### Major restrictions:

- Where Conditions only with operators <, > = (especially no AND, OR operators)
- No JOINs


# Database client library

- connect to webserver
- functions for executing sql queries
    - query as string
    - returns result
        - return value: vec<Struct>
- for later:
    - prepared statement