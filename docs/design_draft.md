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
    

# Database client library

- connect to webserver
- functions for executing sql queries
    - query as string
    - returns result
        - return value: vec<Struct>
- for later:
    - prepared statement