# Features of Bongo DB

## Statements

Any statements not explicitly mentioned here are not supported by BongoDB (yet).

### SELECT

```sql
SELECT <select_item>
FROM <table>
[WHERE <expression>]
[ORDER BY <col_name> [ASC, DESC]]
```

* `<select_item>` is either a comma separated list of columns (e.g. col_1, col_2, col_3) OR a wildcard `*`.
* If the `ORDER BY` clause is specified without `ASC` or `DESC`, it defaults to `ASC`.
* If no `ORDER BY` clause is specified, the order in which rows are returned is non-deterministic due to usage of a hash index and due to the fact that the internal order of the data may not reflect the order in which data has been inserted for performance reasons.
* Check out the section about expressions to find out what expressions are supported.

### INSERT

```sql
INSERT INTO <table> (<col_1>, <col_2>, <col_3>)
VALUES
('sql', 123, 'rust'), 
('is', 124, 'is'), 
[...]
('crazy.', 126, 'fun.');
```

* the list of column names must match the list of columns in the table exactly, the order also needs to be the same.
* Assigning a value of the wrong datatype is not allowed and this will be enforced by the BongoDB.

### UPDATE

```sql
UPDATE <table_name>
SET <col_1> = <val_1>, <col_2> = <val_2> ...
[WHERE <expression>]
```

* Assigning a value of the wrong datatype is not allowed and this will be enforced by the BongoDB.
* Check out the section about expressions to find out what expressions are supported.

### DELETE

```sql
DELETE FROM <table>
[WHERE <expression>]
```

* Check out the section about expressions to find out what expressions are supported.
* Implementation detail: Deleting a row does not actually delete it from disc, but only marks it as ghost (unused). The next `INSERT` statement will then overwrite it. This makes deletions very fast and avoids restructuring the table. Imagine you had one million rows and had to shift all (and then also update the index) just because the first row is deleted. However, this means that a table is actually never getting smaller on disc even if the user deletes a lot of rows. We plan on implementing a garbage collector in the future which will take care of this issue and reorganize the disc as soon as a certain limit of ghost entries is reached.

### CREATE TABLE

```sql
CREATE TABLE <table_name> (
    <col_1> <datatype>,
    <col_2> <datatype>,
    <col_3> <datatype>,
     [...]
); 
```

### DROP TABLE

```sql
DROP TABLE <table_1> [, <table_2>, <table_3> ...]; 
```

### FLUSH

```sql
FLUSH;
```

* `FLUSH` is a statement that is specific to BongoDB.
* Calling flush writes all kept in memory to disk. If the server crashes for some reason after a flush it can be safely restarted and loads all data back from disc. If, however, after the execution of a statement but before a flush, the data on disc might be invalid.
* The BongoServer can be set into auto_flush mode when starting up which will call flush after each statement.
* If the server is shutdown by the program it will automatically call flush. However the graceful shutdown through code is so far not implemented. The safest way so far to shutdown your server is therefore setting it to auto_flush mode and killing it when you can be sure that there are no statements executed at this time.

### CREATE DB

* This is not implemented as BongoDB always works with exactly on DB which maps to one directory on disc. If the Server is started with the parameter `create_db = true` it will create a new database if the folder does not exist yet.

### DROP DB

* Databases cannot be dropped from code, because the BongoDB server assumes there is always exactly one valid database when running (accept on startup where it optionally creates one for you).

## Expressions

* The following binary operators are supported:
    - `>`, `<`, `>=`, `<=`, `=`, `!=`, `AND`, `OR`
* Expressions can be nested arbitrarily deep and are evaluated recursively.
* applying a binary operator to an invalid combination of operands will result in an error that is returned to the client.

## Datatypes and Literals

The following Datatypes are supported:

### `INT`

* 64-bit signed integer values.

### `BOOLEAN`

* Boolean values *true* and *false*

### `VARCHAR(size)`

* A unicode String with a length limit of size bytes.
* Because unicode characters have different sizes this is NOT the size of the actual characters.
* Inserting a String into that column that is too big will result in an error which is returned to the client.

Note that all datatypes are nullable (can store the literal NULL) and therefore work out of the box with the rust Option type.

## Indexing

* By default a hash index built on the first column of a table at time of its creation.
* Currently it is not possible to create more indices on custom columns after the table has been created.
* Usage of the hash is not fully optimized yet. As of now the hash index can be used if all the following conditions are fulfilled:
    - The expression is a binary expression with the operator `=` or `!=`
    - One of the operands is an identifier which is the indexable column (as of now this is always the first column of the table)
    - The other operand is a literal.
* This means that indices are especially not used by BongoDB if recursive expression evaluation would be required. However, if you simply want to get e.g. a customer with a given name, the indices work just right.
* If the index could not be used, a linear search over the entire table is performed, because it has to be checked for each element in the table if the expression would evaluate to true.
* B-Tree indices are not supported yet.

## Parallelism and Concurrency

* You can basically have an unlimited amount of concurrent connections to a BongoDB server. But of course at some point the server will run out of resources.
* *Receiving commands*, *parsing SQL* and *transmitting responses* are done 100% in parallel for multiple parallel connections as those actions are totally independent from other connections.
* The execution of the statements themselves have some restrictions to them. We allow the maximum amount of parallelism while still keeping thread-safety making all your statements run as fast as possible.
   - `CREATE TABLE` , `DROP TABLE` and `FLUSH` statements require exclusive access to the entire DB. i.e. they block until they are scheduled for an exclusive access and other concurrent statements must then wait.
   - `INSERT` , `UPDATE` and `DELETE` statements require exclusive access on the table they refer to. This means that these statements are executed in parallel as long as they run on disjoint subsets of tables.
   - `SELECT` s require read-only access to their respective tables. This means that an unlimited amount of selects can be scheduled in parallel.
