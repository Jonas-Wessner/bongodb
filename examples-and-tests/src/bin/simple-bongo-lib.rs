use bongo_lib::connection::Connection;
use bongo_lib::types::Row;

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

#[cfg(test)]
mod tests {
    use bongo_lib::connection::Connection;
    use bongo_lib::derives::{CreateDropTable, FromRow, Insert, Select, SelectPrimary};

    use bongo_lib::types::{BongoError, Row};

    #[derive(Debug, PartialEq, Clone, Select, FromRow, CreateDropTable, SelectPrimary, Insert)]
    struct Test {
        #[PrimaryKey]
        id: i64,
    }

    #[ignore]
    #[test]
    fn connect() {
        let conn = Connection::connect("localhost:8080");

        assert!(conn.is_ok());

        let conn = Connection::connect("");

        assert!(conn.is_err());
    }

    #[ignore]
    #[test]
    fn disconnect() {
        let mut conn = Connection::connect("localhost:8080").unwrap();

        assert!(conn.disconnect().is_ok());
    }

    #[ignore]
    #[test]
    fn execute() {
        let mut conn = Connection::connect("localhost:8080").unwrap();
        assert_eq!(conn.execute("CREATE TABLE execute_test (id INT);"), Ok(()));
        assert_eq!(
            conn.execute("SELECT * FROM execute_test"),
            Err(BongoError::UnsupportedFeatureError(
                "You must not use a select statement in the execute function".to_string()
            ))
        );

        conn.execute("DROP TABLE execute_test;").unwrap();
    }

    #[ignore]
    #[test]
    fn query() {
        let mut conn = Connection::connect("localhost:8080").unwrap();
        conn.execute("CREATE TABLE query_test (id INT);").unwrap();

        let empty: Vec<Row> = vec![];

        assert_eq!(conn.query("SELECT * FROM query_test"), Ok(empty));
        assert_eq!(
            conn.query::<Row>("CREATE TABLE query_test (id INT);"),
            Err(BongoError::UnsupportedFeatureError(
                "You may only use a select statement in the query function".to_string()
            ))
        );

        conn.execute("DROP TABLE query_test;").unwrap();
    }

    #[ignore]
    #[test]
    fn select_primary() {
        #[derive(Debug, PartialEq, Clone, FromRow, SelectPrimary)]
        #[TableName("select_primary_test")]
        struct Test {
            #[PrimaryKey]
            id: i64,
        }

        let mut conn = Connection::connect("localhost:8080").unwrap();
        conn.execute("CREATE TABLE select_primary_test (id INT);")
            .unwrap();
        conn.execute("INSERT INTO select_primary_test (id) VALUES (1);")
            .unwrap();

        let test = Test { id: 1 };

        assert_eq!(conn.select_primary(1), Ok(test));
        assert_eq!(
            conn.select_primary::<Test, i64>(2),
            Err(BongoError::SqlRuntimeError(
                "No entry for that primary key".to_string(),
            ))
        );

        conn.execute("DROP TABLE select_primary_test;").unwrap();
    }

    #[ignore]
    #[test]
    fn select_all() {
        #[derive(Debug, PartialEq, Clone, FromRow, Select)]
        #[TableName("select_all_test")]
        struct Test {
            id: i64,
        }

        let mut conn = Connection::connect("localhost:8080").unwrap();
        conn.execute("CREATE TABLE select_all_test (id INT);")
            .unwrap();
        conn.execute("INSERT INTO select_all_test (id) VALUES (1);")
            .unwrap();

        let test = vec![Test { id: 1 }];

        assert_eq!(conn.select_all(), Ok(test));

        conn.execute("DROP TABLE select_all_test;").unwrap();
    }

    #[ignore]
    #[test]
    fn select_where() {
        #[derive(Debug, PartialEq, Clone, FromRow, Select)]
        #[TableName("select_where_test")]
        struct Test {
            id: i64,
        }

        let mut conn = Connection::connect("localhost:8080").unwrap();
        conn.execute("CREATE TABLE select_where_test (id INT);")
            .unwrap();
        conn.execute("INSERT INTO select_where_test (id) VALUES (1);")
            .unwrap();

        let test = vec![Test { id: 1 }];
        let empty: Vec<Test> = vec![];

        assert_eq!(conn.select_where("id=1"), Ok(test));
        assert_eq!(conn.select_where("id=2"), Ok(empty));

        conn.execute("DROP TABLE select_where_test;").unwrap();
    }

    #[ignore]
    #[test]
    fn create_table() {
        #[derive(Debug, PartialEq, CreateDropTable)]
        #[TableName("create_table_test")]
        struct Test {
            id: i64,
        }

        let mut conn = Connection::connect("localhost:8080").unwrap();

        assert_eq!(conn.create_table::<Test>(), Ok(()));
        assert!(conn.create_table::<Test>().is_err());

        conn.execute("DROP TABLE create_table_test;").unwrap();
    }

    #[ignore]
    #[test]
    fn drop_table() {
        #[derive(Debug, PartialEq, CreateDropTable)]
        #[TableName("drop_table_test")]
        struct Test {
            id: i64,
        }

        let mut conn = Connection::connect("localhost:8080").unwrap();
        conn.execute("CREATE TABLE drop_table_test (id INT);")
            .unwrap();
        assert_eq!(conn.drop_table::<Test>(), Ok(()));
        assert!(conn.drop_table::<Test>().is_err());
    }

    #[ignore]
    #[test]
    fn insert() {
        #[derive(Debug, PartialEq, Clone, Insert)]
        #[TableName("insert_test")]
        struct Test {
            id: i64,
        }

        let mut conn = Connection::connect("localhost:8080").unwrap();
        conn.execute("CREATE TABLE insert_test (id INT);").unwrap();

        let test = Test { id: 0 };
        let test_vec = vec![Test { id: 1 }, Test { id: 2 }];

        assert_eq!(conn.insert(&test), Ok(()));
        assert_eq!(conn.insert(&test_vec[..]), Ok(()));

        conn.execute("DROP TABLE insert_test;").unwrap();
    }
}
