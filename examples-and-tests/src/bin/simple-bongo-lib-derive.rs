use bongo_lib::connection::Connection;
use bongo_lib::derives::{CreateDropTable, FromRow, Insert, Select};

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
    #[allow(dead_code)] // this is never read in our example, but only printed with dbg! we do not want that warning.
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

#[cfg(test)]
mod tests {
    use bongo_lib::derives::{CreateDropTable, FromRow, Insert, Select, SelectPrimary};
    use bongo_lib::traits::{
        CreateDropTableQuery, FromRow, InsertQuery, SelectPrimaryQuery, SelectQuery,
    };
    use bongo_lib::types::{BongoLiteral, Row};

    #[derive(Debug, PartialEq, Select, FromRow, CreateDropTable, SelectPrimary, Insert)]
    struct Test {
        #[PrimaryKey]
        id: i64,
        name: String,
        test: bool,
    }

    #[test]
    fn from_row() {
        let row: Row = vec![
            BongoLiteral::Int(1),
            BongoLiteral::Varchar("Test".to_string()),
            BongoLiteral::Bool(true),
        ];

        let test = Test {
            id: 1,
            name: "Test".to_string(),
            test: true,
        };

        assert_eq!(Ok(test), Test::from_row(row));
    }

    #[test]
    fn create_table() {
        let query = "CREATE TABLE Test (id INT, name VARCHAR(255), test BOOLEAN);";

        assert_eq!(query, Test::create_table_query());
    }

    #[test]
    fn drop_table() {
        let query = "DROP TABLE Test;";

        assert_eq!(query, Test::drop_table_query());
    }

    #[test]
    fn select_all() {
        let query = "SELECT id, name, test FROM Test;";

        assert_eq!(query, Test::select_all_query());
    }

    #[test]
    fn select_where() {
        let query = "SELECT id, name, test FROM Test WHERE id = 1;";

        assert_eq!(query, Test::select_where_query("id = 1"));
    }

    #[test]
    fn select_primary() {
        let query = "SELECT id, name, test FROM Test WHERE id=1;";

        assert_eq!(query, Test::select_primary_query(1));
    }

    #[test]
    fn insert_head() {
        let query = "INSERT INTO Test (id, name, test) VALUES";

        assert_eq!(query, Test::insert_query_head());
    }

    #[test]
    fn insert_values() {
        let test = Test {
            id: 1,
            name: "Test".to_string(),
            test: true,
        };

        let query = "(1, 'Test', true)";

        assert_eq!(query, test.insert_query_values());
    }

    #[test]
    fn insert_values_multiple() {
        let test = vec![
            Test {
                id: 1,
                name: "Test1".to_string(),
                test: true,
            },
            Test {
                id: 2,
                name: "Test2".to_string(),
                test: false,
            },
            Test {
                id: 3,
                name: "Test3".to_string(),
                test: true,
            },
        ];

        let query = "(1, 'Test1', true), (2, 'Test2', false), (3, 'Test3', true)";

        assert_eq!(query, test.as_slice().insert_query_values());
    }
}
