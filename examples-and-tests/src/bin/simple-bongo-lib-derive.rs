use bongo_lib::connection::Connection;
use bongo_lib::derives::{CreateDropTable, FromRow, Insert, Select, SelectPrimary};

#[derive(Debug, Default, Clone, FromRow, SelectPrimary, Select, CreateDropTable, Insert)]
struct Person {
    #[PrimaryKey]
    #[Persistent]
    id: i64,

    #[Persistent]
    name: String,

    #[Persistent]
    married: bool,

    #[Persistent]
    grade_in_asp: Option<i64>,
    test: u32,
}

fn main() {
    let mut conn = Connection::connect("localhost:8080").unwrap();
    conn.create_table::<Person>().unwrap();

    let person = Person {
        id: 0,
        name: "Eddy".to_string(),
        married: false,
        grade_in_asp: None,
        test: 1,
    };

    conn.insert(&person).unwrap();

    let persons = vec![
        Person {
            id: 1,
            name: "James".to_string(),
            married: true,
            grade_in_asp: Some(3),
            test: 1,
        },
        Person {
            id: 2,
            name: "Karl".to_string(),
            married: false,
            grade_in_asp: None,
            test: 1,
        },
        Person {
            id: 3,
            name: "Sarah".to_string(),
            married: true,
            grade_in_asp: None,
            test: 1,
        },
        Person {
            id: 4,
            name: "Jonas".to_string(),
            married: false,
            grade_in_asp: Some(1),
            test: 1,
        },
        Person {
            id: 5,
            name: "Simon".to_string(),
            married: false,
            grade_in_asp: Some(1),
            test: 1,
        },
        Person {
            id: 6,
            name: "David".to_string(),
            married: true,
            grade_in_asp: Some(5),
            test: 1,
        },
        Person {
            id: 7,
            name: "Linda".to_string(),
            married: true,
            grade_in_asp: Some(2),
            test: 1,
        },
        Person {
            id: 8,
            name: "Pascal".to_string(),
            married: true,
            grade_in_asp: None,
            test: 1,
        },
    ];

    conn.insert_multiple(&persons).unwrap();

    let result: Vec<Person> = conn.select_all().unwrap();
    dbg!(result);

    let result: Vec<Person> = conn.select_all().unwrap();
    dbg!(result);

    let result: Vec<Person> = conn.select_where("married=true").unwrap();
    dbg!(result);

    let result: Person = conn.select_primary(1).unwrap();
    dbg!(result);

    #[derive(Debug, Select, FromRow)]
    #[TableName("Person")]
    struct PersonQuery {
        name: String,
        married: bool,
    }

    let result: Vec<PersonQuery> = conn.select_where("married=true").unwrap();
    dbg!(result);

    conn.drop_table::<Person>().unwrap();

    conn.disconnect().unwrap();
}
