pub struct SqlParser {}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::{Statement as Ast};
use crate::bongo_server::statements::Statement;

impl SqlParser {
    pub fn parse(sql: &str) -> Result<Statement, ParserError> {
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, sql)?[0];

        return Self::ast_to_statement(ast);
    }

    fn ast_to_statement(ast: Ast) -> Result<Statement, ParserError> {
        match ast {
            Ast::Query(_) => {}
            Ast::Insert { .. } => {}
            Ast::Update { .. } => {}
            Ast::Delete { .. } => {}
            Ast::CreateTable { .. } => {}
            Ast::Drop { .. } => {}
            _ => {}
        }
        todo!()
    }
}