use sqlparser::ast::Expr as SpExpr;
use sqlparser::ast::Statement as SpStatement;
use sqlparser::dialect::{Dialect, GenericDialect, PostgreSqlDialect};
use sqlparser::parser::{Parser, ParserError};

/// PG dialect first (for `?` / `?|` / `?&` JSON ops + TRUNCATE CASCADE);
/// fall back to Generic on parse error (for SQLite-style `TRIM(s, c)` etc.).
pub fn parse_statements(sql: &str) -> Result<Vec<SpStatement>, ParserError> {
    parse_with_fallback(sql, Parser::parse_sql)
}

pub fn parse_expr(sql: &str) -> Result<SpExpr, ParserError> {
    parse_with_fallback(sql, |dialect, sql| {
        Parser::new(dialect).try_with_sql(sql)?.parse_expr()
    })
}

fn parse_with_fallback<T, F>(sql: &str, parse_fn: F) -> Result<T, ParserError>
where
    F: Fn(&dyn Dialect, &str) -> Result<T, ParserError>,
{
    let pg = PostgreSqlDialect {};
    match parse_fn(&pg, sql) {
        Ok(r) => Ok(r),
        Err(pg_err) => {
            let generic = GenericDialect {};
            parse_fn(&generic, sql).map_err(|_| pg_err)
        }
    }
}

#[cfg(test)]
#[path = "dialect_tests.rs"]
mod tests;
