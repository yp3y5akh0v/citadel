use sqlparser::ast::{BinaryOperator, Expr as SpExpr, Statement as SpStatement};
use sqlparser::dialect::{Dialect, GenericDialect, PostgreSqlDialect, Precedence};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

/// PG-superset dialect: delegates every method to `PostgreSqlDialect` and adds the
/// `@?_tz` / `@@_tz` infix operators (tokenized as `[AtQuestion|AtAt, Word("_tz")]`,
/// stitched via `parse_infix` into `BinaryOperator::Custom`).
#[derive(Debug)]
pub struct CitadelDialect {
    inner: PostgreSqlDialect,
}

impl CitadelDialect {
    pub fn new() -> Self {
        Self {
            inner: PostgreSqlDialect {},
        }
    }
}

impl Default for CitadelDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl Dialect for CitadelDialect {
    fn dialect(&self) -> std::any::TypeId {
        self.inner.dialect()
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        self.inner.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        self.inner.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.inner.is_delimited_identifier_start(ch)
    }

    fn is_custom_operator_part(&self, ch: char) -> bool {
        self.inner.is_custom_operator_part(ch)
    }
    fn supports_unicode_string_literal(&self) -> bool {
        self.inner.supports_unicode_string_literal()
    }
    fn supports_string_literal_backslash_escape(&self) -> bool {
        self.inner.supports_string_literal_backslash_escape()
    }
    fn supports_string_escape_constant(&self) -> bool {
        self.inner.supports_string_escape_constant()
    }
    fn supports_numeric_literal_underscores(&self) -> bool {
        self.inner.supports_numeric_literal_underscores()
    }
    fn supports_nested_comments(&self) -> bool {
        self.inner.supports_nested_comments()
    }
    fn supports_factorial_operator(&self) -> bool {
        self.inner.supports_factorial_operator()
    }
    fn supports_bitwise_shift_operators(&self) -> bool {
        self.inner.supports_bitwise_shift_operators()
    }
    fn supports_geometric_types(&self) -> bool {
        self.inner.supports_geometric_types()
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        self.inner.get_next_precedence(parser)
    }
    fn prec_value(&self, prec: Precedence) -> u8 {
        self.inner.prec_value(prec)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        self.inner.supports_filter_during_aggregation()
    }
    fn supports_within_after_array_aggregation(&self) -> bool {
        self.inner.supports_within_after_array_aggregation()
    }
    fn supports_group_by_expr(&self) -> bool {
        self.inner.supports_group_by_expr()
    }
    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        self.inner.supports_named_fn_args_with_eq_operator()
    }
    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        self.inner.supports_named_fn_args_with_assignment_operator()
    }
    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        self.inner.supports_named_fn_args_with_rarrow_operator()
    }
    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        self.inner.supports_named_fn_args_with_colon_operator()
    }
    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        self.inner.supports_named_fn_args_with_expr_name()
    }
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        self.inner.supports_window_function_null_treatment_arg()
    }
    fn supports_dictionary_syntax(&self) -> bool {
        self.inner.supports_dictionary_syntax()
    }
    fn supports_lambda_functions(&self) -> bool {
        self.inner.supports_lambda_functions()
    }

    fn supports_in_empty_list(&self) -> bool {
        self.inner.supports_in_empty_list()
    }
    fn supports_start_transaction_modifier(&self) -> bool {
        self.inner.supports_start_transaction_modifier()
    }
    fn supports_parenthesized_set_variables(&self) -> bool {
        self.inner.supports_parenthesized_set_variables()
    }
    fn supports_select_wildcard_except(&self) -> bool {
        self.inner.supports_select_wildcard_except()
    }
    fn supports_empty_projections(&self) -> bool {
        self.inner.supports_empty_projections()
    }
    fn convert_type_before_value(&self) -> bool {
        self.inner.convert_type_before_value()
    }
    fn supports_triple_quoted_string(&self) -> bool {
        self.inner.supports_triple_quoted_string()
    }
    fn supports_array_typedef_with_brackets(&self) -> bool {
        self.inner.supports_array_typedef_with_brackets()
    }
    fn supports_create_index_with_clause(&self) -> bool {
        self.inner.supports_create_index_with_clause()
    }
    fn supports_explain_with_utility_options(&self) -> bool {
        self.inner.supports_explain_with_utility_options()
    }
    fn supports_listen_notify(&self) -> bool {
        self.inner.supports_listen_notify()
    }
    fn supports_comment_on(&self) -> bool {
        self.inner.supports_comment_on()
    }
    fn supports_load_extension(&self) -> bool {
        self.inner.supports_load_extension()
    }
    fn supports_set_names(&self) -> bool {
        self.inner.supports_set_names()
    }
    fn supports_alter_column_type_using(&self) -> bool {
        self.inner.supports_alter_column_type_using()
    }
    fn supports_notnull_operator(&self) -> bool {
        self.inner.supports_notnull_operator()
    }
    fn supports_interval_options(&self) -> bool {
        self.inner.supports_interval_options()
    }
    fn allow_extract_custom(&self) -> bool {
        self.inner.allow_extract_custom()
    }
    fn allow_extract_single_quotes(&self) -> bool {
        self.inner.allow_extract_single_quotes()
    }

    /// Without this hook sqlparser sees `@?` followed by identifier `_tz` and parse-errors.
    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &SpExpr,
        precedence: u8,
    ) -> Option<Result<SpExpr, ParserError>> {
        let next = parser.peek_token().token;
        let custom_op = match next {
            Token::AtQuestion => "@?_tz",
            Token::AtAt => "@@_tz",
            _ => return self.inner.parse_infix(parser, expr, precedence),
        };
        let after = parser.peek_nth_token(1).token;
        let Token::Word(w) = after else {
            return self.inner.parse_infix(parser, expr, precedence);
        };
        if !w.value.eq_ignore_ascii_case("_tz") {
            return self.inner.parse_infix(parser, expr, precedence);
        }
        parser.advance_token();
        parser.advance_token();
        let right = match parser.parse_subexpr(precedence) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(SpExpr::BinaryOp {
            left: Box::new(expr.clone()),
            op: BinaryOperator::Custom(custom_op.to_string()),
            right: Box::new(right),
        }))
    }
}

/// Falls back to `GenericDialect` only for SQLite-style quirks PG rejects (e.g. `TRIM(s, c)`).
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
    let citadel = CitadelDialect::new();
    match parse_fn(&citadel, sql) {
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
