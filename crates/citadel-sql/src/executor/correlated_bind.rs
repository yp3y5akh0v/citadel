use super::*;

#[derive(Default)]
struct Scope {
    columns: FxHashSet<String>,
    qualifiers: FxHashSet<String>,
    unknown_columns: bool,
}

impl Scope {
    fn add(&mut self, qualifier: &str, columns: Option<&[String]>) {
        if !qualifier.is_empty() {
            self.qualifiers.insert(qualifier.to_ascii_lowercase());
        }
        match columns {
            Some(columns) => {
                for name in columns {
                    let lower = name.to_ascii_lowercase();
                    self.columns.insert(lower.clone());
                    self.columns
                        .insert(lower.rsplit('.').next().unwrap().to_owned());
                }
            }
            None => self.unknown_columns = true,
        }
    }
}

struct Binder<'a> {
    schema: &'a SchemaManager,
    outer: &'a TableSchema,
    alias: Option<&'a str>,
    row: Option<&'a [Value]>,
    cancel: Option<citadel::CancelToken>,
    scopes: Vec<Scope>,
    ctes: Vec<FxHashMap<String, Option<Vec<String>>>>,
    views: FxHashSet<String>,
    query_depth: usize,
    bound: bool,
}

/// Detection and substitution use the same lexical resolution. Detection does
/// not execute source queries or change the supplied expression.
pub(super) fn bind_predicate(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    predicate: &mut Expr,
    ctx: &CorrelationCtx<'_>,
    row: Option<&[Value]>,
) -> Result<bool> {
    let mut binder = Binder {
        schema,
        outer: ctx.outer_schema,
        alias: ctx.outer_alias,
        row,
        cancel: wtx.cancel_token().cloned(),
        scopes: Vec::new(),
        ctes: Vec::new(),
        views: FxHashSet::default(),
        query_depth: 0,
        bound: false,
    };
    binder.expr(predicate)?;
    Ok(binder.bound)
}

impl Binder<'_> {
    fn relation_columns(&mut self, name: &str) -> Result<Option<Vec<String>>> {
        if name.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let lower = name.to_ascii_lowercase();
        for ctes in self.ctes.iter().rev() {
            if let Some(columns) = ctes.get(&lower) {
                return Ok(columns.clone());
            }
        }
        if let Some(table) = self.schema.get(&lower) {
            return Ok(Some(table.columns.iter().map(|c| c.name.clone()).collect()));
        }
        if let Some(view) = self.schema.get_view(&lower) {
            if !self.views.insert(lower.clone()) {
                return Err(SqlError::Unsupported(
                    "recursive view in correlation scope".into(),
                ));
            }
            let aliases = view.column_aliases.clone();
            let parsed = crate::parser::parse_sql(&view.sql)?;
            let Statement::Select(mut query) = parsed else {
                return Err(SqlError::Unsupported("view query must be SELECT".into()));
            };
            // A stored view is a closed catalog definition, not a capture of
            // the caller's CTEs or outer-row values.
            let caller_ctes = std::mem::take(&mut self.ctes);
            let result = self.query(&mut query, false);
            self.ctes = caller_ctes;
            self.views.remove(&lower);
            let mut columns = result?;
            if let Some(columns) = &mut columns {
                for (column, alias) in columns.iter_mut().zip(aliases) {
                    *column = alias;
                }
            }
            return Ok(columns);
        }
        let canonical = match lower.as_str() {
            "timezone_names" => "pg_timezone_names",
            "timezone_abbrevs" => "pg_timezone_abbrevs",
            other => other,
        };
        if self.schema.get_virtual(canonical).is_some() {
            // Builtins publish their columns with the registered instance.
            // A custom source without metadata still needs qualified names to
            // distinguish its columns from an enclosing mutation row.
            return Ok(self
                .schema
                .virtual_columns(canonical)
                .map(|columns| columns.iter().map(|name| (*name).to_owned()).collect()));
        }
        Err(SqlError::TableNotFound(name.to_owned()))
    }

    fn function_columns(&self, name: &str, args: &[Expr]) -> Result<Option<Vec<String>>> {
        if matches!(
            name.to_ascii_uppercase().as_str(),
            "JSONB_POPULATE_RECORD" | "JSONB_POPULATE_RECORDSET"
        ) {
            let Some(Expr::TypedNullRecord(table)) = args.first() else {
                return Err(SqlError::InvalidValue(format!(
                    "{name}: first argument must be NULL::table_type"
                )));
            };
            let table = self
                .schema
                .get(&table.to_ascii_lowercase())
                .ok_or_else(|| SqlError::TableNotFound(table.clone()))?;
            return Ok(Some(table.columns.iter().map(|c| c.name.clone()).collect()));
        }
        // These built-in JSON SRFs have fixed output schemas. A NULL argument
        // requests that schema and no rows; caller expressions are never run.
        crate::json::dispatch_srf_with_cancel(name, &[Value::Null], self.cancel.as_ref())
            .map(|(columns, _)| Some(columns))
    }

    fn query(&mut self, query: &mut SelectQuery, visit: bool) -> Result<Option<Vec<String>>> {
        self.ctes.push(FxHashMap::default());
        for cte in &mut query.ctes {
            if query.recursive {
                let columns = if cte.column_aliases.is_empty() {
                    self.body_columns(&mut cte.body.clone())?
                } else {
                    Some(cte.column_aliases.clone())
                };
                self.ctes
                    .last_mut()
                    .unwrap()
                    .insert(cte.name.to_ascii_lowercase(), columns);
            }
            let mut columns = self.body(&mut cte.body, visit)?;
            if let Some(columns) = &mut columns {
                for (column, alias) in columns.iter_mut().zip(&cte.column_aliases) {
                    *column = alias.clone();
                }
            }
            self.ctes
                .last_mut()
                .unwrap()
                .insert(cte.name.to_ascii_lowercase(), columns);
        }
        let result = self.body(&mut query.body, visit);
        self.ctes.pop();
        result
    }

    fn body_columns(&mut self, body: &mut QueryBody) -> Result<Option<Vec<String>>> {
        match body {
            QueryBody::Compound(compound) => self.body_columns(&mut compound.left),
            _ => self.body(body, false),
        }
    }

    fn body(&mut self, body: &mut QueryBody, visit: bool) -> Result<Option<Vec<String>>> {
        match body {
            QueryBody::Select(query) => self.select_impl(query, visit),
            QueryBody::Compound(compound) => {
                let columns = self.body(&mut compound.left, visit)?;
                self.body(&mut compound.right, visit)?;
                if visit {
                    let mut scope = Scope::default();
                    scope.add("", columns.as_deref());
                    self.scopes.push(scope);
                    for order in &mut compound.order_by {
                        self.expr(&mut order.expr)?;
                    }
                    for expr in [&mut compound.limit, &mut compound.offset]
                        .into_iter()
                        .flatten()
                    {
                        self.expr(expr)?;
                    }
                    self.scopes.pop();
                }
                Ok(columns)
            }
            QueryBody::Insert(_) | QueryBody::Update(_) | QueryBody::Delete(_) => Err(
                SqlError::Unsupported("DML derived source in a mutation predicate".into()),
            ),
        }
    }

    fn derived(&mut self, derived: &mut DerivedTable, visit: bool) -> Result<Option<Vec<String>>> {
        // Non-lateral FROM items cannot capture sibling sources. Enclosing
        // query scopes remain visible, as do the mutation's outer columns.
        let sibling_scope = (!derived.lateral).then(|| self.scopes.pop().unwrap());
        let result = self.query(&mut derived.query, visit);
        if let Some(scope) = sibling_scope {
            self.scopes.push(scope);
        }
        result
    }

    fn add_source(
        &mut self,
        qualifier: &str,
        columns: Option<Vec<String>>,
        joined: bool,
        output: &mut Option<Vec<String>>,
    ) {
        self.scopes
            .last_mut()
            .unwrap()
            .add(qualifier, columns.as_deref());
        match (output.as_mut(), columns) {
            (Some(output), Some(columns)) => output.extend(columns.into_iter().map(|name| {
                if joined {
                    format!("{qualifier}.{name}")
                } else {
                    name
                }
            })),
            _ => *output = None,
        }
    }

    fn select(&mut self, query: &mut SelectStmt) -> Result<()> {
        self.select_impl(query, true).map(|_| ())
    }

    fn select_impl(&mut self, query: &mut SelectStmt, visit: bool) -> Result<Option<Vec<String>>> {
        check_cancel(self.cancel.as_ref())?;
        self.query_depth += 1;
        self.scopes.push(Scope::default());
        let joined = !query.joins.is_empty();
        let mut source_columns = Some(Vec::new());
        let (qualifier, columns) = if let Some(derived) = &mut query.from_subquery {
            (derived.alias.clone(), self.derived(derived, visit)?)
        } else if let Some(args) = &mut query.from_args {
            if visit {
                for arg in args.iter_mut() {
                    self.expr(arg)?;
                }
            }
            (
                query.from_alias.as_ref().unwrap_or(&query.from).clone(),
                self.function_columns(&query.from, args)?,
            )
        } else if let Some(spec) = &mut query.from_json_table {
            if visit {
                self.expr(&mut spec.source)?;
            }
            fn names(columns: &[JsonTableCol], out: &mut Vec<String>) {
                for column in columns {
                    match column {
                        JsonTableCol::Named { name, .. } | JsonTableCol::Ordinality { name } => {
                            out.push(name.clone())
                        }
                        JsonTableCol::Nested { columns, .. } => names(columns, out),
                    }
                }
            }
            let mut columns = Vec::new();
            names(&spec.columns, &mut columns);
            (
                query.from_alias.as_ref().unwrap_or(&query.from).clone(),
                Some(columns),
            )
        } else {
            (
                query.from_alias.as_ref().unwrap_or(&query.from).clone(),
                self.relation_columns(&query.from)?,
            )
        };
        self.add_source(&qualifier, columns, joined, &mut source_columns);
        for join in &mut query.joins {
            let (qualifier, columns) = if let Some(derived) = &mut join.subquery {
                (derived.alias.clone(), self.derived(derived, visit)?)
            } else if let Some(args) = &mut join.table.args {
                if visit {
                    for arg in args.iter_mut() {
                        self.expr(arg)?;
                    }
                }
                (
                    join.table
                        .alias
                        .as_ref()
                        .unwrap_or(&join.table.name)
                        .clone(),
                    self.function_columns(&join.table.name, args)?,
                )
            } else {
                (
                    join.table
                        .alias
                        .as_ref()
                        .unwrap_or(&join.table.name)
                        .clone(),
                    self.relation_columns(&join.table.name)?,
                )
            };
            self.add_source(&qualifier, columns, joined, &mut source_columns);
            // ON sees this join and its preceding sources, never later joins.
            if visit {
                if let Some(expr) = &mut join.on_clause {
                    self.expr(expr)?;
                }
            }
        }
        let mut output = Some(Vec::new());
        for column in &mut query.columns {
            match column {
                SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                    match (output.as_mut(), &source_columns) {
                        (Some(output), Some(source)) => output.extend(source.iter().cloned()),
                        _ => output = None,
                    }
                }
                SelectColumn::Expr { expr, alias } => {
                    let name = alias
                        .clone()
                        .unwrap_or_else(|| super::super::helpers::expr_display_name(expr));
                    if let Some(output) = &mut output {
                        output.push(name.clone());
                    }
                    if visit {
                        self.expr(expr)?;
                        // Substitution must not rename a derived output column.
                        if self.row.is_some()
                            && alias.is_none()
                            && super::super::helpers::expr_display_name(expr) != name
                        {
                            *alias = Some(name);
                        }
                    }
                }
            }
        }
        if visit {
            for expr in [&mut query.where_clause, &mut query.limit, &mut query.offset]
                .into_iter()
                .flatten()
            {
                self.expr(expr)?;
            }
            // Output aliases are available to grouping, HAVING and ordering,
            // not to WHERE, FROM or LIMIT.
            if let Some(output) = &output {
                for name in output {
                    self.scopes
                        .last_mut()
                        .unwrap()
                        .columns
                        .insert(name.to_ascii_lowercase());
                }
            }
            for expr in &mut query.group_by {
                self.expr(expr)?;
            }
            if let Some(expr) = &mut query.having {
                self.expr(expr)?;
            }
            for order in &mut query.order_by {
                self.expr(&mut order.expr)?;
            }
        }
        self.scopes.pop();
        self.query_depth -= 1;
        Ok(output)
    }

    fn expr(&mut self, expr: &mut Expr) -> Result<()> {
        check_cancel(self.cancel.as_ref())?;
        let outer_column = if self.query_depth == 0 {
            None
        } else {
            match expr {
                Expr::Column(name) => {
                    let lower = name.to_ascii_lowercase();
                    if self
                        .scopes
                        .iter()
                        .rev()
                        .any(|scope| scope.columns.contains(&lower))
                    {
                        None
                    } else {
                        let outer = self.outer.column_index(name);
                        if outer.is_some() && self.scopes.iter().any(|scope| scope.unknown_columns)
                        {
                            return Err(SqlError::Unsupported(format!(
                                "cannot resolve bare column '{name}' between an outer row and a source without static columns; qualify the column"
                            )));
                        }
                        outer
                    }
                }
                Expr::QualifiedColumn { table, column }
                    if !self
                        .scopes
                        .iter()
                        .rev()
                        .any(|scope| scope.qualifiers.contains(&table.to_ascii_lowercase()))
                        && (table.eq_ignore_ascii_case(&self.outer.name)
                            || self
                                .alias
                                .is_some_and(|alias| table.eq_ignore_ascii_case(alias))) =>
                {
                    Some(
                        self.outer
                            .column_index(column)
                            .ok_or_else(|| SqlError::ColumnNotFound(format!("{table}.{column}")))?,
                    )
                }
                _ => None,
            }
        };
        if let Some(index) = outer_column {
            self.bound = true;
            if let Some(row) = self.row {
                let value = row.get(index).ok_or_else(|| {
                    SqlError::InvalidValue("outer correlation row does not match its schema".into())
                })?;
                *expr = Expr::BoundColumn {
                    value: value.clone(),
                    collation: self.outer.columns[index].collation,
                };
            }
            return Ok(());
        }
        match expr {
            Expr::BinaryOp { left, right, .. } | Expr::IsDistinctFrom { left, right, .. } => {
                self.expr(left)?;
                self.expr(right)?;
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::InSet { expr, .. } => self.expr(expr)?,
            Expr::Function { args, .. } | Expr::Coalesce(args) | Expr::ArrayLiteral(args) => {
                for expr in args {
                    self.expr(expr)?;
                }
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.expr(expr)?;
                self.select(subquery)?;
            }
            Expr::Exists { subquery, .. } | Expr::ScalarSubquery(subquery) => {
                self.select(subquery)?
            }
            Expr::InList { expr, list, .. } => {
                self.expr(expr)?;
                for expr in list {
                    self.expr(expr)?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr)?;
                self.expr(low)?;
                self.expr(high)?;
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.expr(expr)?;
                self.expr(pattern)?;
                if let Some(expr) = escape {
                    self.expr(expr)?;
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(expr) = operand {
                    self.expr(expr)?;
                }
                for (condition, value) in conditions {
                    self.expr(condition)?;
                    self.expr(value)?;
                }
                if let Some(expr) = else_result {
                    self.expr(expr)?;
                }
            }
            Expr::WindowFunction { args, spec, .. } => {
                for expr in args {
                    self.expr(expr)?;
                }
                for expr in &mut spec.partition_by {
                    self.expr(expr)?;
                }
                for order in &mut spec.order_by {
                    self.expr(&mut order.expr)?;
                }
                if let Some(frame) = &mut spec.frame {
                    for bound in [&mut frame.start, &mut frame.end] {
                        if let WindowFrameBound::Preceding(expr)
                        | WindowFrameBound::Following(expr) = bound
                        {
                            self.expr(expr)?;
                        }
                    }
                }
            }
            Expr::Quantified { left, right, .. } => {
                self.expr(left)?;
                match right {
                    QuantifiedRhs::Subquery(query) => self.select(query)?,
                    QuantifiedRhs::Array(expr) => self.expr(expr)?,
                }
            }
            Expr::BoundColumn { .. }
            | Expr::Literal(_)
            | Expr::Column(_)
            | Expr::QualifiedColumn { .. }
            | Expr::CountStar
            | Expr::Parameter(_)
            | Expr::TypedNullRecord(_) => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn predicate(sql: &str) -> Expr {
        let Statement::Update(update) =
            crate::parser::parse_sql(&format!("UPDATE outer_t SET n = 0 WHERE {sql}")).unwrap()
        else {
            panic!("expected UPDATE");
        };
        update.where_clause.unwrap()
    }

    #[test]
    fn detect_and_bind_share_lexical_sources_without_materializing_them() {
        let db = citadel::DatabaseBuilder::new("")
            .passphrase(b"test-passphrase")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE outer_t (id TEXT COLLATE NOCASE PRIMARY KEY, n INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE inner_t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE VIEW named_source AS SELECT id AS n FROM inner_t")
            .unwrap();
        let schema = SchemaManager::load(&db).unwrap();
        let ctx = CorrelationCtx {
            outer_schema: schema.get("outer_t").unwrap(),
            outer_alias: None,
        };
        let mut txn = db.begin_write().unwrap();
        // Even view/derived discovery must not materialize catalog or data
        // values from this transaction just to classify a predicate.
        let budget = citadel_txn::ReadBudget::new(0, 0);
        txn.set_read_budget(Some(budget.clone()));
        let row = [Value::Text("AbC".into()), Value::Integer(3)];
        for (sql, expected) in [
            ("EXISTS (SELECT 1)", false),
            ("EXISTS (SELECT 1 WHERE outer_t.n > 0)", true),
            ("EXISTS (SELECT n FROM inner_t)", true),
            ("EXISTS (SELECT outer_t.id FROM inner_t AS outer_t)", false),
            (
                "EXISTS (SELECT 1 FROM inner_t i WHERE EXISTS (SELECT 1 WHERE i.id = outer_t.n))",
                true,
            ),
            (
                "EXISTS (SELECT 1 FROM inner_t i JOIN inner_t j ON j.id = outer_t.n)",
                true,
            ),
            (
                "EXISTS (SELECT COUNT(*) FROM inner_t HAVING COUNT(*) > outer_t.n)",
                true,
            ),
            (
                "EXISTS (SELECT id FROM inner_t ORDER BY outer_t.n LIMIT outer_t.n)",
                true,
            ),
            (
                "EXISTS (SELECT d.n FROM (SELECT id AS n FROM inner_t) d WHERE d.n > 0)",
                false,
            ),
            ("EXISTS (SELECT d.n FROM (SELECT outer_t.n AS n) d)", true),
            ("EXISTS (SELECT n FROM named_source)", false),
            (
                "EXISTS (SELECT value FROM json_array_elements('[1]'))",
                false,
            ),
            (
                "EXISTS (SELECT value FROM json_array_elements(outer_t.id))",
                true,
            ),
            ("EXISTS (SELECT key FROM json_object_keys('{}'))", false),
        ] {
            let mut expr = predicate(sql);
            let original = format!("{expr:?}");
            assert_eq!(
                bind_predicate(&mut txn, &schema, &mut expr, &ctx, None).unwrap(),
                expected,
                "{sql}"
            );
            assert_eq!(format!("{expr:?}"), original, "detection mutated {sql}");
            assert_eq!(
                bind_predicate(&mut txn, &schema, &mut expr, &ctx, Some(&row)).unwrap(),
                expected,
                "{sql}"
            );
        }
        assert_eq!(budget.remaining(), 0);
    }

    #[test]
    fn bound_columns_retain_implicit_collation_and_derived_output_name() {
        let db = citadel::DatabaseBuilder::new("")
            .passphrase(b"test-passphrase")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE outer_t (id TEXT COLLATE NOCASE PRIMARY KEY, n INTEGER)")
            .unwrap();
        let schema = SchemaManager::load(&db).unwrap();
        let ctx = CorrelationCtx {
            outer_schema: schema.get("outer_t").unwrap(),
            outer_alias: None,
        };
        let mut txn = db.begin_write().unwrap();
        let mut expr = predicate("EXISTS (SELECT id)");
        let row = [Value::Text("AbC".into()), Value::Integer(3)];
        assert!(bind_predicate(&mut txn, &schema, &mut expr, &ctx, Some(&row)).unwrap());
        let Expr::Exists { subquery, .. } = expr else {
            panic!("expected EXISTS")
        };
        assert!(matches!(&subquery.columns[0], SelectColumn::Expr {
            expr: Expr::BoundColumn { value: Value::Text(value), collation: Collation::NoCase },
            alias: Some(alias),
        } if value.as_str() == "AbC" && alias == "id"));
    }
}
