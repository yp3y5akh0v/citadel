//! Differential tests against SQLite over the SQL both engines share.
//!
//! Each seed builds the same schema and rows in both engines, then compares
//! generated queries, run through Citadel's execute and prepared paths and
//! inside a write transaction, or generated data changes, applied both
//! autocommit and inside one transaction, with the tables compared after
//! every change.
//!
//! Generated SQL stays inside the shared semantics: integer and
//! binary-collated text values, divisors guarded by NULLIF, NOCASE columns
//! only in predicates, and ORDER BY over every output column so each result
//! has one correct order. An aggregate in a subquery always reads a column of
//! the subquery's own source: SQLite gives one that reads only outer columns
//! to the outer query, which Citadel does not. SQLite runs without its
//! EXISTS-to-join rewrite, which returns wrong rows in 3.51.3. SQLite
//! evaluates UPDATE SET subqueries against rows the statement has already
//! changed, so they read only the other table.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ty {
    Int,
    Text,
}

#[derive(Clone)]
struct Col {
    sql: String,
    ty: Ty,
    /// NOCASE text: comparable, but never an output value, whose spelling
    /// for case-insensitive ties differs between the engines.
    nocase: bool,
}

struct Table {
    name: &'static str,
    cols: &'static [(&'static str, Ty, bool)],
}

const T1: Table = Table {
    name: "t1",
    cols: &[
        ("id", Ty::Int, false),
        ("a", Ty::Int, false),
        ("b", Ty::Int, false),
        ("c", Ty::Text, false),
        ("d", Ty::Text, true),
    ],
};

const T2: Table = Table {
    name: "t2",
    cols: &[
        ("id", Ty::Int, false),
        ("a", Ty::Int, false),
        ("e", Ty::Int, false),
        ("f", Ty::Text, false),
    ],
};

const TABLES: [&Table; 2] = [&T1, &T2];

const TEXTS: &[&str] = &["", "a", "b", "ab", "ba", "abc", "B", "A b", "b_a", "a%"];
const NOCASE_TEXTS: &[&str] = &["", "x", "X", "xy", "XY", "Xy", "y", "Y"];
const LIKE_PATTERNS: &[&str] = &["a%", "%b", "_", "%", "a_%", "%a%", "A%", "b", "%\\_%"];

const INDEXES: &[&str] = &[
    "CREATE INDEX t1_a ON t1 (a)",
    "CREATE INDEX t1_c ON t1 (c)",
    "CREATE INDEX t1_ab ON t1 (a, b)",
    "CREATE INDEX t1_d ON t1 (d)",
    "CREATE INDEX t1_b_pos ON t1 (b) WHERE a > 0",
    "CREATE INDEX t2_a ON t2 (a)",
    "CREATE INDEX t2_e_f ON t2 (e, f)",
];

fn scope(table: &Table, alias: &str) -> Vec<Col> {
    table
        .cols
        .iter()
        .map(|&(name, ty, nocase)| Col {
            sql: format!("{alias}.{name}"),
            ty,
            nocase,
        })
        .collect()
}

fn text_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

struct Gen {
    rng: Rng,
    aliases: usize,
    /// Tables a subquery may read.
    sources: &'static [&'static Table],
}

impl Gen {
    fn new(rng: Rng) -> Self {
        Self {
            rng,
            aliases: 0,
            sources: &TABLES,
        }
    }

    fn alias(&mut self, prefix: &str) -> String {
        self.aliases += 1;
        format!("{prefix}{}", self.aliases)
    }

    fn int_literal(&mut self) -> String {
        (self.rng.below(11) as i64 - 5).to_string()
    }

    fn column(&mut self, scope: &[Col], ty: Ty, allow_nocase: bool) -> Option<String> {
        let candidates: Vec<&Col> = scope
            .iter()
            .filter(|col| col.ty == ty && (allow_nocase || !col.nocase))
            .collect();
        (!candidates.is_empty()).then(|| self.rng.pick(&candidates).sql.clone())
    }

    fn int_expr(&mut self, scope: &[Col], depth: u32) -> String {
        if depth == 0 || self.rng.one_in(3) {
            return match self.rng.below(6) {
                0 => self.int_literal(),
                1 if self.rng.one_in(3) => "NULL".into(),
                _ => self
                    .column(scope, Ty::Int, false)
                    .unwrap_or_else(|| self.int_literal()),
            };
        }
        let depth = depth - 1;
        match self.rng.below(11) {
            0 => format!(
                "({} + {})",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            1 => format!(
                "({} - {})",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            2 => format!(
                "({} * {})",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            3 => format!(
                "({} / NULLIF({}, 0))",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            4 => format!(
                "({} % NULLIF({}, 0))",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            5 => format!("ABS({})", self.int_expr(scope, depth)),
            6 => format!(
                "COALESCE({}, {})",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            7 => format!(
                "NULLIF({}, {})",
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            8 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.predicate(scope, depth),
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            9 => format!(
                "IIF({}, {}, {})",
                self.predicate(scope, depth),
                self.int_expr(scope, depth),
                self.int_expr(scope, depth)
            ),
            _ => format!("LENGTH({})", self.text_expr(scope, depth)),
        }
    }

    /// Binary-collated text only; NOCASE columns never become values.
    fn text_expr(&mut self, scope: &[Col], depth: u32) -> String {
        if depth == 0 || self.rng.one_in(3) {
            return match self.rng.below(6) {
                0 => text_literal(self.rng.pick(TEXTS)),
                1 if self.rng.one_in(3) => "NULL".into(),
                _ => self
                    .column(scope, Ty::Text, false)
                    .unwrap_or_else(|| text_literal(self.rng.pick(TEXTS))),
            };
        }
        let depth = depth - 1;
        match self.rng.below(8) {
            0 => format!("UPPER({})", self.text_expr(scope, depth)),
            1 => format!("LOWER({})", self.text_expr(scope, depth)),
            2 => format!(
                "({} || {})",
                self.text_expr(scope, depth),
                self.text_expr(scope, depth)
            ),
            3 => format!(
                "SUBSTR({}, {}, {})",
                self.text_expr(scope, depth),
                1 + self.rng.below(2),
                1 + self.rng.below(3)
            ),
            4 => format!(
                "COALESCE({}, {})",
                self.text_expr(scope, depth),
                self.text_expr(scope, depth)
            ),
            5 => format!("REPLACE({}, 'a', 'z')", self.text_expr(scope, depth)),
            6 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.predicate(scope, depth),
                self.text_expr(scope, depth),
                self.text_expr(scope, depth)
            ),
            _ => format!("TRIM({})", self.text_expr(scope, depth)),
        }
    }

    fn expr(&mut self, ty: Ty, scope: &[Col], depth: u32) -> String {
        match ty {
            Ty::Int => self.int_expr(scope, depth),
            Ty::Text => self.text_expr(scope, depth),
        }
    }

    fn comparison(&mut self) -> &'static str {
        self.rng.pick(&["=", "<>", "<", "<=", ">", ">="])
    }

    fn predicate(&mut self, scope: &[Col], depth: u32) -> String {
        if depth == 0 || self.rng.one_in(3) {
            return self.simple_predicate(scope);
        }
        let depth = depth - 1;
        match self.rng.below(7) {
            0 => format!("NOT ({})", self.predicate(scope, depth)),
            1 | 2 => format!(
                "({} AND {})",
                self.predicate(scope, depth),
                self.predicate(scope, depth)
            ),
            3 | 4 => format!(
                "({} OR {})",
                self.predicate(scope, depth),
                self.predicate(scope, depth)
            ),
            5 => self.exists_predicate(scope, depth),
            _ => self.in_subquery_predicate(scope, depth),
        }
    }

    fn simple_predicate(&mut self, scope: &[Col]) -> String {
        match self.rng.below(9) {
            0 => {
                let left = self.int_expr(scope, 1);
                let right = self.int_expr(scope, 1);
                format!("{left} {} {right}", self.comparison())
            }
            1 => {
                let left = self.text_expr(scope, 1);
                let right = self.text_expr(scope, 1);
                format!("{left} {} {right}", self.comparison())
            }
            2 => match self.column(scope, Ty::Text, true) {
                Some(col) if scope.iter().any(|c| c.sql == col && c.nocase) => {
                    let literal = text_literal(self.rng.pick(NOCASE_TEXTS));
                    format!("{col} {} {literal}", self.comparison())
                }
                _ => {
                    let value = self.text_expr(scope, 1);
                    format!(
                        "{value} LIKE {}",
                        text_literal(self.rng.pick(LIKE_PATTERNS))
                    )
                }
            },
            3 => {
                let ty = if self.rng.one_in(2) {
                    Ty::Int
                } else {
                    Ty::Text
                };
                let value = self.expr(ty, scope, 1);
                let not = if self.rng.one_in(2) { " NOT" } else { "" };
                format!("{value} IS{not} NULL")
            }
            4 => {
                let value = self.int_expr(scope, 1);
                let low = self.int_literal();
                let high = self.int_literal();
                let not = if self.rng.one_in(3) { " NOT" } else { "" };
                format!("{value}{not} BETWEEN {low} AND {high}")
            }
            5 => {
                let value = self.int_expr(scope, 1);
                let items: Vec<String> = (0..1 + self.rng.below(3))
                    .map(|_| {
                        if self.rng.one_in(5) {
                            "NULL".into()
                        } else {
                            self.int_literal()
                        }
                    })
                    .collect();
                let not = if self.rng.one_in(3) { " NOT" } else { "" };
                format!("{value}{not} IN ({})", items.join(", "))
            }
            6 => {
                let value = self.text_expr(scope, 1);
                let not = if self.rng.one_in(3) { " NOT" } else { "" };
                format!(
                    "{value}{not} LIKE {}",
                    text_literal(self.rng.pick(LIKE_PATTERNS))
                )
            }
            7 => match self.column(scope, Ty::Text, true) {
                Some(col) if scope.iter().any(|c| c.sql == col && c.nocase) => {
                    let items: Vec<String> = (0..1 + self.rng.below(2))
                        .map(|_| text_literal(self.rng.pick(NOCASE_TEXTS)))
                        .collect();
                    format!("{col} IN ({})", items.join(", "))
                }
                _ => {
                    let left = self.int_expr(scope, 0);
                    format!("{left} = {}", self.int_literal())
                }
            },
            _ => {
                let left = self.int_expr(scope, 0);
                let right = self.int_expr(scope, 0);
                format!("{left} {} {right}", self.comparison())
            }
        }
    }

    /// A subquery over a base table, correlated with the outer scope about
    /// half of the time.
    fn subquery_source(&mut self, outer: &[Col]) -> (String, Vec<Col>, String) {
        let table = self.rng.pick(self.sources);
        let alias = self.alias("s");
        let inner = scope(table, &alias);
        let mut filters = Vec::new();
        if self.rng.one_in(2) {
            if let Some(outer_col) = self.column(outer, Ty::Int, false) {
                let inner_col = self.column(&inner, Ty::Int, false).unwrap();
                filters.push(format!("{inner_col} {} {outer_col}", self.comparison()));
            }
        }
        if self.rng.one_in(2) {
            filters.push(self.predicate(&inner, 1));
        }
        let from = format!("{} AS {alias}", table.name);
        let filter = if filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", filters.join(" AND "))
        };
        (from, inner, filter)
    }

    fn exists_predicate(&mut self, scope: &[Col], _depth: u32) -> String {
        let (from, _, filter) = self.subquery_source(scope);
        let not = if self.rng.one_in(3) { "NOT " } else { "" };
        format!("{not}EXISTS (SELECT 1 FROM {from}{filter})")
    }

    fn in_subquery_predicate(&mut self, scope: &[Col], _depth: u32) -> String {
        let (from, inner, filter) = self.subquery_source(scope);
        let value = self.int_expr(scope, 1);
        let selected = self.int_expr(&inner, 1);
        let not = if self.rng.one_in(3) { " NOT" } else { "" };
        format!("{value}{not} IN (SELECT {selected} FROM {from}{filter})")
    }

    fn scalar_subquery(&mut self, scope: &[Col]) -> String {
        let (from, inner, filter) = self.subquery_source(scope);
        let aggregate = match self.rng.below(4) {
            0 => "COUNT(*)".to_string(),
            1 => format!("SUM({})", self.int_expr(&inner, 1)),
            2 => format!("MIN({})", self.int_expr(&inner, 1)),
            _ => format!("MAX({})", self.int_expr(&inner, 1)),
        };
        // The FILTER may read the outer row, and always reads the subquery's
        // own: an aggregate that reads only outer columns belongs to the outer
        // query.
        let aggregate_filter = if self.rng.one_in(3) {
            let own = self
                .column(&inner, Ty::Int, false)
                .expect("every source has an integer column");
            let visible: Vec<Col> = inner.iter().chain(scope).cloned().collect();
            format!(
                " FILTER (WHERE {own} {} {} AND {})",
                self.comparison(),
                self.int_literal(),
                self.predicate(&visible, 1)
            )
        } else {
            String::new()
        };
        format!("(SELECT {aggregate}{aggregate_filter} FROM {from}{filter})")
    }

    /// Sometimes a FILTER clause for an aggregate over `scope`.
    fn aggregate_filter(&mut self, scope: &[Col]) -> String {
        if self.rng.one_in(3) {
            format!(" FILTER (WHERE {})", self.predicate(scope, 1))
        } else {
            String::new()
        }
    }

    /// FROM clause and the columns it exposes.
    fn source(&mut self) -> (String, Vec<Col>) {
        match self.rng.below(7) {
            0 | 1 => {
                let table = self.rng.pick(&TABLES);
                let alias = self.alias("q");
                (format!("{} AS {alias}", table.name), scope(table, &alias))
            }
            2 | 3 => {
                let left = self.alias("q");
                let right = self.alias("q");
                let mut cols = scope(&T1, &left);
                cols.extend(scope(&T2, &right));
                let join =
                    self.rng
                        .pick(&["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"]);
                let on = if join == "CROSS JOIN" {
                    String::new()
                } else {
                    let key = self.rng.pick(&["a", "e", "id"]);
                    let mut on = format!(" ON {left}.a = {right}.{key}");
                    if self.rng.one_in(3) {
                        on.push_str(&format!(" AND {}", self.simple_predicate(&cols)));
                    }
                    on
                };
                (format!("t1 AS {left} {join} t2 AS {right}{on}"), cols)
            }
            4 => {
                let table = self.rng.pick(&TABLES);
                let alias = self.alias("q");
                let inner = scope(table, &alias);
                let derived = self.alias("d");
                let int_value = self.int_expr(&inner, 1);
                let text_value = self.text_expr(&inner, 1);
                let filter = self.predicate(&inner, 1);
                let from = format!(
                    "(SELECT {alias}.id AS k, {int_value} AS n, {text_value} AS t \
                     FROM {} AS {alias} WHERE {filter}) AS {derived}",
                    table.name
                );
                let cols = vec![
                    Col {
                        sql: format!("{derived}.k"),
                        ty: Ty::Int,
                        nocase: false,
                    },
                    Col {
                        sql: format!("{derived}.n"),
                        ty: Ty::Int,
                        nocase: false,
                    },
                    Col {
                        sql: format!("{derived}.t"),
                        ty: Ty::Text,
                        nocase: false,
                    },
                ];
                (from, cols)
            }
            _ => {
                let left = self.alias("q");
                let right = self.alias("q");
                let mut cols = scope(&T1, &left);
                cols.extend(scope(&T1, &right));
                (
                    format!("t1 AS {left} JOIN t1 AS {right} ON {left}.b = {right}.a"),
                    cols,
                )
            }
        }
    }

    /// ORDER BY every output column, then possibly LIMIT and OFFSET.
    fn order_by(&mut self, width: usize) -> String {
        let terms: Vec<String> = (1..=width)
            .map(|position| {
                let direction = if self.rng.one_in(2) { " DESC" } else { "" };
                let nulls = match self.rng.below(3) {
                    0 => " NULLS FIRST",
                    1 => " NULLS LAST",
                    _ => "",
                };
                format!("{position}{direction}{nulls}")
            })
            .collect();
        let mut clause = format!(" ORDER BY {}", terms.join(", "));
        if self.rng.one_in(3) {
            clause.push_str(&format!(" LIMIT {}", self.rng.below(8)));
            if self.rng.one_in(2) {
                clause.push_str(&format!(" OFFSET {}", self.rng.below(5)));
            }
        }
        clause
    }

    /// Output expressions and their types.
    fn outputs(&mut self, scope: &[Col], width: usize) -> (Vec<String>, Vec<Ty>) {
        (0..width)
            .map(|_| {
                if self.rng.one_in(6) {
                    (self.scalar_subquery(scope), Ty::Int)
                } else if self.rng.one_in(2) {
                    (self.int_expr(scope, 2), Ty::Int)
                } else {
                    (self.text_expr(scope, 2), Ty::Text)
                }
            })
            .unzip()
    }

    fn select_core(&mut self, types: Option<&[Ty]>) -> (String, Vec<Ty>) {
        let (from, cols) = self.source();
        let width = types.map_or(1 + self.rng.below(3), <[Ty]>::len);
        let (exprs, tys) = match types {
            Some(types) => (
                types
                    .iter()
                    .map(|&ty| self.expr(ty, &cols, 2))
                    .collect::<Vec<_>>(),
                types.to_vec(),
            ),
            None => self.outputs(&cols, width),
        };
        let distinct = if types.is_none() && self.rng.one_in(4) {
            "DISTINCT "
        } else {
            ""
        };
        let filter = if self.rng.one_in(5) {
            String::new()
        } else {
            format!(" WHERE {}", self.predicate(&cols, 2))
        };
        (
            format!("SELECT {distinct}{} FROM {from}{filter}", exprs.join(", ")),
            tys,
        )
    }

    fn aggregate_query(&mut self) -> String {
        let (from, cols) = self.source();
        let keys: Vec<String> = (0..self.rng.below(3))
            .map(|_| {
                if self.rng.one_in(2) {
                    self.column(&cols, Ty::Int, false)
                        .unwrap_or_else(|| self.int_expr(&cols, 1))
                } else {
                    self.text_expr(&cols, 1)
                }
            })
            .collect();
        let aggregates: Vec<String> = (0..1 + self.rng.below(3))
            .map(|_| {
                let aggregate = match self.rng.below(7) {
                    0 => "COUNT(*)".to_string(),
                    1 => format!("COUNT({})", self.int_expr(&cols, 1)),
                    2 => format!("SUM({})", self.int_expr(&cols, 1)),
                    3 => format!("MIN({})", self.expr_of_any_type(&cols)),
                    4 => format!("MAX({})", self.expr_of_any_type(&cols)),
                    5 => format!("COUNT(DISTINCT {})", self.expr_of_any_type(&cols)),
                    _ => format!("AVG({})", self.int_expr(&cols, 1)),
                };
                aggregate + &self.aggregate_filter(&cols)
            })
            .collect();
        let filter = if self.rng.one_in(3) {
            String::new()
        } else {
            format!(" WHERE {}", self.predicate(&cols, 2))
        };
        let mut sql = format!(
            "SELECT {} FROM {from}{filter}",
            keys.iter()
                .chain(&aggregates)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !keys.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", keys.join(", ")));
            if self.rng.one_in(2) {
                let having = match self.rng.below(3) {
                    0 => format!(
                        "COUNT(*){} > {}",
                        self.aggregate_filter(&cols),
                        self.rng.below(3)
                    ),
                    1 => format!(
                        "SUM({}){} >= {}",
                        self.int_expr(&cols, 1),
                        self.aggregate_filter(&cols),
                        self.int_literal()
                    ),
                    _ => format!(
                        "MIN({}){} IS NOT NULL",
                        self.int_expr(&cols, 1),
                        self.aggregate_filter(&cols)
                    ),
                };
                sql.push_str(&format!(" HAVING {having}"));
            }
        }
        let order = self.order_by(keys.len() + aggregates.len());
        sql + &order
    }

    fn expr_of_any_type(&mut self, scope: &[Col]) -> String {
        if self.rng.one_in(2) {
            self.int_expr(scope, 1)
        } else {
            self.text_expr(scope, 1)
        }
    }

    fn query(&mut self) -> String {
        let (body, width) = match self.rng.below(6) {
            0..=2 => {
                let (core, tys) = self.select_core(None);
                (core, tys.len())
            }
            3 | 4 => return self.aggregate_query(),
            _ => {
                let (left, tys) = self.select_core(None);
                let (right, _) = self.select_core(Some(&tys));
                let op = self
                    .rng
                    .pick(&["UNION", "UNION ALL", "INTERSECT", "EXCEPT"]);
                (format!("{left} {op} {right}"), tys.len())
            }
        };
        let order = self.order_by(width);
        body + &order
    }

    /// One data change. New rows take ids from `next_id` upward, above every
    /// id either table holds.
    fn change(&mut self, next_id: i64) -> String {
        let table = self.rng.pick(&TABLES);
        let target = scope(table, table.name);
        match self.rng.below(8) {
            0..=2 => self.update(table, &target),
            3 => format!(
                "DELETE FROM {} WHERE {}",
                table.name,
                self.predicate(&target, 2)
            ),
            4 | 5 => {
                let rows: Vec<String> = (0..1 + self.rng.below(3) as i64)
                    .map(|offset| row_literal(&mut self.rng, table, next_id + offset))
                    .collect();
                format!("INSERT INTO {} VALUES {}", table.name, rows.join(", "))
            }
            _ => {
                let source = self.rng.pick(&TABLES);
                let alias = self.alias("q");
                let cols = scope(source, &alias);
                let values: Vec<String> = table
                    .cols
                    .iter()
                    .map(|&(name, ty, _)| {
                        if name == "id" {
                            format!("{alias}.id + {next_id}")
                        } else {
                            self.expr(ty, &cols, 2)
                        }
                    })
                    .collect();
                let filter = self.predicate(&cols, 2);
                // The limit keeps the tables near their starting size.
                format!(
                    "INSERT INTO {} SELECT {} FROM {} AS {alias} WHERE {filter} \
                     ORDER BY {alias}.id LIMIT {}",
                    table.name,
                    values.join(", "),
                    source.name,
                    1 + self.rng.below(4)
                )
            }
        }
    }

    fn update(&mut self, table: &Table, target: &[Col]) -> String {
        let mut columns: Vec<(&str, Ty)> = table
            .cols
            .iter()
            .filter(|&&(name, ..)| name != "id")
            .map(|&(name, ty, _)| (name, ty))
            .collect();
        let other: &'static [&'static Table] = if table.name == T1.name {
            &[&T2]
        } else {
            &[&T1]
        };
        let saved = std::mem::replace(&mut self.sources, other);
        let assignments: Vec<String> = (0..1 + self.rng.below(2))
            .map(|_| {
                let (name, ty) = columns.remove(self.rng.below(columns.len()));
                let value = if ty == Ty::Int && self.rng.one_in(3) {
                    self.scalar_subquery(target)
                } else {
                    self.expr(ty, target, 2)
                };
                format!("{name} = {value}")
            })
            .collect();
        self.sources = saved;
        let filter = if self.rng.one_in(6) {
            String::new()
        } else {
            format!(" WHERE {}", self.predicate(target, 2))
        };
        format!(
            "UPDATE {} SET {}{filter}",
            table.name,
            assignments.join(", ")
        )
    }
}

#[derive(Debug, Clone)]
enum Cell {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Null, Cell::Null) => true,
            (Cell::Int(a), Cell::Int(b)) => a == b,
            (Cell::Real(a), Cell::Real(b)) => (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0),
            (Cell::Text(a), Cell::Text(b)) => a == b,
            _ => false,
        }
    }
}

fn citadel_cell(value: &Value) -> Result<Cell, String> {
    Ok(match value {
        Value::Null => Cell::Null,
        Value::Integer(value) => Cell::Int(*value),
        Value::Boolean(value) => Cell::Int(i64::from(*value)),
        Value::Real(value) => Cell::Real(*value),
        Value::Text(value) => Cell::Text(value.to_string()),
        other => return Err(format!("value outside the compared types: {other:?}")),
    })
}

type Rows = Vec<Vec<Cell>>;

fn citadel_rows(rows: &[Vec<Value>]) -> Result<Rows, String> {
    rows.iter()
        .map(|row| row.iter().map(citadel_cell).collect())
        .collect()
}

fn sqlite_query(sqlite: &rusqlite::Connection, sql: &str) -> Result<Rows, String> {
    let mut statement = sqlite.prepare(sql).map_err(|error| error.to_string())?;
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| {
                    use rusqlite::types::ValueRef;
                    Ok(match row.get_ref(index)? {
                        ValueRef::Null => Cell::Null,
                        ValueRef::Integer(value) => Cell::Int(value),
                        ValueRef::Real(value) => Cell::Real(value),
                        ValueRef::Text(value) => {
                            Cell::Text(String::from_utf8_lossy(value).into_owned())
                        }
                        ValueRef::Blob(_) => {
                            return Err(rusqlite::Error::InvalidColumnType(
                                index,
                                "value outside the compared types".into(),
                                rusqlite::types::Type::Blob,
                            ))
                        }
                    })
                })
                .collect()
        })
        .and_then(|rows| rows.collect())
        .map_err(|error| error.to_string())
}

fn citadel_query(connection: &Connection<'_>, sql: &str) -> Result<Rows, String> {
    connection
        .query(sql)
        .map_err(|error| error.to_string())
        .and_then(|result| citadel_rows(&result.rows))
}

fn citadel_prepared_query(connection: &Connection<'_>, sql: &str) -> Result<Rows, String> {
    connection
        .prepare(sql)
        .and_then(|statement| statement.query_collect(&[]))
        .map_err(|error| error.to_string())
        .and_then(|result| citadel_rows(&result.rows))
}

fn sqlite_change(sqlite: &rusqlite::Connection, sql: &str) -> Result<u64, String> {
    sqlite
        .execute(sql, [])
        .map(|count| count as u64)
        .map_err(|error| error.to_string())
}

fn citadel_change(connection: &Connection<'_>, sql: &str) -> Result<u64, String> {
    match connection.execute(sql).map_err(|error| error.to_string())? {
        ExecutionResult::RowsAffected(count) => Ok(count),
        other => Err(format!("expected a row count: {other:?}")),
    }
}

fn citadel_prepared_change(connection: &Connection<'_>, sql: &str) -> Result<u64, String> {
    connection
        .prepare(sql)
        .and_then(|statement| statement.execute(&[]))
        .map_err(|error| error.to_string())
}

/// SQLite and two Citadel databases holding the same rows: one for
/// autocommit statements, one for statements inside an explicit transaction.
struct Engines {
    autocommit: citadel::Database,
    transactional: citadel::Database,
    sqlite: rusqlite::Connection,
}

impl Engines {
    fn new(rng: &mut Rng) -> Self {
        let create = || {
            DatabaseBuilder::new("")
                .passphrase(b"differential")
                .argon2_profile(Argon2Profile::Iot)
                .create_in_memory()
                .unwrap()
        };
        let engines = Self {
            autocommit: create(),
            transactional: create(),
            sqlite: rusqlite::Connection::open_in_memory().unwrap(),
        };
        without_exists_to_join(&engines.sqlite);
        let mut setup = vec![
            "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, \
             c TEXT, d TEXT COLLATE NOCASE)"
                .to_string(),
            "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, e INTEGER, f TEXT)"
                .to_string(),
        ];
        setup.extend(
            INDEXES
                .iter()
                .filter(|_| rng.one_in(2))
                .map(|index| index.to_string()),
        );
        setup.push(insert_rows(rng, &T1, 1, 40));
        setup.push(insert_rows(rng, &T2, 1, 30));
        for database in [&engines.autocommit, &engines.transactional] {
            let connection = Connection::open(database).unwrap();
            for sql in &setup {
                connection
                    .execute(sql)
                    .unwrap_or_else(|error| panic!("citadel setup {sql}: {error}"));
            }
        }
        for sql in &setup {
            engines
                .sqlite
                .execute_batch(sql)
                .unwrap_or_else(|error| panic!("sqlite setup {sql}: {error}"));
        }
        engines
    }

    /// One above the largest id either table holds.
    fn next_id(&self) -> i64 {
        self.sqlite
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM (SELECT id FROM t1 UNION ALL SELECT id FROM t2)",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }
}

/// SQLite 3.51.3 rewrites a WHERE EXISTS into a join that returns wrong rows:
/// it counts OFFSET against every match, and drops an outer row when the
/// subquery ORs a comparison on a NOCASE-indexed column with a correlated
/// one. The oracle runs without that rewrite.
fn without_exists_to_join(sqlite: &rusqlite::Connection) {
    const SQLITE_EXISTS_TO_JOIN: std::ffi::c_uint = 0x4000_0000;
    // SAFETY: the handle belongs to a live connection, and this operation
    // only sets which of its optimizations are off.
    let status = unsafe {
        rusqlite::ffi::sqlite3_test_control(
            rusqlite::ffi::SQLITE_TESTCTRL_OPTIMIZATIONS,
            sqlite.handle(),
            SQLITE_EXISTS_TO_JOIN,
        )
    };
    assert_eq!(status, rusqlite::ffi::SQLITE_OK);
}

fn value_literal(rng: &mut Rng, ty: Ty, nocase: bool) -> String {
    if rng.one_in(5) {
        return "NULL".into();
    }
    match (ty, nocase) {
        (Ty::Int, _) => (rng.below(9) as i64 - 4).to_string(),
        (Ty::Text, false) => text_literal(rng.pick(TEXTS)),
        (Ty::Text, true) => text_literal(rng.pick(NOCASE_TEXTS)),
    }
}

fn row_literal(rng: &mut Rng, table: &Table, id: i64) -> String {
    let values: Vec<String> = table
        .cols
        .iter()
        .map(|&(name, ty, nocase)| {
            if name == "id" {
                id.to_string()
            } else {
                value_literal(rng, ty, nocase)
            }
        })
        .collect();
    format!("({})", values.join(", "))
}

fn insert_rows(rng: &mut Rng, table: &Table, first: i64, count: i64) -> String {
    let rows: Vec<String> = (first..first + count)
        .map(|id| row_literal(rng, table, id))
        .collect();
    format!("INSERT INTO {} VALUES {}", table.name, rows.join(", "))
}

#[derive(Default)]
struct Report {
    checked: usize,
    mismatches: Vec<String>,
}

impl Report {
    /// Records a disagreement and returns whether the engines agree.
    fn agree<T: PartialEq + std::fmt::Debug>(
        &mut self,
        path: &str,
        sql: &str,
        actual: &T,
        expected: &T,
    ) -> bool {
        if actual == expected {
            return true;
        }
        self.mismatches.push(format!(
            "{path}: {sql}\n  citadel: {actual:?}\n  sqlite:  {expected:?}"
        ));
        false
    }

    fn compare_query(
        &mut self,
        autocommit: &Connection<'_>,
        transaction: &Connection<'_>,
        sqlite: &rusqlite::Connection,
        sql: &str,
    ) {
        self.checked += 1;
        let expected = sqlite_query(sqlite, sql);
        for (path, actual) in [
            ("execute", citadel_query(autocommit, sql)),
            ("prepared", citadel_prepared_query(autocommit, sql)),
            ("transaction", citadel_query(transaction, sql)),
        ] {
            self.agree(path, sql, &actual, &expected);
        }
    }

    /// Applies one change to every engine, then compares the row counts and
    /// every table. Returns whether all of them agree.
    fn compare_change(
        &mut self,
        autocommit: &Connection<'_>,
        transaction: &Connection<'_>,
        sqlite: &rusqlite::Connection,
        sql: &str,
    ) -> bool {
        self.checked += 1;
        let expected = sqlite_change(sqlite, sql);
        let execute = citadel_change(autocommit, sql);
        let prepared = citadel_prepared_change(transaction, sql);
        let counted = self.agree("execute", sql, &execute, &expected)
            & self.agree("prepared in a transaction", sql, &prepared, &expected);
        counted
            && self.compare_tables("execute", autocommit, sqlite, sql)
                & self.compare_tables("prepared in a transaction", transaction, sqlite, sql)
    }

    fn compare_tables(
        &mut self,
        path: &str,
        connection: &Connection<'_>,
        sqlite: &rusqlite::Connection,
        sql: &str,
    ) -> bool {
        TABLES.iter().fold(true, |agree, table| {
            let read = format!("SELECT * FROM {} ORDER BY id", table.name);
            let context = format!("{sql}\n  then {read}");
            let actual = citadel_query(connection, &read);
            let expected = sqlite_query(sqlite, &read);
            self.agree(path, &context, &actual, &expected) && agree
        })
    }
}

fn run_queries(seed: u64, queries: usize, report: &mut Report) {
    let mut rng = Rng(seed);
    let engines = Engines::new(&mut rng);
    let autocommit = Connection::open(&engines.autocommit).unwrap();
    let transaction = Connection::open(&engines.transactional).unwrap();
    transaction.execute("BEGIN").unwrap();
    let mut generator = Gen::new(rng);
    for _ in 0..queries {
        let sql = generator.query();
        report.compare_query(&autocommit, &transaction, &engines.sqlite, &sql);
    }
    transaction.execute("ROLLBACK").unwrap();
}

/// Stops a seed at its first disagreement: every later statement would start
/// from different rows.
fn run_changes(seed: u64, statements: usize, report: &mut Report) {
    let mut rng = Rng(seed);
    let engines = Engines::new(&mut rng);
    let autocommit = Connection::open(&engines.autocommit).unwrap();
    let transaction = Connection::open(&engines.transactional).unwrap();
    transaction.execute("BEGIN").unwrap();
    let mut generator = Gen::new(rng);
    for _ in 0..statements {
        let sql = generator.change(engines.next_id());
        if !report.compare_change(&autocommit, &transaction, &engines.sqlite, &sql) {
            return;
        }
    }
    transaction.execute("COMMIT").unwrap();
    report.compare_tables(
        "committed transaction",
        &transaction,
        &engines.sqlite,
        "COMMIT",
    );
}

fn assert_clean(report: &Report) {
    assert!(report.checked > 0);
    assert!(
        report.mismatches.is_empty(),
        "{} of {} statements disagree with SQLite; first:\n{}",
        report.mismatches.len(),
        report.checked,
        report.mismatches[..report.mismatches.len().min(8)].join("\n")
    );
}

#[test]
fn generated_queries_match_sqlite() {
    let mut report = Report::default();
    for seed in 0..6 {
        run_queries(0xD1FF_0000 + seed, 250, &mut report);
    }
    assert_clean(&report);
}

#[test]
fn generated_changes_match_sqlite() {
    let mut report = Report::default();
    for seed in 0..6 {
        run_changes(0xC4A6_0000 + seed, 80, &mut report);
    }
    assert_clean(&report);
}
