//! Mutation fuzzing of submitted SQL: every statement a seeded mutator makes
//! from a corpus of valid ones must parse and run to a result or an error,
//! never to a panic.
//!
//! Each seed runs its mutated statements on one connection to a database
//! built from `SCHEMA`, so transactions the mutator opens stay open for the
//! statements after them, and starts over on a fresh database every few
//! hundred statements so the mutated DDL does not pile up. A panic is
//! recorded with its message, seed and statement, and the run goes on; the
//! test fails listing each distinct panic. `CITADEL_SQL_FUZZ_SEEDS=<n>` runs
//! `n` seeds instead of the default for a longer search.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::Connection;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

const DEFAULT_SEEDS: u64 = 4;
const STATEMENTS_PER_SEED: usize = 1_500;
const STATEMENTS_PER_DATABASE: usize = 400;

const SCHEMA: &[&str] = &[
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d JSON, e DATE, \
     f TIMESTAMP, g INTERVAL, h BOOLEAN)",
    "CREATE TABLE u (id INTEGER PRIMARY KEY, k INTEGER, name TEXT COLLATE NOCASE)",
    "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, v VECTOR(3))",
    "CREATE INDEX t_a ON t (a)",
    "CREATE INDEX docs_body ON docs USING fts (body)",
    "INSERT INTO t VALUES \
     (1, 10, 'x', 1.5, '{\"k\":[1,2]}', '2024-01-01', '2024-01-01 10:00:00', INTERVAL '1 day', true), \
     (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
     (3, -5, 'yy', 2.5, '[1,2,3]', '2023-12-31', '2023-12-31 23:59:59', INTERVAL '2 hours', false)",
    "INSERT INTO u VALUES (10, 1, 'A'), (20, 3, 'b')",
    "INSERT INTO docs VALUES (1, 'the quick brown fox', '[1, 0, 0]'::VECTOR(3)), \
     (2, 'lazy dogs sleep', '[0, 1, 0]'::VECTOR(3))",
    "CREATE VIEW v AS SELECT id, a FROM t WHERE a > 0",
];

const CORPUS: &[&str] = &[
    "SELECT * FROM t WHERE a > 5 ORDER BY id LIMIT 2 OFFSET 1",
    "SELECT a, COUNT(*), SUM(c), AVG(a), MIN(b), MAX(e) FROM t GROUP BY a HAVING COUNT(*) > 0 ORDER BY 1",
    "SELECT t.id, u.name FROM t LEFT JOIN u ON u.k = t.id WHERE u.name IS NOT NULL",
    "SELECT t.id FROM t RIGHT JOIN u ON u.k = t.id FULL JOIN docs ON docs.id = t.id",
    "SELECT id, ROW_NUMBER() OVER (PARTITION BY a ORDER BY id), \
     SUM(a) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    "SELECT id, LAG(a, 1, 0) OVER w, NTILE(2) OVER w, FIRST_VALUE(b) OVER w FROM t WINDOW w AS (ORDER BY id)",
    "SELECT id, AVG(c) OVER (ORDER BY f RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM t",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r",
    "WITH x AS (SELECT a FROM t), y AS (SELECT k FROM u) SELECT * FROM x JOIN y ON x.a = y.k",
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.id) AND a IN (SELECT k FROM u)",
    "SELECT (SELECT MAX(k) FROM u WHERE u.k <= t.id), (SELECT COUNT(*) FROM docs) FROM t",
    "SELECT c.id, d.x FROM t AS c, LATERAL (SELECT c.a * 2 AS x) AS d",
    "SELECT c.id, d.n FROM t AS c LEFT JOIN LATERAL (SELECT COUNT(*) AS n FROM u WHERE u.k = c.id) AS d ON true",
    "SELECT d->'k'->>0, d #>> '{k,1}', jsonb_array_length(d->'k'), d::JSONB @> '{\"k\":[1]}' FROM t",
    "SELECT * FROM json_array_elements('[1,2,{\"a\":3}]'::JSON)",
    "SELECT key, value FROM jsonb_each(CAST('{\"a\":1,\"b\":[2]}' AS JSONB))",
    "SELECT * FROM JSON_TABLE(CAST('[{\"a\":1}]' AS JSONB), '$[*]' COLUMNS (a INT PATH '$.a')) AS jt",
    "SELECT jsonb_path_query(d::JSONB, '$.k[*] ? (@ > 1)'), jsonb_path_exists(d::JSONB, '$.k') FROM t",
    "SELECT json_agg(a), jsonb_build_object('b', b), json_object_agg(id, a) FROM t",
    "SELECT CASE WHEN a > 0 THEN 'pos' WHEN a < 0 THEN 'neg' ELSE 'zero' END, COALESCE(b, 'none'), \
     NULLIF(a, 10) FROM t",
    "SELECT CAST(a AS TEXT) || b, UPPER(b), SUBSTR(b, 1, 1), LENGTH(b), ABS(a), ROUND(c, 1), \
     REPLACE(b, 'x', 'z'), TRIM(b) FROM t",
    "SELECT e + INTERVAL '1 month', f - g, EXTRACT(YEAR FROM e), DATE_TRUNC('day', f), \
     f AT TIME ZONE 'UTC', AGE(f, TIMESTAMP '2000-01-01') FROM t",
    "SELECT date_bin(INTERVAL '15 minutes', f, TIMESTAMP '2001-01-01'), justify_hours(g) FROM t",
    "SELECT a FROM t UNION SELECT k FROM u INTERSECT SELECT a FROM t EXCEPT SELECT 99",
    "SELECT DISTINCT b FROM t ORDER BY b NULLS LAST",
    "SELECT a BETWEEN 1 AND 20, b LIKE 'x%', b ILIKE 'Y%', a IS DISTINCT FROM NULL FROM t",
    "SELECT a = ANY (SELECT k FROM u), a > ALL (SELECT k FROM u), a = ANY (ARRAY[1, 10]) FROM t",
    "SELECT string_agg(b, ',' ORDER BY b), COUNT(*) FILTER (WHERE a > 0), COUNT(DISTINCT a) FROM t",
    "SELECT id FROM docs WHERE body @@ to_tsquery('quick & fox') ORDER BY id",
    "SELECT id, v <-> '[1, 0, 0]'::VECTOR(3) AS dist FROM docs ORDER BY dist LIMIT 1",
    "INSERT INTO u (id, k, name) VALUES (30, 5, 'c') ON CONFLICT (id) DO UPDATE SET k = excluded.k + 1 \
     RETURNING *",
    "INSERT INTO u (id, k) VALUES (40, 6) ON CONFLICT DO NOTHING",
    "UPDATE t SET a = a + 1, b = b || '!' WHERE id IN (SELECT k FROM u) RETURNING id, a",
    "UPDATE t SET a = (SELECT MAX(k) FROM u WHERE u.k < t.id)",
    "DELETE FROM u WHERE k > (SELECT AVG(a) FROM t) RETURNING name",
    "INSERT INTO t (id, a) SELECT id + 100, k FROM u",
    "INSERT INTO docs (id, body, v) VALUES (9, 'fox runs', '[0, 0, 1]'::VECTOR(3))",
    "EXPLAIN SELECT * FROM t JOIN u ON u.k = t.a WHERE t.a > 1",
    "EXPLAIN ANALYZE SELECT a, COUNT(*) FROM t GROUP BY a",
    "SELECT * FROM v WHERE a < 100",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT s1",
    "ROLLBACK TO s1",
    "RELEASE SAVEPOINT s1",
    "ALTER TABLE u ADD COLUMN z INTEGER DEFAULT 5",
    "ALTER TABLE u RENAME COLUMN name TO label",
    "ALTER TABLE t DROP COLUMN h",
    "CREATE INDEX u_name ON u (name)",
    "CREATE UNIQUE INDEX u_k ON u (k) WHERE k > 0",
    "CREATE TRIGGER trg AFTER INSERT ON u FOR EACH ROW BEGIN UPDATE t SET a = a WHERE id = 1; END",
    "CREATE MATERIALIZED VIEW mv AS SELECT a, COUNT(*) AS n FROM t GROUP BY a",
    "REFRESH MATERIALIZED VIEW mv",
    "CREATE TABLE w (id INTEGER PRIMARY KEY, p INTEGER REFERENCES u(id) ON DELETE CASCADE, \
     q TEXT CHECK (length(q) < 5), r INTEGER GENERATED ALWAYS AS (p * 2) STORED)",
    "DROP TABLE IF EXISTS w",
    "TRUNCATE u",
    "SELECT 1 / 0, 9223372036854775807 + 1, -(-9223372036854775808), 7 % 0",
    "SELECT * FROM t ORDER BY (SELECT 1), random()",
    "SELECT a FROM t WHERE b COLLATE NOCASE = 'X' AND c > 1.0e0 AND h",
    "SELECT CAST('abc' AS INTEGER), CAST('2024-13-45' AS DATE), CAST(1e300 AS INTEGER), \
     CAST('12:00' AS INTERVAL)",
    "SELECT INTERVAL '1 year 2 months 3 days 04:05:06.789', INTERVAL 'P1Y2M3DT4H5M6S'",
    "SELECT $1, $2 + 1",
    "SELECT FROM t WHERE EXISTS (SELECT FROM u WHERE u.k = t.id)",
    "VALUES (1, 'a'), (2, 'b')",
    "SELECT * FROM (SELECT a, b FROM t) AS s JOIN (SELECT k FROM u) AS r ON r.k = s.a",
];

const FRAGMENTS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "(",
    ")",
    ",",
    "'",
    "\"",
    "NULL",
    "*",
    "OVER",
    "PARTITION BY",
    "JOIN",
    "ON",
    "LATERAL",
    "WITH",
    "UNION",
    "::",
    "->",
    "->>",
    "#>",
    "@>",
    "@@",
    "<->",
    "||",
    "CAST(",
    "AS",
    "INTERVAL '",
    "ORDER BY",
    "LIMIT",
    "OFFSET",
    "GROUP BY",
    "HAVING",
    "DISTINCT",
    "EXISTS",
    "IN",
    "ANY",
    "ALL",
    "CASE",
    "WHEN",
    "THEN",
    "END",
    "RETURNING",
    "DEFAULT",
    "COLLATE NOCASE",
    "FILTER (WHERE",
    "t.",
    "u.",
    "d.",
    "$1",
    "?",
    ";",
    "[",
    "]",
    "{",
    "}",
    "ARRAY[",
    "ROWS BETWEEN",
    "UNBOUNDED PRECEDING",
    "RANGE",
    "GROUPS",
    "é",
    "\u{0}",
    "😀",
    "\u{202e}",
    "0x",
    "1.",
    ".5",
    "--",
    "/*",
    "*/",
    "\\",
    "%",
    "!=",
    "<>",
    "<=>",
    "~",
    "^",
    "::VECTOR(3)",
    "::JSONB",
    "::INTERVAL",
    "::DATE",
    "::TIMESTAMP",
    "NOT",
    "IS",
    "BETWEEN",
    "LIKE",
    "ESCAPE",
    "RECURSIVE",
];

const NUMBERS: &[&str] = &[
    "0",
    "1",
    "-1",
    "2147483648",
    "9223372036854775807",
    "-9223372036854775808",
    "18446744073709551616",
    "1e-400",
    "1e999",
    "-0",
    "3.5",
];

/// SplitMix64, as the SQLite differential tester draws.
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
        (self.next() % bound.max(1) as u64) as usize
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len())]
    }
}

/// One to three edits of `sql`: deletions, insertions of SQL fragments and
/// numbers, duplications, truncations and splices of another statement.
fn mutate(rng: &mut Rng, sql: &str) -> String {
    let mut chars: Vec<char> = sql.chars().collect();
    for _ in 0..1 + rng.below(3) {
        let len = chars.len();
        let at = rng.below(len + 1);
        match rng.below(7) {
            0 if at < len => {
                let end = (at + 1 + rng.below(8)).min(len);
                chars.drain(at..end);
            }
            1 => {
                let fragment = format!(" {} ", rng.pick(FRAGMENTS));
                chars.splice(at..at, fragment.chars());
            }
            2 if at < len => {
                let end = (at + 1 + rng.below(20)).min(len);
                let piece: Vec<char> = chars[at..end].to_vec();
                let to = rng.below(len + 1);
                chars.splice(to..to, piece);
            }
            3 => chars.truncate(at),
            4 if at < len => {
                chars[at] = rng.pick(FRAGMENTS).chars().next().unwrap_or(' ');
            }
            5 => {
                let other: Vec<char> = rng.pick(CORPUS).chars().collect();
                let from = rng.below(other.len());
                chars.splice(at..at, other[from..].iter().copied());
            }
            _ => {
                let number = rng.pick(NUMBERS);
                chars.splice(at..at, number.chars());
            }
        }
    }
    chars.into_iter().collect()
}

fn fresh_database() -> Database {
    let db = DatabaseBuilder::new("")
        .passphrase(b"statement-fuzz")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    for sql in SCHEMA {
        conn.execute(sql).unwrap();
    }
    drop(conn);
    db
}

/// The last panic's location and message, written by the hook this test
/// installs so that each distinct site is reported once.
static LAST_PANIC: Mutex<Option<String>> = Mutex::new(None);

/// A panic's site and one statement that reaches it.
struct Found {
    seed: u64,
    statement: String,
    count: usize,
}

/// Runs the seed's statements on one connection per database, so a `BEGIN`
/// the mutator keeps puts the statements after it inside a transaction.
fn run_seed(seed: u64, found: &mut BTreeMap<String, Found>) {
    let mut rng = Rng(seed);
    let mut remaining = STATEMENTS_PER_SEED;
    while remaining > 0 {
        let db = fresh_database();
        let conn = Connection::open(&db).unwrap();
        for _ in 0..STATEMENTS_PER_DATABASE.min(remaining) {
            remaining -= 1;
            let source = rng.pick(CORPUS);
            let statement = mutate(&mut rng, source);
            let ran = catch_unwind(AssertUnwindSafe(|| {
                let _ = conn.execute(&statement);
            }));
            if ran.is_ok() {
                continue;
            }
            let site = LAST_PANIC.lock().unwrap().take().unwrap_or_default();
            found
                .entry(site)
                .or_insert_with(|| Found {
                    seed,
                    statement,
                    count: 0,
                })
                .count += 1;
            // A panic can leave the connection mid-statement, so the next
            // statements start over on a fresh database.
            break;
        }
    }
}

#[test]
fn mutated_statements_end_in_a_result_or_an_error() {
    let seeds = match std::env::var("CITADEL_SQL_FUZZ_SEEDS") {
        Ok(count) => count.parse().expect("CITADEL_SQL_FUZZ_SEEDS is a count"),
        Err(_) => DEFAULT_SEEDS,
    };
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|at| format!("{}:{}", at.file(), at.line()))
            .unwrap_or_default();
        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .map(|message| message.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        *LAST_PANIC.lock().unwrap() = Some(format!("{location}: {message}"));
        default_hook(info);
    }));
    let mut found = BTreeMap::new();
    for seed in 0..seeds {
        run_seed(0xF022_0000 + seed, &mut found);
    }
    // Back to the default hook.
    drop(std::panic::take_hook());
    let report: Vec<String> = found
        .iter()
        .map(|(site, found)| {
            format!(
                "{site} ({} times; seed {:#x}): {:?}",
                found.count, found.seed, found.statement
            )
        })
        .collect();
    assert!(report.is_empty(), "{}", report.join("\n"));
}
