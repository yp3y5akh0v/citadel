//! `ORDER BY <output alias>`, which PostgreSQL, SQLite and MySQL all accept. The sort runs
//! on SOURCE columns before projection, so a select-list-only alias resolved to
//! `Value::Null` and the sort silently became a no-op.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn ints(conn: &Connection, sql: &str) -> Vec<i64> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q
            .rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn texts(conn: &Connection, sql: &str) -> Vec<String> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q
            .rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Text(s)) => s.to_string(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn seeded(conn: &Connection) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 50), (2, 10), (3, 40), (4, 20), (5, 30)")
        .unwrap();
}

#[test]
fn order_by_a_select_alias_sorts() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(&conn, "SELECT n AS score FROM t ORDER BY score"),
        vec![10, 20, 30, 40, 50]
    );
    assert_eq!(
        ints(&conn, "SELECT n AS score FROM t ORDER BY score DESC"),
        vec![50, 40, 30, 20, 10]
    );
}

/// The alias may name a computed expression, which is the case that has no other way to be
/// written: `ORDER BY` cannot see a value the projection invents unless it is resolved.
#[test]
fn order_by_an_alias_over_an_expression_sorts() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(
            &conn,
            "SELECT id, n * 2 AS doubled FROM t ORDER BY doubled DESC"
        ),
        vec![1, 3, 5, 4, 2],
        "ids in order of n*2 descending: 100, 80, 60, 40, 20"
    );
}

/// With LIMIT the executor takes a top-k path rather than a full sort, so it needs the
/// resolved key too - and a wrong top-k drops the wrong rows entirely.
#[test]
fn order_by_an_alias_under_a_limit_keeps_the_right_rows() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(
            &conn,
            "SELECT n AS score FROM t ORDER BY score DESC LIMIT 2"
        ),
        vec![50, 40]
    );
}

/// An output name wins over an input name of the same spelling.
#[test]
fn an_output_name_outranks_an_input_column_of_the_same_name() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    // `n` as an OUTPUT name is `-n`, so ascending output order is descending input order.
    assert_eq!(
        ints(&conn, "SELECT id, -n AS n FROM t ORDER BY n"),
        vec![1, 3, 5, 4, 2],
        "ORDER BY n must mean the projected -n, not the column n"
    );
}

/// A qualified name is an input column by definition, so it is left alone even when an
/// alias of the same spelling exists.
#[test]
fn a_qualified_name_still_means_the_input_column() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(&conn, "SELECT id, -n AS n FROM t ORDER BY t.n"),
        vec![2, 4, 5, 3, 1],
        "t.n is the stored column, ascending: 10, 20, 30, 40, 50"
    );
}

#[test]
fn an_aggregate_alias_sorts_the_projected_groups() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE grouped (id INTEGER PRIMARY KEY, g TEXT)")
        .unwrap();
    conn.execute("INSERT INTO grouped VALUES (1, 'a'), (2, 'b'), (3, 'b'), (4, 'b')")
        .unwrap();

    let ExecutionResult::Query(result) = conn
        .execute("SELECT g, COUNT(*) AS n FROM grouped GROUP BY g ORDER BY n DESC")
        .unwrap()
    else {
        panic!("expected rows")
    };

    assert_eq!(result.rows[0][0], Value::Text("b".into()));
    assert_eq!(result.rows[0][1], Value::Integer(3));
    assert_eq!(result.rows[1][0], Value::Text("a".into()));
    assert_eq!(result.rows[1][1], Value::Integer(1));
}

#[test]
fn a_volatile_alias_is_evaluated_once_then_sorted() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE many (id INTEGER PRIMARY KEY)")
        .unwrap();
    let values = (0..64)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    conn.execute(&format!("INSERT INTO many VALUES {values}"))
        .unwrap();

    let values = ints(&conn, "SELECT RANDOM() AS r FROM many ORDER BY r");

    assert!(
        values.windows(2).all(|pair| pair[0] <= pair[1]),
        "ORDER BY must use the same RANDOM() values that are returned: {values:?}"
    );

    let limited = ints(&conn, "SELECT RANDOM() AS r FROM many ORDER BY r LIMIT 8");
    assert_eq!(limited.len(), 8);
    assert!(
        limited.windows(2).all(|pair| pair[0] <= pair[1]),
        "top-k must use each returned RANDOM() value exactly once: {limited:?}"
    );
}

#[test]
fn a_simple_column_alias_keeps_the_topk_scan_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert!(
        texts(
            &conn,
            "EXPLAIN SELECT n AS score FROM t ORDER BY score LIMIT 2"
        )
        .iter()
        .any(|line| line.contains("TOPK SCAN")),
        "the retained source expression should keep the fused top-k scan eligible"
    );
}

#[test]
fn a_window_alias_sorts_its_materialized_values() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(
            &conn,
            "SELECT id, ROW_NUMBER() OVER (ORDER BY n DESC) AS rn \
             FROM t ORDER BY rn DESC LIMIT 2"
        ),
        vec![2, 4]
    );
}

#[test]
fn an_alias_after_star_uses_its_expanded_output_slot() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    for sql in [
        "SELECT id, *, -n AS score FROM t ORDER BY score LIMIT 2",
        "SELECT DISTINCT id, *, -n AS score FROM t ORDER BY score LIMIT 2",
    ] {
        assert_eq!(
            ints(&conn, sql),
            vec![1, 3],
            "score is the column after the expanded star: {sql}"
        );
    }
}

#[test]
fn duplicate_output_aliases_are_ambiguous() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    for sql in [
        "SELECT id, n AS x, -n AS x FROM t ORDER BY x",
        "SELECT id, n AS x, -n AS x FROM t ORDER BY x LIMIT 2",
    ] {
        assert!(
            matches!(conn.execute(sql), Err(SqlError::AmbiguousColumn(name)) if name == "x"),
            "duplicate x must not change meaning when LIMIT selects a fast path: {sql}"
        );
    }
}

#[test]
fn aliases_that_duplicate_other_projected_names_are_ambiguous() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    for sql in [
        "SELECT id, n AS id FROM t ORDER BY id",
        "SELECT id, n AS id FROM t ORDER BY id LIMIT 2",
        "SELECT *, -n AS n FROM t ORDER BY n",
        "SELECT *, -n AS n FROM t ORDER BY n LIMIT 2",
    ] {
        assert!(
            matches!(conn.execute(sql), Err(SqlError::AmbiguousColumn(_))),
            "every projected column named by ORDER BY must be unique: {sql}"
        );
    }
}

#[test]
fn topk_keeps_explicit_null_placement_for_a_descending_alias() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE nullable (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO nullable VALUES (1,2),(2,NULL),(3,1),(4,3)")
        .unwrap();

    assert_eq!(
        ints(
            &conn,
            "SELECT id, v AS score FROM nullable ORDER BY score DESC NULLS FIRST LIMIT 3"
        ),
        vec![2, 4, 1]
    );
    assert_eq!(
        ints(
            &conn,
            "SELECT id, v AS score FROM nullable ORDER BY score DESC NULLS LAST LIMIT 4"
        ),
        vec![4, 1, 3, 2]
    );
}

#[test]
fn an_aggregate_alias_keeps_the_group_columns_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'B'),(2,'a'),(3,'b')")
        .unwrap();

    assert_eq!(
        texts(
            &conn,
            "SELECT s AS x, COUNT(*) FROM c GROUP BY s ORDER BY x"
        ),
        vec!["a", "B"]
    );
}

#[test]
fn aggregate_sort_keys_can_mix_an_alias_with_an_unselected_group_column() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE grouped (id INTEGER PRIMARY KEY, g TEXT)")
        .unwrap();
    conn.execute("INSERT INTO grouped VALUES (1,'b'),(2,'a'),(3,'b'),(4,'a')")
        .unwrap();

    assert_eq!(
        ints(
            &conn,
            "SELECT MIN(id) AS first_id, COUNT(*) AS n \
             FROM grouped GROUP BY g ORDER BY n DESC, g DESC"
        ),
        vec![1, 2],
        "equal aggregate aliases are broken by the unselected group key"
    );
}
