//! Grouping, deduplicating and joining decide equality by hashing, not comparing, so they
//! need a folded key to honour a collation. SQLite documents GROUP BY, DISTINCT and the
//! comparison operators; set operations, COUNT(DISTINCT) and CTE survival follow the same
//! rule here by consistency, not citation.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    rows(conn, sql)
        .iter()
        .map(|r| match r.first() {
            Some(Value::Integer(n)) => *n,
            other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

fn texts(conn: &Connection, sql: &str) -> Vec<String> {
    rows(conn, sql)
        .iter()
        .map(|r| match r.first() {
            Some(Value::Text(s)) => s.to_string(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

/// `b` is binary and `s` is NOCASE, in one table, so a fix that folds too eagerly shows up.
fn seeded(conn: &Connection) {
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, b TEXT, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'b','b'),(2,'A','A'),(3,'B','B'),(4,'a','a')")
        .unwrap();
}

#[test]
fn group_by_groups_what_equality_calls_equal() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let grouped = rows(&conn, "SELECT s, COUNT(*) AS n FROM c GROUP BY s");
    assert_eq!(grouped.len(), 2, "A/a is one group and B/b is the other");
    for row in &grouped {
        assert_eq!(row[1], Value::Integer(2), "two rows per group");
    }

    assert_eq!(
        rows(&conn, "SELECT b, COUNT(*) AS n FROM c GROUP BY b").len(),
        4,
        "the binary column still groups by exact bytes"
    );
}

/// Without ORDER BY, DISTINCT takes a streaming path that keys on raw stored bytes; with one
/// it takes the general path. Both have to answer the same.
#[test]
fn distinct_removes_what_equality_calls_equal() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(texts(&conn, "SELECT DISTINCT s FROM c").len(), 2);
    assert_eq!(
        texts(&conn, "SELECT DISTINCT s FROM c ORDER BY s").len(),
        2,
        "the general path agrees with the streaming one"
    );
    assert_eq!(
        texts(&conn, "SELECT DISTINCT b FROM c").len(),
        4,
        "the binary column keeps all four"
    );
}

#[test]
fn count_distinct_counts_what_equality_calls_equal() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        rows(&conn, "SELECT COUNT(DISTINCT s) AS n FROM c")[0][0],
        Value::Integer(2)
    );
    assert_eq!(
        rows(&conn, "SELECT COUNT(DISTINCT b) AS n FROM c")[0][0],
        Value::Integer(4)
    );
}

/// A hash join builds a map from one side and probes it from the other, so both sides have
/// to fold identically or the probe misses rows the ON clause matches.
#[test]
fn a_join_matches_what_equality_calls_equal() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);
    conn.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO d VALUES (10,'a'),(11,'Y')")
        .unwrap();

    assert_eq!(
        ids(&conn, "SELECT c.id FROM c JOIN d ON c.s = d.s"),
        vec![2, 4],
        "'A' and 'a' both match d's 'a'"
    );
    assert_eq!(
        ids(&conn, "SELECT c.id FROM c JOIN d ON d.s = c.s"),
        vec![2, 4],
        "and the answer does not depend on which side the ON clause names first"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT c.id FROM c LEFT JOIN d ON c.s = d.s WHERE d.id IS NULL"
        ),
        vec![1, 3],
        "the rows with no match are the other two"
    );
}

/// A column always carries a collation, including BINARY. The syntactic left operand wins;
/// changing hash build/probe order must not change the ON predicate.
#[test]
fn a_join_follows_syntactic_left_collation_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);
    conn.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, plain TEXT)")
        .unwrap();
    conn.execute("INSERT INTO d VALUES (10,'a')").unwrap();

    assert_eq!(
        ids(&conn, "SELECT c.id FROM c JOIN d ON c.s = d.plain"),
        vec![2, 4]
    );
    assert_eq!(
        ids(&conn, "SELECT c.id FROM d JOIN c ON d.plain = c.s"),
        vec![4],
        "the BINARY left operand wins even when it belongs to the outer table"
    );
    assert_eq!(
        ids(&conn, "SELECT c.id FROM d JOIN c ON c.s = d.plain"),
        vec![2, 4],
        "the NOCASE left operand wins even when it belongs to the inner table"
    );
}

/// The folded key is only a key. RTRIM folds 'x  ' and 'x ' to 'x', which is not stored in
/// any row, so a group that reported its key would report a value that never existed.
#[test]
fn a_group_reports_a_stored_value_not_the_folded_key() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, t TEXT COLLATE RTRIM)")
        .unwrap();
    conn.execute("INSERT INTO r VALUES (1,'x  '),(2,'x ')")
        .unwrap();

    let grouped = rows(&conn, "SELECT t, COUNT(*) AS n FROM r GROUP BY t");
    assert_eq!(grouped.len(), 1, "both rows are one group under RTRIM");
    assert_eq!(grouped[0][1], Value::Integer(2));
    match &grouped[0][0] {
        Value::Text(s) => assert!(
            s.as_str() == "x  " || s.as_str() == "x ",
            "reported {s:?}, which is neither stored value"
        ),
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn rtrim_reaches_grouping_and_joining_too() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, t TEXT COLLATE RTRIM)")
        .unwrap();
    conn.execute("INSERT INTO r VALUES (1,'x'),(2,'x  '),(3,'y')")
        .unwrap();
    conn.execute("CREATE TABLE q (id INTEGER PRIMARY KEY, t TEXT COLLATE RTRIM)")
        .unwrap();
    conn.execute("INSERT INTO q VALUES (9,'x ')").unwrap();

    assert_eq!(texts(&conn, "SELECT DISTINCT t FROM r").len(), 2);
    assert_eq!(
        rows(&conn, "SELECT t, COUNT(*) AS n FROM r GROUP BY t").len(),
        2
    );
    assert_eq!(
        ids(&conn, "SELECT r.id FROM r JOIN q ON r.t = q.t"),
        vec![1, 2]
    );
}

/// A compound select has two lanes: a cached one, and a general one taken as soon as there
/// is an ORDER BY, LIMIT or OFFSET. Both run the same six set operations, and before this
/// they folded differently - so `UNION` and `UNION ... ORDER BY` deduplicated the same rows
/// to different answers, with the ORDER BY deciding which lane ran.
#[test]
fn both_compound_lanes_deduplicate_alike() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        texts(&conn, "SELECT s FROM c UNION SELECT s FROM c").len(),
        2,
        "the cached lane"
    );
    assert_eq!(
        texts(&conn, "SELECT s FROM c UNION SELECT s FROM c ORDER BY s").len(),
        2,
        "the general lane, which an ORDER BY selects"
    );
    assert_eq!(
        texts(&conn, "SELECT s FROM c UNION SELECT s FROM c LIMIT 10").len(),
        2,
        "and a LIMIT selects it too"
    );
}

#[test]
fn set_operations_compare_what_equality_calls_equal() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        texts(&conn, "SELECT s FROM c INTERSECT SELECT 'a'").len(),
        1
    );
    assert_eq!(
        texts(&conn, "SELECT s FROM c EXCEPT SELECT 'A'"),
        vec!["b"],
        "removing 'A' removes 'a' too, and B/b dedup to one row"
    );
    assert_eq!(
        texts(&conn, "SELECT s FROM c UNION ALL SELECT s FROM c").len(),
        8,
        "ALL keeps every row, collation or not"
    );
    assert_eq!(
        texts(&conn, "SELECT b FROM c UNION SELECT b FROM c").len(),
        4,
        "the binary column still compares by exact bytes"
    );
}

/// ORDER BY over a compound sorts the way the same column sorts inside a branch: 'B' and 'a'
/// are the discriminator, since binary puts every uppercase letter first.
#[test]
fn a_compound_order_by_keeps_the_branch_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (1,'B'),(2,'a')")
        .unwrap();

    assert_eq!(
        texts(&conn, "SELECT s FROM p UNION SELECT s FROM p ORDER BY s"),
        vec!["a", "B"]
    );
}

/// A derived table, a CTE and a view all present their rows as a table, and that table used
/// to report every column as binary - so a NOCASE column compared by exact bytes as soon as
/// it was read through one of them, in every operation, not just set operations.
#[test]
fn a_collation_survives_a_derived_table() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM (SELECT id, s FROM c) x WHERE s = 'A'"
        ),
        ids(&conn, "SELECT id FROM c WHERE s = 'A'"),
        "reading through a derived table matches reading the column directly"
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT s, COUNT(*) AS n FROM (SELECT s FROM c) x GROUP BY s"
        )
        .len(),
        2
    );
    assert_eq!(
        texts(&conn, "SELECT DISTINCT s FROM (SELECT s FROM c) x").len(),
        2
    );
    assert_eq!(
        texts(&conn, "SELECT DISTINCT b FROM (SELECT b FROM c) x").len(),
        4,
        "and a binary column still compares by exact bytes through one"
    );
}

#[test]
fn a_collation_survives_a_named_cte() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(
            &conn,
            "WITH w AS (SELECT id, s FROM c) SELECT id FROM w WHERE s = 'A'"
        ),
        vec![2, 4]
    );
    assert_eq!(
        texts(
            &conn,
            "WITH w AS (SELECT s FROM c) SELECT s FROM w UNION SELECT s FROM w"
        )
        .len(),
        2,
        "a set operation over a CTE deduplicates by the CTE's collation"
    );
}

#[test]
fn a_no_from_projection_reports_its_explicit_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();

    assert_eq!(
        texts(
            &conn,
            "SELECT s FROM (SELECT 'A' COLLATE NOCASE AS s) x WHERE s = 'a'"
        ),
        vec!["A"]
    );
    assert_eq!(
        texts(
            &conn,
            "WITH x AS (\
                SELECT 'A' COLLATE NOCASE AS s UNION ALL SELECT 'a'\
             ) SELECT DISTINCT s FROM x"
        )
        .len(),
        1
    );
}

#[test]
fn a_dml_cte_inherits_collation_from_returning_columns() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE returned (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();

    assert_eq!(
        texts(
            &conn,
            "WITH inserted AS (\
                INSERT INTO returned VALUES (1,'A'),(2,'a') RETURNING s\
             ) SELECT DISTINCT s FROM inserted"
        )
        .len(),
        1
    );
    assert_eq!(
        ids(
            &conn,
            "WITH updated AS (\
                UPDATE returned SET s = s RETURNING id, s\
             ) SELECT id FROM updated WHERE s = 'a' ORDER BY id"
        ),
        vec![1, 2]
    );
}

#[test]
fn a_collation_survives_a_view() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);
    conn.execute("CREATE VIEW v AS SELECT id, s FROM c")
        .unwrap();

    assert_eq!(ids(&conn, "SELECT id FROM v WHERE s = 'A'"), vec![2, 4]);
    assert_eq!(texts(&conn, "SELECT DISTINCT s FROM v").len(), 2);
}

/// The union of a derived table with a base table: the branch that supplies the collation is
/// the leftmost one, and resolving it has to reach through the subquery.
#[test]
fn a_set_operation_over_a_derived_table_deduplicates_alike() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        texts(
            &conn,
            "SELECT s FROM (SELECT s FROM c) x UNION SELECT s FROM c"
        )
        .len(),
        2
    );
    assert_eq!(
        texts(
            &conn,
            "SELECT b FROM (SELECT b FROM c) x UNION SELECT b FROM c"
        )
        .len(),
        4,
        "the binary column is unaffected"
    );
}

/// `*` stands for every column of its source, so resolving it has to produce one output
/// column each. Describing it as a single placeholder made the width disagree with the row,
/// and a mismatched resolution is discarded - which quietly cost the collation.
#[test]
fn a_star_projection_keeps_every_columns_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute(
        "CREATE TABLE m (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, t TEXT COLLATE NOCASE)",
    )
    .unwrap();
    conn.execute("INSERT INTO m VALUES (1,'b','x'),(2,'A','x'),(3,'a','X')")
        .unwrap();

    let explicit = rows(&conn, "SELECT s, t FROM m UNION SELECT s, t FROM m");
    assert_eq!(explicit.len(), 2, "'a','X' folds onto 'A','x'");
    assert_eq!(
        rows(
            &conn,
            "SELECT * FROM (SELECT s, t FROM m) x UNION SELECT s, t FROM m"
        )
        .len(),
        explicit.len(),
        "a star over the same two columns answers the same"
    );
    assert_eq!(
        rows(&conn, "SELECT DISTINCT * FROM (SELECT s, t FROM m) x").len(),
        2
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM m").len(),
        3,
        "and a plain star still returns every row"
    );
}

/// An index gives DISTINCT and the join a different route to the rows.
#[test]
fn an_index_does_not_change_the_grouping() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);
    conn.execute("CREATE INDEX ix ON c(s)").unwrap();

    assert_eq!(texts(&conn, "SELECT DISTINCT s FROM c").len(), 2);
    assert_eq!(
        rows(&conn, "SELECT s, COUNT(*) AS n FROM c GROUP BY s").len(),
        2
    );
}

#[test]
fn correlated_exists_uses_the_equality_operands_collation_for_its_hash_key() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE outer_c (id INTEGER PRIMARY KEY, s TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE inner_c (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO outer_c VALUES (1,'a')").unwrap();
    conn.execute("INSERT INTO inner_c VALUES (10,'A')").unwrap();

    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_c o \
             WHERE EXISTS (SELECT 1 FROM inner_c i WHERE i.s = o.s)"
        ),
        vec![1],
        "the inner NOCASE column is the syntactic left operand"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_c o \
             WHERE EXISTS (SELECT 1 FROM inner_c i WHERE o.s = i.s)"
        ),
        Vec::<i64>::new(),
        "the outer BINARY column is the syntactic left operand"
    );
}

#[test]
fn correlated_in_folds_values_under_the_effective_operand_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE outer_i (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE inner_i (\
            id INTEGER PRIMARY KEY, outer_id INTEGER, \
            nocase_value TEXT COLLATE NOCASE, binary_value TEXT)",
    )
    .unwrap();
    conn.execute("INSERT INTO outer_i VALUES (1),(2)").unwrap();
    conn.execute(
        "INSERT INTO inner_i VALUES \
         (10,1,'A','A'),(11,1,NULL,NULL),(20,2,'B','B')",
    )
    .unwrap();

    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_i o WHERE 'a' IN (\
                SELECT nocase_value FROM inner_i i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        vec![1],
        "a collation-free left value inherits NOCASE from the selected column"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_i o WHERE ('a' COLLATE BINARY) IN (\
                SELECT nocase_value FROM inner_i i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        Vec::<i64>::new(),
        "an explicit BINARY left operand overrides the selected NOCASE column"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_i o WHERE ('a' COLLATE NOCASE) IN (\
                SELECT binary_value FROM inner_i i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        vec![1],
        "an explicit NOCASE left operand overrides the selected BINARY column"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_i o WHERE 'a' IN (\
                SELECT binary_value COLLATE NOCASE \
                FROM inner_i i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        vec![1],
        "an explicit collation on the selected IN value supplies comparison semantics"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_i o WHERE 'z' NOT IN (\
                SELECT binary_value FROM inner_i i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        vec![2],
        "the NULL belonging to outer key 1 must not make key 2 UNKNOWN"
    );
}

#[test]
fn correlated_in_keeps_empty_and_nonempty_null_truth_tables_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE outer_null (id INTEGER PRIMARY KEY, probe TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE inner_null (id INTEGER PRIMARY KEY, outer_id INTEGER, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO outer_null VALUES (1,NULL),(2,NULL)")
        .unwrap();
    conn.execute("INSERT INTO inner_null VALUES (10,1,'present')")
        .unwrap();

    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_null o WHERE o.probe NOT IN (\
                SELECT v FROM inner_null i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        vec![2],
        "NULL NOT IN a nonempty result is UNKNOWN, but NULL NOT IN an empty result is true"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_null o WHERE o.probe IN (\
                SELECT v FROM inner_null i WHERE i.outer_id = o.id\
             ) ORDER BY o.id"
        ),
        Vec::<i64>::new()
    );
}

#[test]
fn every_correlated_conjunct_must_pass_before_the_fast_scan_emits_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute(
        "CREATE TABLE outer_filter (\
            id INTEGER PRIMARY KEY, probe TEXT, threshold INTEGER)",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE first_filter (\
            id INTEGER PRIMARY KEY, outer_id INTEGER, probe TEXT, n INTEGER)",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE second_filter (\
            id INTEGER PRIMARY KEY, outer_id INTEGER, probe TEXT)",
    )
    .unwrap();
    conn.execute("INSERT INTO outer_filter VALUES (1,'hit',5)")
        .unwrap();
    conn.execute("INSERT INTO first_filter VALUES (10,1,'hit',10)")
        .unwrap();
    conn.execute("INSERT INTO second_filter VALUES (20,1,'miss')")
        .unwrap();

    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_filter o WHERE \
             EXISTS (SELECT 1 FROM first_filter a \
                     WHERE a.outer_id = o.id AND a.n > o.threshold) \
             AND EXISTS (SELECT 1 FROM second_filter b \
                         WHERE b.outer_id = o.id AND b.probe = o.probe)"
        ),
        Vec::<i64>::new(),
        "a passing filtered EXISTS must not skip the later failing EXISTS"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_filter o WHERE \
             o.probe IN (SELECT probe FROM first_filter a WHERE a.outer_id = o.id) \
             AND o.probe IN (SELECT probe FROM second_filter b WHERE b.outer_id = o.id)"
        ),
        Vec::<i64>::new(),
        "a passing correlated IN must not skip the later failing IN"
    );
}

#[test]
fn correlated_fast_paths_propagate_expression_errors() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE outer_error (id INTEGER PRIMARY KEY, bad TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE inner_error (\
            id INTEGER PRIMARY KEY, outer_id INTEGER, n INTEGER, bad TEXT)",
    )
    .unwrap();
    conn.execute("INSERT INTO outer_error VALUES (1,'not-an-integer')")
        .unwrap();
    conn.execute("INSERT INTO inner_error VALUES (10,1,1,'not-an-integer')")
        .unwrap();

    let in_error = conn
        .execute(
            "SELECT o.id FROM outer_error o WHERE CAST(o.bad AS INTEGER) IN (\
                SELECT n FROM inner_error i WHERE i.outer_id = o.id\
             )",
        )
        .unwrap_err();
    assert!(
        in_error.to_string().contains("cannot cast"),
        "an invalid correlated-IN probe must propagate its cast error, got {in_error}"
    );
    let scalar_error = conn
        .execute(
            "SELECT o.id FROM outer_error o WHERE CAST(o.bad AS INTEGER) = (\
                SELECT n FROM inner_error i WHERE i.outer_id = o.id\
             )",
        )
        .unwrap_err();
    assert!(
        scalar_error.to_string().contains("cannot cast"),
        "an invalid correlated-scalar operand must propagate its cast error, got {scalar_error}"
    );

    conn.execute("CREATE VIEW inner_error_view AS SELECT id, outer_id, bad FROM inner_error")
        .unwrap();
    let view_error = conn
        .execute(
            "SELECT o.id FROM outer_error o WHERE EXISTS (\
                SELECT 1 FROM inner_error_view i \
                WHERE i.outer_id = o.id AND CAST(i.bad AS INTEGER) > 0\
             )",
        )
        .unwrap_err();
    assert!(
        view_error.to_string().contains("cannot cast"),
        "an invalid filtered view row must propagate its cast error, got {view_error}"
    );
}

#[test]
fn correlated_non_equality_binding_walks_wrappers_and_respects_inner_shadowing() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute(
        "CREATE TABLE outer_bind (\
            id INTEGER PRIMARY KEY, value TEXT, expected TEXT, threshold TEXT)",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE inner_bind (\
            id INTEGER PRIMARY KEY, outer_id INTEGER, value TEXT)",
    )
    .unwrap();
    conn.execute("INSERT INTO outer_bind VALUES (1,'wrong','a','A')")
        .unwrap();
    conn.execute("INSERT INTO inner_bind VALUES (10,1,'a')")
        .unwrap();

    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_bind o WHERE EXISTS (\
                SELECT 1 FROM inner_bind i WHERE i.outer_id = o.id \
                AND i.value = (CAST(o.threshold AS TEXT) COLLATE NOCASE)\
             )"
        ),
        vec![1],
        "the wrapped outer reference must be classified as correlated and bound"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT o.id FROM outer_bind o WHERE EXISTS (\
                SELECT 1 FROM inner_bind i WHERE i.outer_id = o.id \
                AND value = o.expected\
             )"
        ),
        vec![1],
        "an unqualified name shared by both scopes belongs to the inner scope"
    );
}
