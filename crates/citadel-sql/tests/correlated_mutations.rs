use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{executor, parser, schema::SchemaManager, Connection, ExecutionResult, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"correlated-mutations")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(count) => count,
        other => panic!("{other:?}"),
    }
}

fn mutate(db: &Database, conn: &Connection<'_>, sql: &str, mode: usize) -> u64 {
    match mode {
        0 => affected(conn.execute(sql).unwrap()),
        1 => conn.prepare(sql).unwrap().execute(&[]).unwrap(),
        2 => {
            conn.execute("BEGIN").unwrap();
            let n = affected(conn.execute(sql).unwrap());
            conn.execute("COMMIT").unwrap();
            n
        }
        3 => affected(conn.execute_batch(sql).unwrap().remove(0)),
        4 => {
            let mut schema = SchemaManager::load(db).unwrap();
            let mut txn = db.begin_write().unwrap();
            let n = affected(
                executor::execute_in_txn(
                    &mut txn,
                    &mut schema,
                    &parser::parse_sql(sql).unwrap(),
                    &[],
                )
                .unwrap(),
            );
            txn.commit().unwrap();
            n
        }
        _ => unreachable!(),
    }
}

fn fixture(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, v INTEGER, PRIMARY KEY (a,b))")
        .unwrap();
    conn.execute(
        "CREATE TABLE refs (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, threshold INTEGER)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1,1,10),(1,2,20),(2,1,30),(3,1,40),(4,1,50)")
        .unwrap();
    conn.execute("INSERT INTO refs VALUES (1,1,2,15),(2,2,1,35),(3,3,1,NULL)")
        .unwrap();
}

#[test]
fn correlated_mutations_preserve_full_physical_keys_and_residual_predicates() {
    let cases = [
        ("(SELECT t.v)>35", vec![(3, 1), (4, 1)]),
        ("EXISTS (SELECT 1 WHERE t.v>35)", vec![(3, 1), (4, 1)]),
        (
            "EXISTS (SELECT COUNT(*) FROM refs HAVING MAX(threshold)<t.v)",
            vec![(3, 1), (4, 1)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs r JOIN refs s ON r.id=s.id AND t.v<r.threshold)",
            vec![(1, 1), (1, 2), (2, 1)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs LIMIT CASE WHEN t.a=1 THEN 0 ELSE 1 END)",
            vec![(2, 1), (3, 1), (4, 1)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs r WHERE r.a=t.b AND r.b=r.a)",
            vec![],
        ),
        (
            "EXISTS (SELECT 1 FROM refs r WHERE r.a=t.b AND t.a=t.b)",
            vec![(1, 1)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs WHERE refs.a=t.a AND refs.b=t.b)",
            vec![(1, 2), (2, 1), (3, 1)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs WHERE refs.a=t.a AND t.v>refs.threshold)",
            vec![(1, 2)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs WHERE refs.threshold<t.v)",
            vec![(1, 2), (2, 1), (3, 1), (4, 1)],
        ),
        (
            "t.a=4 OR (EXISTS (SELECT 1 FROM refs WHERE refs.a=t.a AND t.v>refs.threshold))",
            vec![(1, 2), (4, 1)],
        ),
        (
            "t.v > (SELECT MAX(threshold) FROM refs WHERE refs.a=t.a)",
            vec![(1, 2)],
        ),
        (
            "(SELECT COUNT(*) FROM refs WHERE refs.a=t.a)=0",
            vec![(4, 1)],
        ),
        (
            "EXISTS (SELECT 1 FROM refs WHERE refs.a=t.a LIMIT 0)",
            vec![],
        ),
    ];
    for mode in 0..5 {
        for (predicate, expected) in &cases {
            for delete in [false, true] {
                let db = database();
                let conn = Connection::open(&db).unwrap();
                fixture(&conn);
                let sql = if delete {
                    format!("DELETE FROM t WHERE {predicate}")
                } else {
                    format!("UPDATE t SET v=99 WHERE {predicate}")
                };
                assert_eq!(
                    mutate(&db, &conn, &sql, mode),
                    expected.len() as u64,
                    "{mode}: {sql}"
                );
                let rows = conn
                    .query(if delete {
                        "SELECT a,b FROM t ORDER BY a,b"
                    } else {
                        "SELECT a,b FROM t WHERE v=99 ORDER BY a,b"
                    })
                    .unwrap()
                    .rows;
                let keys = if delete {
                    vec![(1, 1), (1, 2), (2, 1), (3, 1), (4, 1)]
                        .into_iter()
                        .filter(|key| !expected.contains(key))
                        .collect::<Vec<_>>()
                } else {
                    expected.clone()
                };
                assert_eq!(
                    rows,
                    keys.into_iter()
                        .map(|(a, b)| vec![Value::Integer(a), Value::Integer(b)])
                        .collect::<Vec<_>>(),
                    "{mode}: {sql}"
                );
            }
        }
    }
}

#[test]
fn correlated_predicates_observe_uncommitted_rows_and_preserve_rollback() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    fixture(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO refs VALUES (4,4,1,45)").unwrap();
    assert_eq!(conn.prepare("UPDATE t SET v=99 WHERE EXISTS (SELECT 1 FROM refs WHERE refs.a=t.a AND t.v>refs.threshold)").unwrap().execute(&[]).unwrap(),2);
    conn.execute("SAVEPOINT kept").unwrap();
    assert_eq!(affected(conn.execute("DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM refs WHERE refs.a=t.a AND refs.b=t.b)").unwrap()),1);
    conn.execute("ROLLBACK TO kept").unwrap();
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM t").unwrap().rows,
        vec![vec![Value::Integer(5)]]
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        conn.query("SELECT v FROM t ORDER BY a,b").unwrap().rows,
        vec![
            vec![Value::Integer(10)],
            vec![Value::Integer(20)],
            vec![Value::Integer(30)],
            vec![Value::Integer(40)],
            vec![Value::Integer(50)]
        ]
    );
}

#[test]
fn nested_correlations_respect_alias_shadowing() {
    for mode in 0..5 {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        fixture(&conn);
        conn.execute("CREATE TABLE thresholds (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO thresholds VALUES (1,18)")
            .unwrap();
        assert_eq!(mutate(&db,&conn,"UPDATE t SET v=99 WHERE EXISTS (SELECT 1 FROM refs r WHERE r.a=t.a AND EXISTS (SELECT 1 FROM thresholds s WHERE s.v<t.v AND s.v>r.threshold))",mode),1);
        assert_eq!(
            conn.query("SELECT a,b FROM t WHERE v=99").unwrap().rows,
            vec![vec![Value::Integer(1), Value::Integer(2)]]
        );
        // The innermost t is a local alias, and must not bind to the target.
        assert_eq!(mutate(&db,&conn,"DELETE FROM t WHERE EXISTS (SELECT 1 FROM refs r WHERE r.a=t.a AND EXISTS (SELECT 1 FROM thresholds t WHERE t.v=18))",mode),4);
    }
}

#[test]
fn qualified_temp_target_uses_its_sql_name() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMP TABLE target (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE refs (id INTEGER PRIMARY KEY, threshold INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO target VALUES (1,10),(2,20)")
        .unwrap();
    conn.execute("INSERT INTO refs VALUES (1,15)").unwrap();
    assert_eq!(affected(conn.execute("UPDATE target SET v=99 WHERE EXISTS (SELECT 1 FROM refs WHERE refs.threshold<target.v)").unwrap()),1);
    assert_eq!(
        conn.query("SELECT id FROM target WHERE v=99").unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
}

#[test]
fn correlated_bound_columns_keep_implicit_collation_precedence() {
    for (predicate, count) in [
        ("t.name = r.name", 1),
        ("r.name = t.name", 0),
        ("t.name COLLATE BINARY = r.name", 0),
        ("(t.name COLLATE BINARY) = r.name", 0),
        ("r.name = t.name COLLATE NOCASE", 1),
        ("r.name = (t.name COLLATE NOCASE)", 1),
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
            .unwrap();
        conn.execute("CREATE TABLE refs (id INTEGER PRIMARY KEY, name TEXT COLLATE RTRIM)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,'UPPER')").unwrap();
        conn.execute("INSERT INTO refs VALUES (1,'upper')").unwrap();
        let select=format!("SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM refs r WHERE r.id=t.id AND (({predicate}) OR r.id=99))");
        assert_eq!(
            conn.query(&select).unwrap().rows.len() as u64,
            count,
            "read: {predicate}"
        );
        let sql = format!(
            "UPDATE t SET id=2 WHERE EXISTS (SELECT 1 FROM refs r WHERE ({predicate}) OR r.id=99)"
        );
        assert_eq!(affected(conn.execute(&sql).unwrap()), count, "{predicate}");
    }
}

#[test]
fn mixed_predicates_scan_independent_sources_once_and_keep_semijoins() {
    const OUTER: u64 = 64;
    const INDEPENDENT: u64 = 128;
    for disjunction in [false, true] {
        for scalar in [false, true] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
                .unwrap();
            conn.execute("CREATE TABLE dependent (id INTEGER PRIMARY KEY, target INTEGER)")
                .unwrap();
            conn.execute("CREATE TABLE fixed (id INTEGER PRIMARY KEY, v INTEGER)")
                .unwrap();
            conn.execute("BEGIN").unwrap();
            for id in 0..OUTER as i64 {
                conn.execute_params("INSERT INTO t VALUES ($1,0)", &[Value::Integer(id)])
                    .unwrap();
                conn.execute_params(
                    "INSERT INTO dependent VALUES ($1,$1)",
                    &[Value::Integer(id)],
                )
                .unwrap();
            }
            for id in 0..INDEPENDENT as i64 {
                conn.execute_params("INSERT INTO fixed VALUES ($1,1)", &[Value::Integer(id)])
                    .unwrap();
            }
            conn.execute("COMMIT").unwrap();
            let closed = match (scalar, disjunction) {
                (true, false) => "(SELECT SUM(v) FROM fixed WHERE v >= $1) = 128",
                (true, true) => "(SELECT SUM(v) FROM fixed WHERE v >= $1) = 0",
                (false, false) => "EXISTS (SELECT 1 FROM fixed WHERE v >= $1)",
                (false, true) => "EXISTS (SELECT 1 FROM fixed WHERE v < $1)",
            };
            let operator = if disjunction { "OR" } else { "AND" };
            let sql = format!("UPDATE t SET v=1 WHERE ({closed}) {operator} EXISTS (SELECT 1 FROM dependent WHERE dependent.target=t.id)");
            let statement = conn.prepare(&sql).unwrap();
            let scans = db.measure_scans();
            assert_eq!(statement.execute(&[Value::Integer(1)]).unwrap(), OUTER);
            let entries = scans.rows_scanned();
            // OR needs one dependent scan per outer row. AND remains a single
            // semijoin scan after the independent query has been materialized.
            let dependent_work = if disjunction { OUTER * OUTER } else { OUTER };
            let bound = dependent_work + OUTER * 2 + INDEPENDENT;
            assert!(entries <= bound, "scalar={scalar}, disjunction={disjunction}: {entries} scanned entries exceeds {bound}");
            drop(scans);
            assert_eq!(
                conn.query("SELECT COUNT(*) FROM t WHERE v=1").unwrap().rows,
                vec![vec![Value::Integer(OUTER as i64)]]
            );
        }
    }
}
