//! Correlated projections filter their outer input before computing values.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"correlated-scan")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn correlated_scalar_projection_keeps_the_outer_index_range() {
    const ROWS: i64 = 1_000;
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, parent_id INTEGER)")
        .unwrap();
    connection.execute("BEGIN").unwrap();
    for id in 0..ROWS {
        connection
            .execute_params("INSERT INTO outer_rows VALUES ($1)", &[Value::Integer(id)])
            .unwrap();
        connection
            .execute_params(
                "INSERT INTO inner_rows VALUES ($1,$1)",
                &[Value::Integer(id)],
            )
            .unwrap();
    }
    connection.execute("COMMIT").unwrap();
    let statement = connection
        .prepare("SELECT o.id, (SELECT COUNT(*) FROM inner_rows i WHERE i.parent_id = o.id) AS n FROM outer_rows o WHERE o.id >= $1 AND o.id < $2 ORDER BY o.id")
        .unwrap();
    // Alternation prevents the prepared result cache from hiding scan work.
    for start in [100, 200, 100] {
        let measurement = db.measure_scans();
        let rows = statement
            .query_collect(&[Value::Integer(start), Value::Integer(start + 10)])
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            (start..start + 10)
                .map(|id| vec![Value::Integer(id), Value::Integer(1)])
                .collect::<Vec<_>>()
        );
        // One inner scan for hash decorrelation, ten outer rows and at most
        // the range's stopping entry. A full outer scan would double this.
        assert!(
            measurement.rows_scanned() <= ROWS as u64 + 11,
            "scanned {} entries for ten outer rows",
            measurement.rows_scanned()
        );
    }
}

#[test]
fn correlated_scalar_cardinality_ignores_filtered_outer_rows() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES (1),(2)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, parent_id INTEGER, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1,1,10),(2,2,20),(3,2,30)")
        .unwrap();
    for mode in ["", "BEGIN READ ONLY", "BEGIN"] {
        if !mode.is_empty() {
            connection.execute(mode).unwrap();
        }
        let statement = connection
            .prepare("SELECT o.id, (SELECT i.v FROM inner_rows i WHERE i.parent_id = o.id) AS v FROM outer_rows o WHERE o.id = $1")
            .unwrap();
        assert_eq!(
            statement.query_collect(&[Value::Integer(1)]).unwrap().rows,
            vec![vec![Value::Integer(1), Value::Integer(10)]]
        );
        assert!(matches!(
            statement.query_collect(&[Value::Integer(2)]),
            Err(SqlError::SubqueryMultipleRows)
        ));
        assert!(statement
            .query_collect(&[Value::Integer(3)])
            .unwrap()
            .rows
            .is_empty());
        if !mode.is_empty() {
            connection.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn correlated_scalar_projection_waits_for_deferred_where_predicates() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES (1),(2)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, parent_id INTEGER, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1,1,10),(2,2,20),(3,2,30)")
        .unwrap();
    for predicate in [
        // The whole CASE stays deferred; its unchosen scalar must stay silent.
        "o.id >= 1 AND CASE WHEN o.id = 1 THEN TRUE ELSE COALESCE(FALSE, (SELECT i.v FROM inner_rows i WHERE i.parent_id = o.id) = 20) END",
        "o.id >= 1 AND (SELECT COUNT(*) FROM inner_rows i WHERE i.parent_id = o.id) = 1",
        "o.id >= 1 AND EXISTS (SELECT 1 FROM inner_rows i WHERE i.parent_id = o.id AND i.v = 10)",
        // A volatile predicate stays at its original evaluation boundary.
        "o.id >= 1 AND CASE WHEN RANDOM() IS NOT NULL THEN o.id = 1 ELSE FALSE END",
    ] {
        let sql = format!(
            "SELECT o.id, (SELECT i.v FROM inner_rows i WHERE i.parent_id = o.id) AS v FROM outer_rows o WHERE {predicate}"
        );
        assert_eq!(
            connection.query(&sql).unwrap_or_else(|error| panic!("{sql}: {error}")).rows,
            vec![vec![Value::Integer(1), Value::Integer(10)]],
            "{sql}"
        );
    }
}
