use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"prepared-read-context")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn create_old_row(conn: &Connection<'_>, data_type: &str, default: &str) {
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1)").unwrap();
    conn.execute(&format!(
        "ALTER TABLE items ADD COLUMN d {data_type} DEFAULT ({default})"
    ))
    .unwrap();
    conn.execute(&format!(
        "ALTER TABLE items ADD COLUMN v {data_type} GENERATED ALWAYS AS (d) VIRTUAL"
    ))
    .unwrap();
}

fn assert_no_parameters<T: std::fmt::Debug>(result: Result<T, citadel_sql::SqlError>) {
    let error = result.unwrap_err();
    assert!(
        matches!(
            error,
            citadel_sql::SqlError::ParameterCountMismatch {
                expected: 1,
                got: 0
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        citadel_sql::eval::resolve_scoped_param(1).unwrap(),
        Value::Integer(99)
    );
}

#[test]
fn empty_statement_parameters_mask_ambient_bindings_in_buffered_reads() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "INTEGER", "$1");
    // ORDER BY deliberately uses buffered execution independently of the
    // separate plain prepared stream/collect admission repair.
    let sql = "SELECT d, v FROM items ORDER BY id";
    let prepared = conn.prepare(sql).unwrap();
    citadel_sql::eval::with_scoped_params(&[Value::Integer(99)], || {
        assert_no_parameters(conn.query(sql));
        assert_no_parameters(prepared.query_collect(&[]));
        assert_no_parameters(prepared.query(&[]).and_then(|rows| rows.collect()));
        assert_no_parameters(conn.query_params_bounded(
            sql,
            &[],
            &citadel_sql::ReadBudget::new(1024, 1024 * 1024),
        ));
    });
}

#[test]
fn empty_statement_parameters_mask_ambient_bindings_in_active_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "INTEGER", "$1");
    let sql = "SELECT d, v FROM items ORDER BY id";
    let prepared = conn.prepare(sql).unwrap();
    for begin in ["BEGIN READ ONLY", "BEGIN"] {
        conn.execute(begin).unwrap();
        citadel_sql::eval::with_scoped_params(&[Value::Integer(99)], || {
            // Direct reads dispatch in the active transaction; prepared reads
            // take the separate compiled-in-transaction boundary.
            assert_no_parameters(conn.query(sql));
            assert_no_parameters(prepared.query_collect(&[]));
            assert_no_parameters(prepared.query(&[]).and_then(|rows| rows.collect()));
            assert_no_parameters(conn.query_params_bounded(
                sql,
                &[],
                &citadel_sql::ReadBudget::new(1024, 1024 * 1024),
            ));
        });
        conn.execute("ROLLBACK").unwrap();
    }
}

#[test]
fn explicit_statement_bindings_and_direct_compiled_insert_remain_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "INTEGER", "$1");
    conn.execute("CREATE TABLE plain (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    let select = conn
        .prepare("SELECT d, v, $1 FROM items ORDER BY id")
        .unwrap();
    let insert = conn.prepare("INSERT INTO plain VALUES ($1, $2)").unwrap();
    conn.execute("BEGIN").unwrap();
    citadel_sql::eval::with_scoped_params(&[Value::Integer(99)], || {
        assert_eq!(
            select.query_collect(&[Value::Integer(7)]).unwrap().rows,
            vec![vec![Value::Integer(7); 3]]
        );
        assert_eq!(
            insert
                .execute(&[Value::Integer(1), Value::Integer(8)])
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query("SELECT value FROM plain WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(8)]]
        );
        assert_eq!(
            citadel_sql::eval::resolve_scoped_param(1).unwrap(),
            Value::Integer(99)
        );
    });
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn prepared_hidden_parameter_defaults_cannot_borrow_an_outer_parameter_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "INTEGER", "$1");
    let prepared = conn.prepare("SELECT d, v FROM items").unwrap();
    citadel_sql::eval::with_scoped_params(&[Value::Integer(99)], || {
        let collected = prepared.query_collect(&[]).unwrap_err();
        let streamed = prepared
            .query(&[])
            .and_then(|rows| rows.collect())
            .unwrap_err();
        for error in [collected, streamed] {
            assert!(
                matches!(
                    error,
                    citadel_sql::SqlError::ParameterCountMismatch {
                        expected: 1,
                        got: 0
                    }
                ),
                "{error:?}"
            );
        }
        // Each buffered statement restores the previous scope even on error.
        assert_eq!(
            citadel_sql::eval::resolve_scoped_param(1).unwrap(),
            Value::Integer(99)
        );
    });
}
