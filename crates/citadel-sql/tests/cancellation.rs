//! SQL statements observe cancellation at entry and during bounded work, pinned
//! deterministically rather than by scheduler timing.

use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};
use citadel_sql::schema::SchemaManager;
use citadel_sql::system_tables::VirtualTable;
use citadel_sql::{Connection, QueryResult, SqlError, Value};
use std::sync::{Arc, Barrier};

const ROWS: i64 = 20_000;

fn seeded_db(dir: &std::path::Path) -> citadel::Database {
    let db = DatabaseBuilder::new(dir.join("cancel.db"))
        .passphrase(b"cancellation-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        for i in 0..ROWS {
            conn.execute_params(
                "INSERT INTO t (id, tag) VALUES ($1, $2)",
                &[Value::Integer(i), Value::Text(format!("tag{i}").into())],
            )
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }
    db
}

fn is_cancelled(e: &SqlError) -> bool {
    matches!(e, SqlError::Storage(citadel::Error::Interrupted))
}

struct CancellationHandoff {
    entered: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl VirtualTable for CancellationHandoff {
    fn name(&self) -> &str {
        "cancellation_handoff"
    }

    fn scan(
        &self,
        _schema: &SchemaManager,
        cancel: Option<&CancelToken>,
    ) -> citadel_sql::Result<QueryResult> {
        let cancel = cancel.expect("the executor omitted the read transaction's token");
        self.entered.wait();
        self.resume.wait();
        cancel.check().map_err(SqlError::Storage)?;
        Ok(QueryResult {
            columns: vec!["value".into()],
            rows: vec![vec![Value::Integer(1)]],
        })
    }
}

/// The worker trips the token only after this one executor operation has entered
/// its virtual scan. A refusal at the next statement boundary cannot satisfy it.
#[test]
fn a_select_observes_a_token_tripped_from_another_thread() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let token = CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let mut schema = SchemaManager::load(&db).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    schema.register_virtual(Arc::new(CancellationHandoff {
        entered: Arc::clone(&entered),
        resume: Arc::clone(&resume),
    }));
    let stmt = citadel_sql::parser::parse_sql("SELECT value FROM cancellation_handoff").unwrap();
    let mut rtx = db.begin_read();

    let stopper = std::thread::spawn(move || {
        entered.wait();
        token.cancel();
        resume.wait();
    });
    let err = citadel_sql::executor::execute_with_read(&mut rtx, &schema, &stmt, &[])
        .expect_err("the in-flight scan ignored cancellation");
    stopper.join().unwrap();
    assert!(is_cancelled(&err), "got {err:?}");
}

/// A cancel already in place refuses immediately rather than running first.
///
/// The connection is opened before the token is tripped: `Connection::open`
/// loads the schema through a scan, so a token set first stops the open.
#[test]
fn an_already_cancelled_handle_refuses_the_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let err = conn.query("SELECT * FROM t").unwrap_err();

    assert!(is_cancelled(&err), "got {err:?}");
}

#[test]
fn an_already_cancelled_begin_does_not_take_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let err = conn.execute("BEGIN").expect_err("BEGIN should be refused");
    assert!(is_cancelled(&err), "got {err:?}");
    assert!(!conn.in_transaction());

    db.set_cancel(None);
    conn.execute("BEGIN").unwrap();
    conn.execute("ROLLBACK").unwrap();
}

/// The other half of that: opening a connection is work, and work stops too.
#[test]
fn opening_a_connection_is_itself_cancellable() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let Err(err) = Connection::open(&db) else {
        panic!("opening a connection ignored the cancel");
    };

    assert!(is_cancelled(&err), "got {err:?}");
}

/// A pre-cancelled UPDATE is refused before it can change a row.
#[test]
fn an_update_stops_and_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let err = conn.execute("UPDATE t SET tag = 'clobbered'").unwrap_err();
    assert!(is_cancelled(&err), "got {err:?}");

    db.set_cancel(None);
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .query("SELECT COUNT(*) FROM t WHERE tag = 'clobbered'")
        .unwrap();
    assert_eq!(
        rows.rows[0][0],
        Value::Integer(0),
        "the cancelled UPDATE left rows behind"
    );
}

/// Clearing the token restores ordinary behaviour, so a cancel is not sticky
/// for the life of the handle.
#[test]
fn clearing_the_token_lets_queries_run_again() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let error = conn.query("SELECT * FROM t LIMIT 1").unwrap_err();
    assert!(is_cancelled(&error), "got {error:?}");

    db.set_cancel(None);
    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(ROWS));
}

/// The default path is untouched: no token, no behaviour change.
#[test]
fn a_handle_with_no_token_behaves_exactly_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());

    let conn = Connection::open(&db).unwrap();
    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();

    assert_eq!(rows.rows[0][0], Value::Integer(ROWS));
}

/// An explicit transaction outlives the statement, so a cancelled INSERT that
/// already mutated its write set must not remain committable. The discriminating
/// step is clearing the token before COMMIT: only the transaction failure state
/// can refuse it then.
#[test]
fn an_interrupted_statement_poisons_its_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE copy (id INTEGER PRIMARY KEY, tag TEXT)")
        .unwrap();
    const COPY: &str = "INSERT INTO copy (id, tag) SELECT id, tag FROM t";

    // Measured rather than guessed: the sweep has to cover this machine's
    // timing, and a hardcoded delay either lands before the loop every time or
    // after it every time.
    conn.execute("BEGIN").unwrap();
    let start = std::time::Instant::now();
    conn.execute(COPY).unwrap();
    let full = start.elapsed();
    conn.execute("ROLLBACK").unwrap();

    let mut observed_poison = false;
    for step in 1..20 {
        conn.execute("BEGIN").unwrap();

        let token = CancelToken::new();
        db.set_cancel(Some(token.clone()));
        let delay = full.mul_f64(f64::from(step) / 20.0);
        let stopper = std::thread::spawn(move || {
            std::thread::sleep(delay);
            token.cancel();
        });
        let outcome = conn.execute(COPY);
        stopper.join().unwrap();
        db.set_cancel(None);

        match outcome {
            Err(ref e) if is_cancelled(e) => {
                // Refused at the door, or observed after the write transaction
                // mutated. Only the latter advances the marker and poisons.
                let followup = conn.query("SELECT 1");
                match conn.execute("COMMIT") {
                    Err(e) => {
                        assert!(is_cancelled(&e), "got {e:?}");
                        let followup =
                            followup.expect_err("poisoned transaction admitted a later statement");
                        assert!(
                            is_cancelled(&followup),
                            "poisoned transaction returned {followup:?}"
                        );
                        observed_poison = true;
                        break;
                    }
                    Ok(_) => {
                        followup.expect("unpoisoned transaction refused a later statement");
                    }
                }
            }
            Err(e) => panic!("unexpected error {e:?}"),
            // The cancel arrived after the statement had finished.
            Ok(_) => conn.execute("ROLLBACK").map(|_| ()).unwrap(),
        }
    }
    assert!(
        observed_poison,
        "no attempt observed cancellation after the insert mutated"
    );

    // The same connection, not a fresh one: a refused COMMIT must leave it
    // usable, and reopening would hide that.
    let rows = conn.query("SELECT COUNT(*) FROM copy").unwrap();
    assert_eq!(
        rows.rows[0][0],
        Value::Integer(0),
        "a refused commit still let the interrupted prefix through"
    );
}

/// A prepared statement with a compiled plan does not go through `dispatch`,
/// so it needs the token read onto its transaction in its own right. Opening
/// the transaction before the token exists is the arrangement that catches it:
/// a lane that trusts the copy taken at BEGIN sees `None` and runs.
#[test]
fn a_prepared_statement_reads_the_token_installed_after_its_transaction_began() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let stmt = conn.prepare("UPDATE t SET tag = $1 WHERE id = $2").unwrap();

    conn.execute("BEGIN").unwrap();
    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let err = stmt
        .execute(&[Value::Text("clobbered".into()), Value::Integer(1)])
        .expect_err("the prepared update must be refused");
    assert!(is_cancelled(&err), "got {err:?}");

    db.set_cancel(None);
    conn.execute("COMMIT").unwrap();

    let rows = conn.query("SELECT tag FROM t WHERE id = 1").unwrap();
    assert_eq!(rows.rows[0][0], Value::Text("tag1".into()));
}

/// An INSERT inside an explicit transaction takes an arm of its own that never
/// reaches the executor's entry check, and a VALUES list never reaches a scan.
#[test]
fn an_explicit_transaction_insert_is_refused_by_an_already_cancelled_token() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let err = conn
        .execute("INSERT INTO t (id, tag) VALUES (999999, 'new')")
        .expect_err("the insert must be refused");
    assert!(is_cancelled(&err), "got {err:?}");

    // Refused before it ran, so nothing was applied and the transaction is
    // intact: committing it is allowed, and must carry nothing.
    db.set_cancel(None);
    conn.execute("COMMIT").unwrap();

    let rows = conn
        .query("SELECT COUNT(*) FROM t WHERE id = 999999")
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(0));
}

/// A refused COMMIT ends the transaction as surely as a successful one, so the
/// connection must not go on holding what that transaction was carrying.
#[test]
fn a_refused_commit_leaves_the_connection_clean() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE staged (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    // A savepoint captures its snapshot lazily, when a mutating statement
    // follows it. Without one there would be nothing behind the name and the
    // hazard below would be a lookup rather than a restore.
    conn.execute("INSERT INTO staged (id) VALUES (1)").unwrap();

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));
    let err = conn
        .execute("COMMIT")
        .expect_err("the commit must be refused");
    assert!(is_cancelled(&err), "got {err:?}");
    db.set_cancel(None);

    // The sharp one. The savepoint belonged to the transaction that just ended;
    // carried into the NEXT one it stays addressable, and `ROLLBACK TO` would
    // restore a snapshot taken inside a different transaction.
    conn.execute("BEGIN").unwrap();
    assert!(
        conn.execute("ROLLBACK TO s").is_err(),
        "a savepoint outlived its transaction and is addressable in the next one"
    );
    conn.execute("ROLLBACK").unwrap();

    // The table was created inside the transaction that just rolled back, so a
    // connection still offering it is describing something that is not there.
    assert!(
        conn.query("SELECT * FROM staged").is_err(),
        "the rolled-back table survived in the connection's schema"
    );
    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(ROWS));
}

/// ROLLBACK is the caller doing exactly what the cancel asked for. Refusing it
/// under a tripped token would leave a cancelled connection with an open
/// transaction and no statement able to close it.
#[test]
fn a_cancelled_transaction_can_still_be_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, tag) VALUES (999999, 'new')")
        .unwrap();

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    conn.execute("ROLLBACK").unwrap();
    db.set_cancel(None);

    let rows = conn
        .query("SELECT COUNT(*) FROM t WHERE id = 999999")
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(0));
}

/// Statements that never enter a scan loop. Each reaches its answer - and for
/// the writes, its commit - from metadata, a cache or a single point lookup, so
/// a check that lived only inside scan loops never ran for any of them.
#[test]
fn an_already_cancelled_token_stops_the_paths_that_never_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());

    for sql in [
        "SELECT 1",
        "SELECT COUNT(*) FROM t",
        "INSERT INTO t (id, tag) VALUES (999999, 'new')",
        "UPDATE t SET tag = 'point' WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
    ] {
        let conn = Connection::open(&db).unwrap();
        let token = CancelToken::new();
        token.cancel();
        db.set_cancel(Some(token));

        let err = conn.execute(sql).expect_err("should refuse: {sql}");
        assert!(is_cancelled(&err), "{sql} gave {err:?}");
        db.set_cancel(None);
    }

    // None of the writes above may have landed.
    let conn = Connection::open(&db).unwrap();
    for (sql, want) in [
        ("SELECT COUNT(*) FROM t WHERE id = 999999", 0),
        ("SELECT COUNT(*) FROM t WHERE tag = 'point'", 0),
        ("SELECT COUNT(*) FROM t WHERE id = 2", 1),
    ] {
        assert_eq!(
            conn.query(sql).unwrap().rows[0][0],
            Value::Integer(want),
            "{sql}"
        );
    }
}

/// The second execution is served by the compiled plan's materialized-result
/// cache. It must still pass through the connection's cancellation guard.
#[test]
fn an_already_cancelled_token_stops_a_warmed_result_cache_hit() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    const SQL: &str = "SELECT id, tag FROM t WHERE id = 1";

    let warmed = conn.query(SQL).unwrap();
    assert_eq!(warmed.rows.len(), 1);

    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));

    let err = conn
        .query(SQL)
        .expect_err("the warmed cache ignored cancellation");
    assert!(is_cancelled(&err), "got {err:?}");
}

// A cancelled sort is measured where it can be driven deterministically, in
// `executor::select_tests`: a query-level cancel cannot be made to land in the
// comparator rather than in the scan ahead of it, so a test here would report
// zero while proving nothing.
