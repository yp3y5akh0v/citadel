use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

#[test]
fn unchosen_closed_set_subqueries_preserve_trigger_refresh() {
    for expression in [
        "v + 1",
        "CASE WHEN id > 0 THEN v + 1 ELSE (SELECT 99) END",
        "COALESCE(v + 1, (SELECT 99))",
    ] {
        for begin in [None, Some("BEGIN")] {
            let database = DatabaseBuilder::new("")
                .passphrase(b"conditional-update-refresh")
                .argon2_profile(Argon2Profile::Iot)
                .create_in_memory()
                .unwrap();
            let connection = Connection::open(&database).unwrap();
            connection
                .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
                .unwrap();
            connection
                .execute("INSERT INTO t VALUES (1,10),(2,20)")
                .unwrap();
            connection
                .execute(
                    "CREATE TRIGGER bump_later AFTER UPDATE ON t FOR EACH ROW \
                     WHEN NEW.id = 1 BEGIN UPDATE t SET v = v + 100 WHERE id = 2; END",
                )
                .unwrap();
            if let Some(begin) = begin {
                connection.execute(begin).unwrap();
            }

            let sql = format!("UPDATE t SET v = {expression}");
            connection
                .execute(&sql)
                .unwrap_or_else(|error| panic!("{begin:?}: {sql}: {error}"));

            // The second row's normal RHS observes the preceding trigger's
            // update. Neither closed subquery is demanded by either row.
            let expected = vec![
                vec![Value::Integer(1), Value::Integer(11)],
                vec![Value::Integer(2), Value::Integer(121)],
            ];
            assert_eq!(
                connection
                    .query("SELECT id,v FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                expected,
                "{begin:?}: {sql}",
            );
            if begin.is_some() {
                connection.execute("COMMIT").unwrap();
                assert_eq!(
                    connection
                        .query("SELECT id,v FROM t ORDER BY id")
                        .unwrap()
                        .rows,
                    expected,
                    "committed: {sql}",
                );
            }
        }
    }
}
