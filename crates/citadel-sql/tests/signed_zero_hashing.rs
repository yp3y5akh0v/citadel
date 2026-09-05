use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

#[test]
fn hash_grouping_and_joins_treat_signed_zeros_as_equal() {
    let db = DatabaseBuilder::new("")
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, number REAL)")
        .unwrap();
    for (id, number) in [(1, 0.0), (2, -0.0)]
        .into_iter()
        .chain((3..=130).map(|id| (id, (id - 2) as f64)))
    {
        conn.execute_params(
            "INSERT INTO items VALUES ($1, $2)",
            &[Value::Integer(id), Value::Real(number)],
        )
        .unwrap();
    }
    let grouped = conn
        .query("SELECT number, COUNT(*) FROM items GROUP BY number ORDER BY number")
        .unwrap();
    let expected_groups = std::iter::once(vec![Value::Real(0.0), Value::Integer(2)])
        .chain((1..=128).map(|n| vec![Value::Real(n as f64), Value::Integer(1)]))
        .collect::<Vec<_>>();
    assert_eq!(grouped.rows, expected_groups);
    let joined = conn
        .query(
            "SELECT a.id, b.id FROM items a JOIN items b ON a.number = b.number \
             ORDER BY a.id, b.id",
        )
        .unwrap();
    assert_eq!(
        joined.rows,
        [(1, 1), (1, 2), (2, 1), (2, 2)]
            .into_iter()
            .chain((3..=130).map(|id| (id, id)))
            .map(|(a, b)| vec![Value::Integer(a), Value::Integer(b)])
            .collect::<Vec<_>>()
    );
}
