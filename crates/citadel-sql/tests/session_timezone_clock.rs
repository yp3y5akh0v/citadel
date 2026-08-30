use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::parser::{Statement, TimezoneValue};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"session-timezone-clock")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query_row(connection: &Connection<'_>) -> Vec<Value> {
    connection
        .query(
            "SELECT CURRENT_TIMESTAMP, LOCALTIMESTAMP, CURRENT_DATE, \
                    CURRENT_TIME, LOCALTIME, TRANSACTION_TIMESTAMP(), \
                    STATEMENT_TIMESTAMP()",
        )
        .unwrap()
        .rows
        .remove(0)
}

fn assert_local_fields(row: &[Value], offset_micros: i64) {
    let (Value::Timestamp(timestamp), Value::Timestamp(local)) = (&row[0], &row[1]) else {
        panic!("expected current and local timestamps, got {row:?}");
    };
    assert_eq!(*local - *timestamp, offset_micros);
    assert_eq!(
        row[2],
        Value::Date(citadel_sql::datetime::ts_split(*local).0)
    );
    assert_eq!(
        row[3],
        Value::Time(citadel_sql::datetime::ts_split(*local).1)
    );
    assert_eq!(row[4], row[3]);
    if row.len() > 5 {
        assert_eq!(row[5], row[0]);
    }
}

#[test]
fn current_fields_use_the_session_zone_and_the_transaction_clock() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    connection.execute("SET TIME ZONE '+10:00'").unwrap();
    connection.execute("BEGIN").unwrap();

    let first = query_row(&connection);
    assert_local_fields(&first, 10 * citadel_sql::datetime::MICROS_PER_HOUR);
    let aliases = connection
        .query(
            "SELECT DATE(), DATE('now'), CURRENT_DATE, TIME(), TIME('now'), \
                    CURRENT_TIME, DATETIME(), DATETIME('now'), LOCALTIMESTAMP",
        )
        .unwrap();
    let aliases = &aliases.rows[0];
    assert_eq!(aliases[0], aliases[2]);
    assert_eq!(aliases[1], aliases[2]);
    assert_eq!(aliases[3], aliases[5]);
    assert_eq!(aliases[4], aliases[5]);
    assert_eq!(aliases[6], aliases[8]);
    assert_eq!(aliases[7], aliases[8]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let second = query_row(&connection);
    assert_eq!(second[..6], first[..6]);
    let (Value::Timestamp(first_statement), Value::Timestamp(second_statement)) =
        (&first[6], &second[6])
    else {
        panic!("expected statement timestamps");
    };
    assert!(second_statement > first_statement);

    connection.execute("SET TIME ZONE '-07:00'").unwrap();
    let shifted = query_row(&connection);
    assert_eq!(shifted[0], first[0]);
    assert_local_fields(&shifted, -7 * citadel_sql::datetime::MICROS_PER_HOUR);
    connection.execute("COMMIT").unwrap();
    assert_eq!(connection.session_timezone(), "-07:00");
}

#[test]
fn current_time_precision_is_rounded_and_range_checked() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    let row = connection
        .query(
            "SELECT CURRENT_TIMESTAMP(3), LOCALTIMESTAMP(3), \
                    CURRENT_TIME(3), LOCALTIME(3)",
        )
        .unwrap()
        .rows
        .remove(0);
    for value in row {
        let micros = match value {
            Value::Timestamp(micros) | Value::Time(micros) => micros,
            other => panic!("expected a rounded temporal value, got {other:?}"),
        };
        assert_eq!(micros.rem_euclid(1_000), 0);
    }
    assert!(connection.query("SELECT CURRENT_TIMESTAMP(7)").is_err());
}

#[test]
fn timezone_local_value_is_distinct_from_set_local_scope() {
    let plain = citadel_sql::parser::parse_sql("SET TIME ZONE LOCAL").unwrap();
    assert!(matches!(
        plain,
        Statement::SetTimezone {
            zone: TimezoneValue::Local,
            local: false
        }
    ));
    let scoped = citadel_sql::parser::parse_sql("SET LOCAL TIME ZONE '+05:00'").unwrap();
    assert!(matches!(
        scoped,
        Statement::SetTimezone {
            zone: TimezoneValue::Named(ref zone),
            local: true
        } if zone == "+05:00"
    ));

    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    connection.execute("SET TIME ZONE '+05:00'").unwrap();
    connection.execute("SET TIME ZONE LOCAL").unwrap();
    assert_eq!(connection.session_timezone(), "UTC");

    connection.execute("SET TIME ZONE '+05:00'").unwrap();
    connection.execute("BEGIN").unwrap();
    connection.execute("SET LOCAL TIME ZONE LOCAL").unwrap();
    assert_eq!(connection.session_timezone(), "UTC");
    connection.execute("COMMIT").unwrap();
    assert_eq!(connection.session_timezone(), "+05:00");

    assert!(connection.execute("SET TIME ZONE 'LOCAL'").is_err());
    assert_eq!(connection.session_timezone(), "+05:00");

    assert!(matches!(
        connection.execute("SET LOCAL TIME ZONE UTC"),
        Err(citadel_sql::SqlError::NoActiveTransaction)
    ));
    assert_eq!(connection.session_timezone(), "+05:00");
}

#[test]
fn numeric_and_interval_timezone_values_use_postgresql_hour_semantics() {
    for (sql, seconds, displayed) in [
        ("SET TIME ZONE -7", -7 * 3_600, "-07:00"),
        ("SET TIME ZONE 5.5", 5 * 3_600 + 30 * 60, "+05:30"),
        ("SET TIME ZONE 5.3", 5 * 3_600 + 18 * 60, "+05:18"),
        ("SET TIME ZONE 0.000277777777777777777777", 0, "+00:00"),
        ("SET TIME ZONE 0.000277777777777777777778", 1, "+00:00:01"),
        (
            "SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE",
            -8 * 3_600,
            "-08:00",
        ),
        ("SET TIME ZONE INTERVAL '1.999999 seconds'", 1, "+00:00:01"),
        (
            "SET TIME ZONE INTERVAL '-1.999999 seconds'",
            -1,
            "-00:00:01",
        ),
    ] {
        let parsed = citadel_sql::parser::parse_sql(sql).unwrap();
        assert!(matches!(
            parsed,
            Statement::SetTimezone {
                zone: TimezoneValue::OffsetSeconds(actual),
                local: false
            } if actual == seconds
        ));

        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let connection = Connection::open(&db).unwrap();
        connection.execute(sql).unwrap();
        assert_eq!(connection.session_timezone(), displayed);
    }

    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    assert!(connection
        .execute("SET TIME ZONE INTERVAL '1 day'")
        .is_err());
    assert!(connection.execute("SET TIME ZONE 16").is_err());
    assert!(connection.execute("SET TIME ZONE '+16:00'").is_err());
    assert_eq!(connection.session_timezone(), "UTC");
}

#[test]
fn execute_batch_commits_or_restores_timezone_state_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    connection.execute("SET TIME ZONE '+01:00'").unwrap();

    connection
        .execute_batch("SET TIME ZONE '+02:00'; SELECT LOCALTIMESTAMP")
        .unwrap();
    assert_eq!(connection.session_timezone(), "+02:00");

    let failure = connection
        .execute_batch("SET TIME ZONE '+03:00'; SELECT * FROM deliberately_missing_table");
    assert!(failure.is_err());
    assert_eq!(connection.session_timezone(), "+02:00");

    let results = connection
        .execute_batch(
            "SET LOCAL TIME ZONE '+09:00'; \
             SELECT CURRENT_TIMESTAMP, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIME",
        )
        .unwrap();
    let ExecutionResult::Query(query) = &results[1] else {
        panic!("expected the batch query result, got {:?}", results[1]);
    };
    assert_local_fields(&query.rows[0], 9 * citadel_sql::datetime::MICROS_PER_HOUR);
    assert_eq!(connection.session_timezone(), "+02:00");
}

#[test]
fn execute_script_stops_without_losing_prior_timezone_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();

    let script = connection.execute_script(
        "SET TIME ZONE '+05:00'; \
         SELECT * FROM deliberately_missing_table; \
         SET TIME ZONE '+09:00'",
    );
    assert_eq!(script.completed.len(), 1);
    assert!(script.error.is_some());
    assert_eq!(connection.session_timezone(), "+05:00");

    let transactional = connection.execute_script(
        "BEGIN; SET TIME ZONE '+08:00'; \
         SELECT * FROM deliberately_missing_table; COMMIT",
    );
    assert_eq!(transactional.completed.len(), 2);
    assert!(transactional.error.is_some());
    assert!(connection.in_transaction());
    assert_eq!(connection.session_timezone(), "+08:00");
    connection.execute("ROLLBACK").unwrap();
    assert_eq!(connection.session_timezone(), "+05:00");
}
