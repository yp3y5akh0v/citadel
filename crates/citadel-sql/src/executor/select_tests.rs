use super::*;
use crate::eval::ColumnMap;
use crate::parser::{Expr, SelectColumn, SelectStmt};
use crate::types::{Collation, ColumnDef, DataType, ExecutionResult, Value};

fn col(name: &str, dt: DataType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable: true,
        position: 0,
        default_expr: None,
        default_sql: None,
        check_expr: None,
        check_sql: None,
        check_name: None,
        is_with_timezone: false,
        generated_expr: None,
        generated_sql: None,
        generated_kind: None,
        collation: Collation::Binary,
    }
}

fn cols(specs: &[(&str, DataType)]) -> Vec<ColumnDef> {
    specs
        .iter()
        .enumerate()
        .map(|(i, (n, t))| {
            let mut c = col(n, *t);
            c.position = i as u16;
            c
        })
        .collect()
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn empty_select(from: &str) -> SelectStmt {
    SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: from.into(),
        from_alias: None,
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: false,
        where_clause: None,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by: vec![],
        having: None,
    }
}

#[test]
fn compute_scan_limit_none_when_no_limit() {
    let s = empty_select("t");
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn compute_scan_limit_simple_limit() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    assert_eq!(compute_scan_limit(&s), Some(10));
}

#[test]
fn compute_scan_limit_with_offset_adds() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(5)));
    s.offset = Some(Expr::Literal(i(3)));
    assert_eq!(compute_scan_limit(&s), Some(8));
}

#[test]
fn compute_scan_limit_none_with_order_by() {
    use crate::parser::OrderByItem;
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.order_by = vec![OrderByItem {
        expr: Expr::Column("x".into()),
        descending: false,
        nulls_first: None,
    }];
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn compute_scan_limit_none_with_group_by() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.group_by = vec![Expr::Column("x".into())];
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn compute_scan_limit_none_with_distinct() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.distinct = true;
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn try_count_star_shortcut_matches_select_count_star() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: None,
    }];
    let r = try_count_star_shortcut(&s, || Ok(42)).unwrap();
    assert!(matches!(
        r,
        Some(ExecutionResult::Query(q)) if q.rows[0][0] == i(42)
    ));
}

#[test]
fn try_count_star_shortcut_rejects_where_clause() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: None,
    }];
    s.where_clause = Some(Expr::Literal(Value::Boolean(true)));
    let r = try_count_star_shortcut(&s, || Ok(1)).unwrap();
    assert!(r.is_none());
}

#[test]
fn try_count_star_shortcut_rejects_extra_columns() {
    let mut s = empty_select("t");
    s.columns = vec![
        SelectColumn::Expr {
            expr: Expr::CountStar,
            alias: None,
        },
        SelectColumn::Expr {
            expr: Expr::Column("x".into()),
            alias: None,
        },
    ];
    let r = try_count_star_shortcut(&s, || Ok(1)).unwrap();
    assert!(r.is_none());
}

#[test]
fn try_count_star_shortcut_uses_alias() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: Some("n".into()),
    }];
    let r = try_count_star_shortcut(&s, || Ok(7)).unwrap();
    if let Some(ExecutionResult::Query(q)) = r {
        assert_eq!(q.columns[0], "n");
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn resolve_simple_col_unqualified_resolves() {
    let cs = cols(&[("a", DataType::Integer), ("b", DataType::Text)]);
    let cm = ColumnMap::new(&cs);
    assert_eq!(resolve_simple_col(&Expr::Column("a".into()), &cm), Some(0));
    assert_eq!(resolve_simple_col(&Expr::Column("b".into()), &cm), Some(1));
}

#[test]
fn resolve_simple_col_unknown_returns_none() {
    let cs = cols(&[("a", DataType::Integer)]);
    let cm = ColumnMap::new(&cs);
    assert_eq!(
        resolve_simple_col(&Expr::Column("missing".into()), &cm),
        None
    );
}

#[test]
fn resolve_simple_col_non_column_returns_none() {
    let cs = cols(&[("a", DataType::Integer)]);
    let cm = ColumnMap::new(&cs);
    assert_eq!(resolve_simple_col(&Expr::Literal(i(1)), &cm), None);
}

fn agg_select_stmt(sql: &str) -> SelectStmt {
    match crate::parser::parse_sql(sql).unwrap() {
        crate::parser::Statement::Select(q) => match q.body {
            crate::parser::QueryBody::Select(s) => *s,
            _ => panic!("expected a plain select"),
        },
        _ => panic!("expected a select"),
    }
}

fn agg_rows(plan: StreamAggPlan, states: Vec<AggState>) -> Vec<Vec<Value>> {
    match plan.finish(states) {
        ExecutionResult::Query(q) => q.rows,
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn merge_sum_matches_serial_feed() {
    let op = StreamAgg::Sum(1);
    let mut serial = AggState::new(&op);
    serial.feed_val(&Value::Integer(i64::MAX - 1)).unwrap();
    serial.feed_val(&Value::Integer(1)).unwrap();

    let mut left = AggState::new(&op);
    left.feed_val(&Value::Integer(i64::MAX - 1)).unwrap();
    let mut right = AggState::new(&op);
    right.feed_val(&Value::Integer(1)).unwrap();
    left.merge(right);

    assert_eq!(left.finish(), serial.finish());
}

/// Overflow behavior is profile-dependent (`+=` panics in debug, wraps in
/// release); the merge must diverge from serial feeding in NEITHER profile.
#[test]
fn merge_sum_overflow_parity_with_serial_feed() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let op = StreamAgg::Sum(1);

    let serial = catch_unwind(AssertUnwindSafe(|| {
        let mut s = AggState::new(&op);
        s.feed_val(&Value::Integer(i64::MAX)).unwrap();
        s.feed_val(&Value::Integer(1)).unwrap();
        s.finish()
    }));
    let merged = catch_unwind(AssertUnwindSafe(|| {
        let mut left = AggState::new(&op);
        left.feed_val(&Value::Integer(i64::MAX)).unwrap();
        let mut right = AggState::new(&op);
        right.feed_val(&Value::Integer(1)).unwrap();
        left.merge(right);
        left.finish()
    }));
    match (serial, merged) {
        (Ok(a), Ok(b)) => assert_eq!(a, b),
        (Err(_), Err(_)) => {}
        (a, b) => panic!(
            "divergent overflow behavior: serial_ok={} merged_ok={}",
            a.is_ok(),
            b.is_ok()
        ),
    }
}

#[test]
fn merge_min_max_keep_left_on_tie() {
    let mut left = AggState::Min(Some(Value::Integer(3)));
    left.merge(AggState::Min(Some(Value::Integer(3))));
    assert_eq!(left.finish(), Value::Integer(3));

    let mut left = AggState::Max(Some(Value::Text("b".into())));
    left.merge(AggState::Max(Some(Value::Text("a".into()))));
    assert_eq!(left.finish(), Value::Text("b".into()));

    let mut left = AggState::Min(None);
    left.merge(AggState::Min(Some(Value::Integer(7))));
    assert_eq!(left.finish(), Value::Integer(7));
}

#[test]
fn merge_counts_add() {
    let mut a = AggState::CountStar(41);
    a.merge(AggState::CountStar(1));
    assert_eq!(a.finish(), Value::Integer(42));
    let mut a = AggState::Count(10);
    a.merge(AggState::Count(5));
    assert_eq!(a.finish(), Value::Integer(15));
}

#[cfg(not(target_arch = "wasm32"))]
mod parallel {
    use super::*;
    use crate::connection::Connection;

    fn agg_db(dir: &std::path::Path) -> citadel::Database {
        citadel::DatabaseBuilder::new(dir.join("agg.db"))
            .passphrase(b"x")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create()
            .unwrap()
    }

    #[test]
    fn parallel_sharded_agg_matches_serial() {
        let dir = tempfile::tempdir().unwrap();
        let db = agg_db(dir.path());
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, s TEXT)")
                .unwrap();
            conn.execute("BEGIN").unwrap();
            let ins = conn.prepare("INSERT INTO t VALUES ($1, $2, $3)").unwrap();
            for k in 0..3000i64 {
                let v = if k % 7 == 0 {
                    Value::Null
                } else {
                    Value::Integer(k * 3 - 1000)
                };
                // A few overflow-sized values so shards materialize overflow chains.
                let s = if k % 501 == 0 {
                    Value::Text(format!("big{k}{}", "x".repeat(9000)).into())
                } else {
                    Value::Text(format!("s{k:05}").into())
                };
                ins.execute(&[Value::Integer(k), v, s]).unwrap();
            }
            conn.execute("COMMIT").unwrap();
        }

        let schema = crate::schema::SchemaManager::load(&db).unwrap();
        let table_schema = schema.get("t").unwrap();
        let stmt = agg_select_stmt(
            "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), MIN(s), MAX(s) FROM t",
        );

        let serial_rows = {
            let plan = StreamAggPlan::try_new(&stmt, table_schema)
                .unwrap()
                .unwrap();
            assert!(plan.parallel_ok);
            let mut rtx = db.begin_read();
            let leaves = rtx.collect_table_leaves(b"t").unwrap();
            assert!(
                leaves.len() >= 8,
                "want multiple leaves, got {}",
                leaves.len()
            );
            let mut states: Vec<AggState> =
                plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
            let mut err = None;
            rtx.scan_leaves(&leaves, |k, v| {
                plan.feed_row_raw(k, v, &mut states, &mut err)
            })
            .unwrap();
            assert!(err.is_none());
            agg_rows(plan, states)
        };

        for shard_leaves in [1usize, 2, 3, 7] {
            let plan = StreamAggPlan::try_new(&stmt, table_schema)
                .unwrap()
                .unwrap();
            let mut rtx = db.begin_read();
            let leaves = rtx.collect_table_leaves(b"t").unwrap();
            let states = parallel_stream_agg_sharded(&rtx, &plan, &leaves, shard_leaves).unwrap();
            assert_eq!(
                agg_rows(plan, states),
                serial_rows,
                "shard size {shard_leaves}"
            );
        }
    }

    #[test]
    fn parallel_gate_excludes_order_sensitive_ops() {
        let dir = tempfile::tempdir().unwrap();
        let db = agg_db(dir.path());
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, v INTEGER, r REAL, iv INTERVAL)",
            )
            .unwrap();
        }
        let schema = crate::schema::SchemaManager::load(&db).unwrap();
        let table_schema = schema.get("t").unwrap();

        let ok = |sql: &str| {
            StreamAggPlan::try_new(&agg_select_stmt(sql), table_schema)
                .unwrap()
                .unwrap()
                .parallel_ok
        };
        assert!(ok(
            "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v) FROM t"
        ));
        assert!(!ok("SELECT AVG(v) FROM t"));
        assert!(!ok("SELECT SUM(r) FROM t"));
        assert!(!ok("SELECT MIN(r) FROM t"));
        assert!(!ok("SELECT MAX(r) FROM t"));
        assert!(!ok("SELECT SUM(iv) FROM t"));
        assert!(!ok("SELECT MIN(iv) FROM t"));
    }
}
