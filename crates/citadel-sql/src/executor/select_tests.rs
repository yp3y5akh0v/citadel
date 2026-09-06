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

fn scan_limit_schema() -> crate::types::TableSchema {
    crate::types::TableSchema::new(
        "t".into(),
        cols(&[("id", DataType::Integer), ("x", DataType::Integer)]),
        vec![0],
        vec![],
        vec![],
        vec![],
    )
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

mod owned_projection {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(
            "t".into(),
            cols(&[
                ("id", DataType::Integer),
                ("payload", DataType::Blob),
                ("label", DataType::Text),
            ]),
            vec![0],
            vec![],
            vec![],
            vec![],
        )
    }

    fn projection(sql: &str) -> StreamProj {
        build_stream_proj(&agg_select_stmt(sql).columns, &schema()).unwrap()
    }

    fn row() -> Vec<Value> {
        vec![
            i(7),
            Value::Blob(vec![0x5a; 16 * 1024]),
            Value::Text("label".repeat(1024).into()),
        ]
    }

    fn blob_ptr(value: &Value) -> *const u8 {
        match value {
            Value::Blob(bytes) => bytes.as_ptr(),
            _ => panic!("expected Blob"),
        }
    }

    fn text_ptr(value: &Value) -> *const u8 {
        match value {
            Value::Text(text) => text.as_str().as_ptr(),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn identity_transfers_row_and_large_value_allocations() {
        let mut input = row();
        let row_ptr = input.as_ptr();
        let payload_ptr = blob_ptr(&input[1]);
        let label_ptr = text_ptr(&input[2]);
        let output = projection("SELECT * FROM t")
            .project_decoded(&mut input, None)
            .unwrap();
        assert_eq!(
            output.as_ptr(),
            row_ptr,
            "identity must transfer the Vec, not clone it"
        );
        assert_eq!(blob_ptr(&output[1]), payload_ptr);
        assert_eq!(text_ptr(&output[2]), label_ptr);
        assert!(input.is_empty());
    }

    #[test]
    fn unique_columns_move_values_and_retain_scratch_allocation() {
        let mut input = row();
        let scratch_ptr = input.as_ptr();
        let payload_ptr = blob_ptr(&input[1]);
        let label_ptr = text_ptr(&input[2]);
        let output = projection("SELECT label AS name, payload FROM t")
            .project_decoded(&mut input, None)
            .unwrap();
        assert_eq!(text_ptr(&output[0]), label_ptr, "large Text must move");
        assert_eq!(blob_ptr(&output[1]), payload_ptr, "large Blob must move");
        assert_eq!(input.as_ptr(), scratch_ptr);
        assert_eq!(input, vec![i(7), Value::Null, Value::Null]);
    }

    #[test]
    fn duplicates_keep_every_selected_value() {
        let mut input = row();
        let expected = vec![
            input[1].clone(),
            input[2].clone(),
            input[1].clone(),
            input[2].clone(),
        ];
        let proj = projection("SELECT payload, label, payload AS again, label AS name FROM t");
        assert!(matches!(proj, StreamProj::Columns { unique: false, .. }));
        assert_eq!(proj.project_decoded(&mut input, None).unwrap(), expected);
    }

    #[test]
    fn expressions_share_the_original_row_and_propagate_errors() {
        let mut input = row();
        let expected = vec![
            input[2].clone(),
            Value::Text("label".repeat(2048).into()),
            i(14),
        ];
        let output = projection("SELECT label, label || label, id + id FROM t")
            .project_decoded(&mut input, None)
            .unwrap();
        assert_eq!(output, expected);
        let error = projection("SELECT id / (id - id) FROM t")
            .project_decoded(&mut input, None)
            .unwrap_err();
        assert!(matches!(error, SqlError::DivisionByZero));
    }

    #[test]
    fn cancellation_precedes_projection_or_value_moves() {
        let cancel = CancelToken::new();
        cancel.cancel();
        for sql in [
            "SELECT * FROM t",
            "SELECT label, payload FROM t",
            "SELECT payload, payload FROM t",
            "SELECT id + id FROM t",
        ] {
            let mut input = row();
            let expected = input.clone();
            let error = projection(sql)
                .project_decoded(&mut input, Some(&cancel))
                .unwrap_err();
            assert!(
                matches!(error, SqlError::Storage(citadel_core::Error::Interrupted)),
                "{sql}: {error}"
            );
            assert_eq!(input, expected, "cancelled projection must not move values");
        }
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn compiled_point_and_filter_lanes_preserve_projection_results() {
        use crate::connection::Connection;
        let dir = tempfile::tempdir().unwrap();
        let db = citadel::DatabaseBuilder::new(dir.path().join("projection.db"))
            .passphrase(b"x")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB, label TEXT)")
            .unwrap();
        let original = row();
        conn.execute_params("INSERT INTO t VALUES ($1, $2, $3)", &original)
            .unwrap();
        conn.execute_params(
            "INSERT INTO t VALUES ($1, $2, $3)",
            &[i(8), Value::Blob(vec![1]), Value::Text("other".into())],
        )
        .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN added TEXT DEFAULT 'legacy'")
            .unwrap();
        let schema = SchemaManager::load(&db).unwrap();
        for predicate in ["id = 7", "id >= 7 AND id < 8"] {
            for (select, expected) in [
                (
                    "*",
                    vec![
                        i(7),
                        original[1].clone(),
                        original[2].clone(),
                        Value::Text("legacy".into()),
                    ],
                ),
                (
                    "label AS name, payload",
                    vec![original[2].clone(), original[1].clone()],
                ),
                (
                    "payload, payload AS again, added",
                    vec![
                        original[1].clone(),
                        original[1].clone(),
                        Value::Text("legacy".into()),
                    ],
                ),
                ("id + id AS twice, id - 1 AS previous", vec![i(14), i(6)]),
            ] {
                let sql = format!("SELECT {select} FROM t WHERE {predicate}");
                let lane = build_select_lane(&schema, &agg_select_stmt(&sql))
                    .expect("must use compiled lane");
                assert_eq!(
                    matches!(lane, CompiledSelectLane::Point(_)),
                    predicate == "id = 7"
                );
                let actual = lane.run(&mut db.begin_read()).unwrap();
                assert_eq!(actual.rows, vec![expected], "{sql}");
            }
        }
        let missing = build_select_lane(
            &schema,
            &agg_select_stmt("SELECT label FROM t WHERE id = 404"),
        )
        .unwrap();
        assert!(missing.run(&mut db.begin_read()).unwrap().rows.is_empty());
    }
}

mod strategy {
    use super::*;
    use crate::executor::select::{choose_strategy, Strategy};
    use crate::parser::OrderByItem;

    fn schema() -> crate::types::TableSchema {
        scan_limit_schema()
    }

    fn count_star_stmt() -> SelectStmt {
        let mut s = empty_select("t");
        s.columns = vec![SelectColumn::Expr {
            expr: Expr::CountStar,
            alias: None,
        }];
        s
    }

    #[test]
    fn a_bare_count_star_takes_the_catalog_shortcut() {
        let s = count_star_stmt();
        assert!(matches!(
            choose_strategy(&s, &schema()).unwrap(),
            Strategy::CountStar
        ));
    }

    /// The same query with a WHERE clause cannot use the catalog count, so it
    /// must fall to a strategy that actually reads rows.
    #[test]
    fn a_filtered_count_star_does_not() {
        let mut s = count_star_stmt();
        s.where_clause = Some(Expr::Column("x".into()));
        assert!(!matches!(
            choose_strategy(&s, &schema()).unwrap(),
            Strategy::CountStar
        ));
    }

    #[test]
    fn a_plain_select_scans() {
        let s = empty_select("t");
        assert!(matches!(
            choose_strategy(&s, &schema()).unwrap(),
            Strategy::Scan { limit: None }
        ));
    }

    /// A bare LIMIT lets the scan stop early; the strategy carries that limit
    /// rather than the scan rediscovering it.
    #[test]
    fn a_limited_select_carries_its_limit() {
        let mut s = empty_select("t");
        s.limit = Some(Expr::Literal(i(10)));
        assert!(matches!(
            choose_strategy(&s, &schema()).unwrap(),
            Strategy::Scan { limit: Some(10) }
        ));
    }

    /// ORDER BY plus LIMIT is the top-k shape: the whole table is read but only
    /// k rows are kept, so it is one fused node rather than scan-then-sort.
    #[test]
    fn order_by_with_a_limit_is_top_k() {
        let mut s = empty_select("t");
        s.limit = Some(Expr::Literal(i(10)));
        s.order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: true,
            nulls_first: None,
        }];
        assert!(matches!(
            choose_strategy(&s, &schema()).unwrap(),
            Strategy::TopKScan(_)
        ));
    }

    /// Every strategy that runs as one fused pass names itself for EXPLAIN; the
    /// plain scan does not, because its plan lines describe it already.
    #[test]
    fn only_the_fused_strategies_claim_an_explain_label() {
        let plain = choose_strategy(&empty_select("t"), &schema()).unwrap();
        assert_eq!(plain.label(), None);

        let fused = choose_strategy(&count_star_stmt(), &schema()).unwrap();
        let label = fused.label().expect("a fused strategy must name itself");
        assert!(!label.is_empty());
    }
}

// Materialized rows isolate cancellation after the scan.
mod post_scan_cancellation {
    use super::*;
    use citadel::CancelToken;

    fn rows(n: i64) -> Vec<Vec<Value>> {
        (0..n).map(|k| vec![i(k), i(n - k)]).collect()
    }

    fn schema_cols() -> Vec<ColumnDef> {
        cols(&[("id", DataType::Integer), ("x", DataType::Integer)])
    }

    fn cancelled_token() -> CancelToken {
        let t = CancelToken::new();
        t.cancel();
        t
    }

    fn is_interrupted(e: &crate::error::SqlError) -> bool {
        matches!(
            e,
            crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
        )
    }

    #[test]
    fn a_pre_cancelled_filter_shape_is_refused() {
        let token = cancelled_token();
        let columns = schema_cols();
        let mut stmt = empty_select("t");
        stmt.where_clause = Some(Expr::Column("x".into()));

        let err =
            process_select(rows(500), SelectCtx::new(&columns, &stmt, Some(&token))).unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
    }

    #[test]
    fn a_pre_cancelled_distinct_shape_is_refused() {
        let token = cancelled_token();
        let columns = schema_cols();
        let mut stmt = empty_select("t");
        stmt.distinct = true;

        let err =
            process_select(rows(500), SelectCtx::new(&columns, &stmt, Some(&token))).unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
    }

    #[test]
    fn a_pre_cancelled_sort_shape_is_refused() {
        use crate::parser::OrderByItem;
        let token = cancelled_token();
        let columns = schema_cols();
        let mut stmt = empty_select("t");
        stmt.order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];

        let err =
            process_select(rows(500), SelectCtx::new(&columns, &stmt, Some(&token))).unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
    }

    #[test]
    fn no_token_means_no_behaviour_change() {
        let columns = schema_cols();
        let stmt = empty_select("t");

        let out = process_select(rows(10), SelectCtx::new(&columns, &stmt, None)).unwrap();

        let ExecutionResult::Query(qr) = out else {
            panic!("expected a query result");
        };
        assert_eq!(qr.rows.len(), 10);
    }

    fn division_by_zero() -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Literal(i(1))),
            op: crate::parser::BinOp::Div,
            right: Box::new(Expr::Literal(i(0))),
        }
    }

    #[test]
    fn no_from_where_propagates_evaluator_errors() {
        let mut stmt = empty_select("");
        stmt.where_clause = Some(division_by_zero());

        let err = exec_select_no_from(&stmt, None).unwrap_err();

        assert!(matches!(err, crate::error::SqlError::DivisionByZero));
    }

    #[test]
    fn post_scan_where_propagates_evaluator_errors() {
        let columns = schema_cols();
        let mut stmt = empty_select("t");
        stmt.where_clause = Some(division_by_zero());

        let err = process_select(rows(3), SelectCtx::new(&columns, &stmt, None)).unwrap_err();

        assert!(matches!(err, crate::error::SqlError::DivisionByZero));
    }

    /// Every sort shape shares the same pre-work cancellation boundary.
    #[test]
    fn every_sort_shape_refuses_a_pre_cancelled_token() {
        use crate::executor::helpers::sort_rows;
        use crate::parser::OrderByItem;

        let by = |name: &str| OrderByItem {
            expr: Expr::Column(name.into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        };
        let lanes: [(&str, Vec<OrderByItem>); 3] = [
            ("single flat key", vec![by("x")]),
            (
                "single collated key",
                vec![OrderByItem {
                    expr: Expr::Collate {
                        expr: Box::new(Expr::Column("x".into())),
                        collation: crate::types::Collation::NoCase,
                    },
                    output_name: None,
                    output_ordinal: None,
                    descending: false,
                    nulls_first: None,
                }],
            ),
            ("multiple keys", vec![by("x"), by("id")]),
        ];

        for (lane, order_by) in lanes {
            let token = cancelled_token();
            let columns = schema_cols();
            let mut r = rows(5_000);

            let err = sort_rows(&mut r, &order_by, &columns, Some(&token)).unwrap_err();

            assert!(is_interrupted(&err), "{lane}: got {err:?}");
        }
    }

    /// The token is tripped by the comparator itself, proving this is an
    /// in-progress cancellation rather than only an entry-boundary check.
    #[test]
    fn a_sort_stops_after_comparisons_have_started() {
        use crate::executor::helpers::sort_indices_by;

        let token = CancelToken::new();
        let mut indices: Vec<usize> = (0..5_000).rev().collect();
        let mut comparisons = 0;

        let err = sort_indices_by(&mut indices, Some(&token), |a, b| {
            comparisons += 1;
            if comparisons == 64 {
                token.cancel();
            }
            a.cmp(&b)
        })
        .unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
        assert!(comparisons >= 64, "the comparator was never reached");
    }

    #[test]
    fn owned_materialized_sorts_stop_after_comparisons_start() {
        use crate::executor::helpers::{sort_vec_by, sort_vec_unstable_by};

        let token = CancelToken::new();
        let mut comparisons = 0;
        let values: Vec<usize> = (0..5_000).rev().collect();
        let err = sort_vec_by(values, Some(&token), |a, b| {
            comparisons += 1;
            if comparisons == 64 {
                token.cancel();
            }
            a.cmp(b)
        })
        .unwrap_err();
        assert!(is_interrupted(&err), "stable owned sort: got {err:?}");
        assert!(comparisons >= 64, "stable comparator was never reached");

        let token = CancelToken::new();
        let mut comparisons = 0;
        let values: Vec<usize> = (0..5_000).rev().collect();
        let err = sort_vec_unstable_by(values, Some(&token), |a, b| {
            comparisons += 1;
            if comparisons == 64 {
                token.cancel();
            }
            a.cmp(b)
        })
        .unwrap_err();
        assert!(is_interrupted(&err), "unstable owned sort: got {err:?}");
        assert!(comparisons >= 64, "unstable comparator was never reached");
    }

    /// Cancellation is ordinary error propagation, so crash reporters and
    /// other process-wide panic hooks must not observe it.
    #[test]
    #[cfg(panic = "unwind")]
    fn a_cancelled_sort_does_not_trip_the_panic_hook() {
        use crate::executor::helpers::sort_indices_by;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let hits = StdArc::new(AtomicUsize::new(0));
        let counter = StdArc::clone(&hits);
        // Only this thread's panics count: the hook is process-wide and the
        // rest of the suite is running beside it.
        let mine = std::thread::current().id();

        let token = CancelToken::new();
        let mut indices: Vec<usize> = (0..5_000).rev().collect();
        let mut comparisons = 0;

        // Catch only at the test boundary so an unexpected implementation
        // panic cannot leave the process-wide hook installed for other tests.
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |_| {
            if std::thread::current().id() == mine {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            sort_indices_by(&mut indices, Some(&token), |a, b| {
                comparisons += 1;
                if comparisons == 64 {
                    token.cancel();
                }
                a.cmp(&b)
            })
        }));
        let panics = hits.load(Ordering::Relaxed);
        std::panic::set_hook(previous);

        let outcome = outcome.expect("cancellation must not unwind");
        assert!(
            outcome.as_ref().is_err_and(is_interrupted),
            "got {outcome:?}"
        );
        assert!(comparisons >= 64, "the comparator was never reached");
        assert_eq!(panics, 0, "cancellation reached the process panic hook");
    }

    #[test]
    fn a_topk_sort_stops_too() {
        use crate::executor::helpers::topk_rows;
        use crate::parser::OrderByItem;

        let token = cancelled_token();
        let columns = schema_cols();
        let mut r = rows(5_000);
        let order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];

        let err = topk_rows(&mut r, &order_by, &columns, 10, Some(&token)).unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
    }

    #[test]
    fn a_topk_selection_stops_after_comparisons_have_started() {
        use crate::executor::helpers::topk_indices_by;

        let token = CancelToken::new();
        let mut indices: Vec<usize> = (0..5_000).rev().collect();
        let mut comparisons = 0;

        let err = topk_indices_by(&mut indices, 37, Some(&token), |a, b| {
            comparisons += 1;
            if comparisons == 64 {
                token.cancel();
            }
            a.cmp(&b)
        })
        .unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
        assert!(comparisons >= 64, "selection never reached its comparator");
    }

    #[test]
    fn fused_topk_winner_sort_stops_after_comparisons_have_started() {
        use crate::encoding::{encode_composite_key, encode_row};
        use crate::executor::topk::{
            arm_topk_sort_cancel, take_topk_sort_comparisons, TopKScanPlan,
        };
        use crate::parser::OrderByItem;

        let schema = scan_limit_schema();
        let mut stmt = empty_select("t");
        stmt.order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];
        stmt.limit = Some(Expr::Literal(i(1_500)));
        let plan = TopKScanPlan::try_new(&stmt, &schema)
            .unwrap()
            .expect("top-k scan shape");
        let records: Vec<(Vec<u8>, Vec<u8>)> = (0..2_048i64)
            .map(|id| (encode_composite_key(&[i(id)]), encode_row(&[i(2_048 - id)])))
            .collect();
        let token = CancelToken::new();
        let _ = take_topk_sort_comparisons();

        let err = plan
            .execute_scan(&schema, &stmt, Some(&token), |visit| {
                for (key, value) in &records {
                    assert!(visit(key, value));
                }
                // Arm only after the heap has been built, so the cancellation
                // is caused by the final user-sized winner sort itself.
                arm_topk_sort_cancel(token.clone(), 64);
                Ok(())
            })
            .unwrap_err();
        let comparisons = take_topk_sort_comparisons();

        assert!(is_interrupted(&err), "got {err:?}");
        assert!(
            comparisons >= 64,
            "winner sort comparator was never reached"
        );
    }

    /// Refusal at sort entry leaves the input untouched.
    #[test]
    fn a_pre_cancelled_sort_leaves_the_rows_untouched() {
        use crate::executor::helpers::sort_rows;
        use crate::parser::OrderByItem;

        let token = cancelled_token();
        let columns = schema_cols();
        let mut r = rows(5_000);
        let before = r.clone();
        let order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];

        sort_rows(&mut r, &order_by, &columns, Some(&token)).unwrap_err();

        assert_eq!(r, before, "a cancelled sort half-reordered the rows");
    }

    /// A comparator bug still has its normal panic behavior; cancellation does
    /// not install a catch boundary that could translate or swallow it.
    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn a_genuine_panic_is_re_raised_rather_than_reported_as_cancelled() {
        use crate::executor::helpers::sort_rows;
        use crate::parser::OrderByItem;

        let token = CancelToken::new();
        let columns = schema_cols();
        let order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];
        let mut r: Vec<Vec<Value>> = (0..64i64)
            .map(|k| {
                if k == 32 {
                    vec![i(k)] // one column short: comparator indexes past it
                } else {
                    vec![i(k), i(64 - k)]
                }
            })
            .collect();

        let _ = sort_rows(&mut r, &order_by, &columns, Some(&token));
    }

    #[test]
    fn materialized_checks_are_amortized_but_bounded() {
        use crate::executor::helpers::{check_cancel_at, CANCEL_CHECK_INTERVAL};

        let token = cancelled_token();
        for iteration in 1..CANCEL_CHECK_INTERVAL {
            check_cancel_at(Some(&token), iteration)
                .expect("a materialized loop should not load the token on every item");
        }
        let err = check_cancel_at(Some(&token), CANCEL_CHECK_INTERVAL).unwrap_err();
        assert!(is_interrupted(&err), "got {err:?}");
    }

    #[test]
    fn projection_honors_a_cancelled_token() {
        use crate::executor::helpers::project_rows_with_cancel;

        let token = cancelled_token();
        let columns = schema_cols();
        let stmt = empty_select("t");
        let err = project_rows_with_cancel(&columns, &stmt.columns, rows(5_000), Some(&token))
            .unwrap_err();

        assert!(is_interrupted(&err), "got {err:?}");
    }

    #[test]
    fn an_untripped_token_matches_the_standard_sort_path() {
        use crate::executor::helpers::sort_rows;
        use crate::parser::OrderByItem;

        let columns = schema_cols();
        let order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];
        let source: Vec<Vec<Value>> = (0..5_003i64)
            .map(|id| vec![i(id), i((id * 7_919) % 113)])
            .collect();
        let mut expected = source.clone();
        let mut actual = source;

        sort_rows(&mut expected, &order_by, &columns, None).unwrap();
        sort_rows(&mut actual, &order_by, &columns, Some(&CancelToken::new())).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn an_untripped_token_matches_the_standard_topk_path() {
        use crate::executor::helpers::{sort_rows, topk_rows};
        use crate::parser::OrderByItem;

        let columns = schema_cols();
        let order_by = vec![
            OrderByItem {
                expr: Expr::Column("x".into()),
                output_name: None,
                output_ordinal: None,
                descending: false,
                nulls_first: None,
            },
            OrderByItem {
                expr: Expr::Column("id".into()),
                output_name: None,
                output_ordinal: None,
                descending: false,
                nulls_first: None,
            },
        ];
        let source: Vec<Vec<Value>> = (0..5_003i64)
            .map(|id| vec![i(id), i((id * 7_919) % 113)])
            .collect();
        let mut expected = source.clone();
        let mut actual = source;
        let keep = 137;

        sort_rows(&mut expected, &order_by, &columns, None).unwrap();
        expected.truncate(keep);
        topk_rows(
            &mut actual,
            &order_by,
            &columns,
            keep,
            Some(&CancelToken::new()),
        )
        .unwrap();
        actual.truncate(keep);

        assert_eq!(actual, expected);
    }

    #[test]
    fn fallible_topk_selection_matches_a_full_sort_across_input_shapes() {
        use crate::executor::helpers::topk_indices_by;

        let inputs: Vec<Vec<usize>> = vec![
            (0..257).collect(),
            (0..257).rev().collect(),
            vec![7; 257],
            (0..257).map(|n| (n * 97) % 31).collect(),
        ];

        for values in inputs {
            for keep in [1, 2, 17, values.len() / 2, values.len() - 1, values.len()] {
                let mut expected = values.clone();
                expected.sort();
                expected.truncate(keep);

                let mut indices: Vec<usize> = (0..values.len()).collect();
                topk_indices_by(&mut indices, keep, Some(&CancelToken::new()), |a, b| {
                    values[a].cmp(&values[b])
                })
                .unwrap();
                let actual: Vec<usize> = indices[..keep].iter().map(|&i| values[i]).collect();

                assert_eq!(actual, expected, "keep={keep}, values={values:?}");
            }
        }
    }

    #[test]
    fn an_untripped_token_lets_every_phase_run() {
        use crate::parser::OrderByItem;
        let token = CancelToken::new();
        let columns = schema_cols();
        let mut stmt = empty_select("t");
        stmt.distinct = true;
        stmt.order_by = vec![OrderByItem {
            expr: Expr::Column("x".into()),
            output_name: None,
            output_ordinal: None,
            descending: false,
            nulls_first: None,
        }];

        let out = process_select(rows(10), SelectCtx::new(&columns, &stmt, Some(&token))).unwrap();

        let ExecutionResult::Query(qr) = out else {
            panic!("expected a query result");
        };
        assert_eq!(qr.rows.len(), 10);
    }
}

mod materialized_row_clone_cancellation {
    use super::*;
    use crate::error::{Result, SqlError};
    use crate::types::{QueryResult, TableSchema};
    use citadel::CancelToken;

    fn assert_interrupted<T>(outcome: Result<T>) {
        let err = match outcome {
            Err(err) => err,
            Ok(_) => panic!("materialized rows ignored cancellation"),
        };
        assert!(matches!(
            err,
            SqlError::Storage(citadel_core::Error::Interrupted)
        ));
    }

    struct NoIo;

    impl LateralIo for NoIo {
        fn exec_select(
            &mut self,
            _: &crate::schema::SchemaManager,
            _: &crate::parser::SelectQuery,
        ) -> Result<QueryResult> {
            panic!("the outer CTE clone must finish before a lateral query runs")
        }

        fn scan_table(
            &mut self,
            _: &crate::schema::SchemaManager,
            _: &str,
        ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
            panic!("the outer source is a materialized CTE")
        }
    }

    #[test]
    fn lateral_outer_cte_clone_stops_when_cancelled_during_copy() {
        let token = CancelToken::new();
        let _cancel = crate::executor::cancel_on_nth_cte_row(token.clone(), 2);
        let mut ctes = CteContext::default();
        ctes.insert(
            "c".into(),
            CteRows::binary(QueryResult {
                columns: vec!["x".into()],
                rows: (0..1_024).map(|n| vec![Value::Integer(n)]).collect(),
            })
            .shared(),
        );

        let outcome = exec_select_lateral_with_io(
            &crate::schema::SchemaManager::empty(),
            &empty_select("c"),
            &ctes,
            &mut NoIo,
            Some(&token),
        );

        assert_interrupted(outcome);
        assert!(token.is_cancelled());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn correlated_view_clone_stops_when_cancelled_during_copy() {
        use crate::connection::Connection;

        let dir = tempfile::tempdir().unwrap();
        let db = citadel::DatabaseBuilder::new(dir.path().join("view-clone.db"))
            .passphrase(b"x")
            .argon2_profile(citadel::Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        let insert = conn.prepare("INSERT INTO t VALUES ($1)").unwrap();
        for n in 0..1_024 {
            insert.execute(&[Value::Integer(n)]).unwrap();
        }
        conn.execute("COMMIT").unwrap();
        conn.execute("CREATE VIEW v (x) AS SELECT id FROM t")
            .unwrap();

        let token = CancelToken::new();
        db.set_cancel(Some(token.clone()));
        let _cancel = crate::executor::cancel_on_nth_cte_row(token.clone(), 2);

        let outcome = conn.query(
            "SELECT x FROM v WHERE EXISTS (SELECT 1 FROM t AS inner_t WHERE inner_t.id = v.x)",
        );

        assert_interrupted(outcome);
        assert!(token.is_cancelled());
    }
}

#[test]
fn compute_scan_limit_none_when_no_limit() {
    let s = empty_select("t");
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), None);
}

#[test]
fn compute_scan_limit_simple_limit() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), Some(10));
}

#[test]
fn compute_scan_limit_with_offset_adds() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(5)));
    s.offset = Some(Expr::Literal(i(3)));
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), Some(8));
}

#[test]
fn compute_scan_limit_saturates_large_counts_and_their_sum() {
    for (limit, offset) in [
        (i64::from(u32::MAX) + 1, 0),
        (i64::from(u32::MAX) + 2, 3),
        (3, i64::from(u32::MAX) + 1),
        (i64::MAX, i64::MAX),
        (-1, 3),
    ] {
        let mut s = empty_select("t");
        s.limit = Some(Expr::Literal(i(limit)));
        s.offset = Some(Expr::Literal(i(offset)));
        let expected =
            (limit.max(0) as u128 + offset.max(0) as u128).min(usize::MAX as u128) as usize;
        assert_eq!(
            compute_scan_limit(&s, &scan_limit_schema()),
            Some(expected),
            "LIMIT {limit} OFFSET {offset}"
        );
    }
}

#[test]
fn apply_offset_limit_keeps_large_limits_and_exhausts_large_offsets() {
    let original = vec![vec![i(1)], vec![i(2)], vec![i(3)]];
    for count in [i64::from(u32::MAX) + 1, i64::from(u32::MAX) + 2, i64::MAX] {
        let mut s = empty_select("t");
        s.limit = Some(Expr::Literal(i(count)));
        let mut rows = original.clone();
        apply_offset_limit(&mut rows, &s).unwrap();
        assert_eq!(rows, original, "LIMIT {count}");

        s.offset = Some(Expr::Literal(i(count)));
        apply_offset_limit(&mut rows, &s).unwrap();
        assert!(rows.is_empty(), "OFFSET {count}");
    }
    let mut s = empty_select("t");
    s.offset = Some(Expr::Literal(i(-1)));
    let mut rows = original.clone();
    apply_offset_limit(&mut rows, &s).unwrap();
    assert_eq!(rows, original);
    s.limit = Some(Expr::Literal(i(-1)));
    apply_offset_limit(&mut rows, &s).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn scan_limit_prediction_leaves_expression_errors_to_execution() {
    let mut s = empty_select("t");
    s.offset = Some(Expr::Literal(Value::Text("bad".into())));
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), None);
    s.limit = Some(Expr::Literal(i(3)));
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), Some(3));
    assert!(apply_offset_limit(&mut vec![vec![i(1)]], &s).is_err());
}

#[test]
fn compute_scan_limit_none_with_order_by() {
    use crate::parser::OrderByItem;
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.order_by = vec![OrderByItem {
        expr: Expr::Column("x".into()),
        output_name: None,
        output_ordinal: None,
        descending: false,
        nulls_first: None,
    }];
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), None);
}

#[test]
fn compute_scan_limit_none_with_group_by() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.group_by = vec![Expr::Column("x".into())];
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), None);
}

#[test]
fn compute_scan_limit_none_with_distinct() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.distinct = true;
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), None);
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

// Merged and serial aggregates must agree in both debug and release profiles.
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
    let mut left = AggState::Min {
        current: Some(Value::Integer(3)),
        collation: Collation::Binary,
    };
    left.merge(AggState::Min {
        current: Some(Value::Integer(3)),
        collation: Collation::Binary,
    });
    assert_eq!(left.finish(), Value::Integer(3));

    let mut left = AggState::Max {
        current: Some(Value::Text("b".into())),
        collation: Collation::Binary,
    };
    left.merge(AggState::Max {
        current: Some(Value::Text("a".into())),
        collation: Collation::Binary,
    });
    assert_eq!(left.finish(), Value::Text("b".into()));

    let mut left = AggState::Min {
        current: None,
        collation: Collation::Binary,
    };
    left.merge(AggState::Min {
        current: Some(Value::Integer(7)),
        collation: Collation::Binary,
    });
    assert_eq!(left.finish(), Value::Integer(7));

    let mut left = AggState::Min {
        current: Some(Value::Text("A".into())),
        collation: Collation::NoCase,
    };
    left.merge(AggState::Min {
        current: Some(Value::Text("a".into())),
        collation: Collation::NoCase,
    });
    assert_eq!(left.finish(), Value::Text("A".into()));
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

#[test]
fn compute_scan_limit_allows_pk_asc_order() {
    use crate::parser::OrderByItem;
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.offset = Some(Expr::Literal(i(5)));
    s.order_by = vec![OrderByItem {
        expr: Expr::Column("id".into()),
        output_name: None,
        output_ordinal: None,
        descending: false,
        nulls_first: None,
    }];
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), Some(15));

    s.order_by[0].descending = true;
    assert_eq!(compute_scan_limit(&s, &scan_limit_schema()), None);
}

#[test]
fn raw_aggregate_feed_rejects_truncated_headers_before_using_defaults() {
    let targets = [RawAggTarget::NonPk(0)];
    let defaults = [Some(i(7))];
    let feed = RawFeed {
        raw_targets: &targets,
        num_pk_cols: 1,
        nonpk_agg_defaults: &defaults,
    };
    for bytes in [&[][..], &[0], &[1, 0], &[1, 0x80]] {
        let mut states = [AggState::new(&StreamAgg::Sum(1))];
        let mut error = None;
        assert!(!feed.feed(&[], bytes, &mut states, &mut error));
        assert!(error.is_some(), "{bytes:?}");
    }
}

#[test]
fn numeric_aggregate_states_match_generic_families_and_accumulation() {
    let interval = |months, days, micros| Value::Interval {
        months,
        days,
        micros,
    };
    let fixtures = [
        vec![],
        vec![Value::Null, Value::Null],
        vec![Value::Null, i(1), Value::Null, Value::Real(2.5)],
        vec![Value::Null, Value::Real(2.5), i(-1)],
        vec![
            Value::Real(9007199254740992.0),
            i(1),
            Value::Real(-9007199254740992.0),
        ],
        vec![
            Value::Null,
            interval(1, 2, 3),
            Value::Null,
            interval(2, 4, 6),
        ],
        vec![interval(i32::MAX, i32::MAX, i64::MAX), interval(1, 1, 1)],
        vec![interval(i32::MIN, i32::MIN, i64::MIN), interval(-1, -1, -1)],
        vec![Value::Null, i(1), Value::Null, interval(0, 1, 0)],
        vec![Value::Null, Value::Real(1.5), interval(0, 1, 0)],
        vec![Value::Null, interval(0, 1, 0), Value::Null, i(1)],
        vec![Value::Null, interval(0, 1, 0), Value::Real(1.5)],
    ];
    let columns = cols(&[("v", DataType::Integer)]);
    let column_map = ColumnMap::new(&columns);
    for values in fixtures {
        let rows: Vec<_> = values.iter().map(|value| vec![value.clone()]).collect();
        let row_refs: Vec<_> = rows.iter().collect();
        for (name, op) in [("SUM", StreamAgg::Sum(0)), ("AVG", StreamAgg::Avg(0))] {
            let expr = Expr::Function {
                name: name.into(),
                args: vec![Expr::Column("v".into())],
                distinct: false,
            };
            let expected = eval_aggregate_expr(&expr, &column_map, &row_refs);
            for representation in 0..4 {
                let mut state = AggState::new(&op);
                let mut accepted = 0;
                let outcome = values.iter().enumerate().try_for_each(|(index, value)| {
                    let raw = match representation {
                        0 => false,
                        1 => true,
                        2 => index % 2 == 0,
                        _ => index % 2 != 0,
                    };
                    if raw {
                        let encoded = crate::encoding::encode_row(std::slice::from_ref(value));
                        state.feed_raw(&decode_stored_column_raw(&encoded, 0)?.unwrap())?;
                    } else {
                        state.feed_val(value)?;
                    }
                    accepted += 1;
                    Ok::<(), SqlError>(())
                });
                match (outcome, &expected) {
                    (Ok(()), Ok(expected)) => {
                        assert_eq!(state.finish(), *expected, "{name} {values:?}")
                    }
                    (Err(actual), Err(expected)) => {
                        assert_eq!(
                            actual.to_string(),
                            expected.to_string(),
                            "{name} {values:?}"
                        );
                        let prefix =
                            eval_aggregate_expr(&expr, &column_map, &row_refs[..accepted]).unwrap();
                        assert_eq!(state.finish(), prefix, "failed feed changed {name} state");
                    }
                    (actual, expected) => {
                        panic!("{name} {values:?}: actual={actual:?}, expected={expected:?}")
                    }
                }
            }
        }
    }
}

#[test]
fn numeric_aggregate_states_report_the_first_non_null_family_for_bad_types() {
    for (prefix, expected_family) in [
        (Value::Null, "numeric"),
        (i(1), "numeric"),
        (
            Value::Interval {
                months: 0,
                days: 1,
                micros: 0,
            },
            "INTERVAL",
        ),
    ] {
        for op in [StreamAgg::Sum(0), StreamAgg::Avg(0)] {
            for raw in [false, true] {
                let mut state = AggState::new(&op);
                state.feed_val(&Value::Null).unwrap();
                state.feed_val(&prefix).unwrap();
                let error = if raw {
                    state.feed_raw(&RawColumn::Text("not numeric"))
                } else {
                    state.feed_val(&Value::Text("not numeric".into()))
                }
                .unwrap_err();
                assert!(
                    matches!(error, SqlError::TypeMismatch { ref expected, .. } if expected == expected_family)
                );
            }
        }
    }
}
