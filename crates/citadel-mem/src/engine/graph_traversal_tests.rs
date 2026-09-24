use super::*;

fn database() -> Database {
    citadel::DatabaseBuilder::new("")
        .passphrase(b"test-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE graph_atoms (id INTEGER PRIMARY KEY, region_id INTEGER, expires_at TIMESTAMP)").unwrap();
    conn.execute("CREATE TABLE memory_edges (src_id INTEGER, dst_id INTEGER, kind TEXT, weight REAL, PRIMARY KEY (src_id,dst_id,kind))").unwrap();
    conn.execute("INSERT INTO graph_atoms VALUES (1,1,NULL),(2,1,NULL),(3,1,NULL),(4,2,NULL),(5,1,TIMESTAMP '2000-01-01 00:00:00'),(100,1,NULL)").unwrap();
}

fn scope() -> GraphFetchScope<'static> {
    GraphFetchScope {
        table: "graph_atoms",
        region_id: 1,
        kind_allowlist: &[],
        payload_filter: None,
        sealed_db: None,
    }
}

fn insert_edge(conn: &Connection<'_>, src: AtomId, dst: AtomId, kind: EdgeKind) {
    conn.execute_params(
        "INSERT INTO memory_edges VALUES ($1,$2,$3,1.0)",
        &[
            Value::Integer(src),
            Value::Integer(dst),
            Value::Text(kind.as_str().into()),
        ],
    )
    .unwrap();
}

#[test]
fn graph_wave_seeks_sources_without_scanning_unrelated_edges() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("BEGIN").unwrap();
    for src in -1000..0 {
        insert_edge(&conn, src, 2, EdgeKind::DerivedFrom);
    }
    for src in 1000..2000 {
        insert_edge(&conn, src, 2, EdgeKind::DerivedFrom);
    }
    insert_edge(&conn, 1, 2, EdgeKind::DerivedFrom);
    insert_edge(&conn, 1, 3, EdgeKind::DerivedFrom);
    conn.execute("COMMIT").unwrap();
    let mut budget = GraphTraversalBudget::new(2);
    budget.edges.limit = 2;
    let measured = db.measure_scans();
    let reached = region_edge_wave(
        &conn,
        scope(),
        &[1],
        &FxHashSet::default(),
        &[EdgeKind::DerivedFrom],
        &mut budget,
        None,
    )
    .unwrap();
    assert_eq!(reached, vec![2, 3]);
    assert_eq!(budget.edges.examined, 2);
    assert_eq!(measured.rows_scanned(), 2);
}

#[test]
fn source_edge_read_limits_work_with_a_redundant_secondary_index() {
    for indexed in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        setup(&conn);
        if indexed {
            conn.execute("CREATE INDEX edges_source ON memory_edges (src_id)")
                .unwrap();
        }
        conn.execute("BEGIN").unwrap();
        for dst in 0..256 {
            insert_edge(&conn, 1, dst, EdgeKind::DerivedFrom);
        }
        conn.execute("COMMIT").unwrap();
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            // Start each transaction mode with a cold statement/result cache.
            let conn = Connection::open(&db).unwrap();
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            // A repeated read can reuse SQL rows, but must still enforce the
            // graph budget. Writers always read their own transaction state.
            let repeat_scans = if begin == Some("BEGIN") { 5 } else { 0 };
            for expected_scans in [5, repeat_scans] {
                let mut budget = GraphEdgeBudget::new();
                budget.limit = 4;
                let measured = db.measure_scans();
                let error = source_edge_rows(&conn, 1, &mut budget).unwrap_err();
                assert!(matches!(
                    error,
                    MemError::WorkLimitExceeded {
                        operation: "graph edge inspection",
                        limit: 4
                    }
                ));
                assert_eq!(
                    measured.rows_scanned(),
                    expected_scans,
                    "indexed={indexed}, {begin:?}"
                );
            }
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn graph_wave_counts_rejected_edges_and_keeps_late_eligible_destinations() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (dst, kind) in [
        (2, EdgeKind::Causes),
        (4, EdgeKind::DerivedFrom),
        (5, EdgeKind::DerivedFrom),
        (6, EdgeKind::DerivedFrom),
        (100, EdgeKind::DerivedFrom),
    ] {
        insert_edge(&conn, 1, dst, kind);
    }
    let mut budget = GraphTraversalBudget::new(1);
    budget.edges.limit = 5;
    let reached = region_edge_wave(
        &conn,
        scope(),
        &[1],
        &FxHashSet::default(),
        &[EdgeKind::DerivedFrom],
        &mut budget,
        None,
    )
    .unwrap();
    assert_eq!(reached, vec![100]);
    assert_eq!(budget.edges.examined, 5);
    assert_eq!(budget.nodes, 1);

    let mut budget = GraphTraversalBudget::new(1);
    budget.edges.limit = 4;
    let error = region_edge_wave(
        &conn,
        scope(),
        &[1],
        &FxHashSet::default(),
        &[EdgeKind::DerivedFrom],
        &mut budget,
        None,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemError::WorkLimitExceeded {
            operation: "graph edge inspection",
            limit: 4
        }
    ));
}

#[test]
fn graph_wave_counts_duplicate_and_visited_edges_without_spending_extra_nodes() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    insert_edge(&conn, 1, 2, EdgeKind::DerivedFrom);
    insert_edge(&conn, 1, 2, EdgeKind::SimilarTo);
    insert_edge(&conn, 1, 3, EdgeKind::DerivedFrom);
    let mut visited = FxHashSet::default();
    visited.insert(3);
    let mut budget = GraphTraversalBudget::new(1);
    budget.edges.limit = 3;
    let reached = region_edge_wave(&conn, scope(), &[1], &visited, &[], &mut budget, None).unwrap();
    assert_eq!(reached, vec![2]);
    assert_eq!(budget.edges.examined, 3);
    assert_eq!(budget.nodes, 1);
}

#[test]
fn graph_edge_budget_is_shared_across_sources_and_waves() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    insert_edge(&conn, 1, 2, EdgeKind::DerivedFrom);
    insert_edge(&conn, 2, 3, EdgeKind::DerivedFrom);
    insert_edge(&conn, 2, 100, EdgeKind::DerivedFrom);
    let mut budget = GraphTraversalBudget::new(8);
    budget.edges.limit = 2;
    let first = region_edge_wave(
        &conn,
        scope(),
        &[1],
        &FxHashSet::default(),
        &[],
        &mut budget,
        None,
    )
    .unwrap();
    assert_eq!(first, vec![2]);
    let error = region_edge_wave(
        &conn,
        scope(),
        &first,
        &FxHashSet::default(),
        &[],
        &mut budget,
        None,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemError::WorkLimitExceeded {
            operation: "graph edge inspection",
            limit: 2
        }
    ));
}

#[test]
fn graph_wave_does_not_follow_foreign_or_expired_sources() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    insert_edge(&conn, 4, 2, EdgeKind::DerivedFrom);
    insert_edge(&conn, 5, 3, EdgeKind::DerivedFrom);
    let mut budget = GraphTraversalBudget::new(0);
    budget.edges.limit = 0;
    let reached = region_edge_wave(
        &conn,
        scope(),
        &[4, 5],
        &FxHashSet::default(),
        &[],
        &mut budget,
        None,
    )
    .unwrap();
    assert!(reached.is_empty());
    assert_eq!(budget.edges.examined, 0);
}

#[test]
fn source_edge_read_checks_cancellation_before_scanning() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    insert_edge(&conn, 1, 2, EdgeKind::DerivedFrom);
    let token = citadel_core::CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));
    let mut budget = GraphEdgeBudget::new();
    let error = source_edge_rows(&conn, 1, &mut budget).unwrap_err();
    assert!(matches!(
        error,
        MemError::Sql(citadel_sql::SqlError::Storage(
            citadel_core::Error::Interrupted
        ))
    ));
    assert_eq!(budget.examined, 0);
}

#[test]
fn global_cycle_check_follows_only_the_requested_kind() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    insert_edge(&conn, 1, 2, EdgeKind::Causes);
    insert_edge(&conn, 1, 3, EdgeKind::DerivedFrom);
    insert_edge(&conn, 3, 100, EdgeKind::DerivedFrom);
    assert!(would_cycle(&conn, 100, 1, EdgeKind::DerivedFrom).unwrap());
    assert!(!would_cycle(&conn, 100, 1, EdgeKind::Causes).unwrap());
    assert!(would_cycle(&conn, 1, 1, EdgeKind::DerivedFrom).unwrap());
}

#[test]
fn cycle_work_refusal_preserves_atoms_edges_and_keys() {
    struct LimitGuard(Option<usize>);
    impl Drop for LimitGuard {
        fn drop(&mut self) {
            GRAPH_EDGE_LIMIT_FOR_TEST.with(|limit| limit.set(self.0));
        }
    }

    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            citadel::DatabaseBuilder::new(dir.path().join("graph.db"))
                .passphrase(b"test-passphrase")
                .argon2_profile(citadel::Argon2Profile::Iot)
                .enable_region_keys(true)
                .create()
                .unwrap(),
        );
        let engine = MemoryEngine::open(db.clone()).unwrap();
        let embedder = Arc::new(crate::MockEmbedder::new(8));
        if encrypted {
            engine.create_encrypted_region("graph", embedder).unwrap();
        } else {
            engine.create_region("graph", embedder).unwrap();
        }
        let first = engine
            .remember("graph", AtomInput::new("fact", "first source"))
            .unwrap();
        let second = engine
            .remember("graph", AtomInput::new("fact", "second source"))
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        for destination in 100..103 {
            conn.execute_params(
                "INSERT INTO memory_edges (src_id,dst_id,kind,weight) VALUES ($1,$2,'causes',1.0)",
                &[Value::Integer(second), Value::Integer(destination)],
            )
            .unwrap();
        }
        let before_keys = db.atom_store_live_bindings().unwrap();
        let before_count = engine.count_region("graph").unwrap();
        let previous = GRAPH_EDGE_LIMIT_FOR_TEST.with(|limit| limit.replace(Some(2)));
        let limit_guard = LimitGuard(previous);
        let error = engine
            .link_in_region("graph", first, second, EdgeKind::DependsOn, 1.0)
            .unwrap_err();
        assert!(matches!(
            error,
            MemError::WorkLimitExceeded {
                operation: "graph edge inspection",
                limit: 2
            }
        ));
        assert_eq!(engine.count_region("graph").unwrap(), before_count);
        assert_eq!(db.atom_store_live_bindings().unwrap(), before_keys);
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM memory_edges")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(3)]]
        );
        drop(limit_guard);
        engine
            .link_in_region("graph", first, second, EdgeKind::DependsOn, 1.0)
            .unwrap();
        let edges = conn
            .query_params(
                "SELECT dst_id FROM memory_edges WHERE src_id = $1 ORDER BY dst_id",
                &[Value::Integer(first)],
            )
            .unwrap();
        assert_eq!(edges.rows, vec![vec![Value::Integer(second)]]);
    }
}
