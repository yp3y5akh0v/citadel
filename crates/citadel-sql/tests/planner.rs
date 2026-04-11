use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::parser::parse_sql;
use citadel_sql::planner::{plan_select, ScanPlan};
use citadel_sql::schema::SchemaManager;
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn query_result(result: ExecutionResult) -> QueryResult {
    match result {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

/// Verify the planner picks a specific scan type for a given SQL query.
fn assert_plan(db: &citadel::Database, sql: &str, expected: &str) {
    let schema_mgr = SchemaManager::load(db).unwrap();
    let stmt = parse_sql(sql).unwrap();
    let (table_name, where_clause) = match &stmt {
        citadel_sql::parser::Statement::Select(sq) => match &sq.body {
            citadel_sql::parser::QueryBody::Select(s) => {
                (s.from.to_ascii_lowercase(), &s.where_clause)
            }
            _ => panic!("assert_plan only works with simple SELECT"),
        },
        citadel_sql::parser::Statement::Update(u) => {
            (u.table.to_ascii_lowercase(), &u.where_clause)
        }
        citadel_sql::parser::Statement::Delete(d) => {
            (d.table.to_ascii_lowercase(), &d.where_clause)
        }
        _ => panic!("assert_plan only works with SELECT/UPDATE/DELETE"),
    };
    let table_schema = schema_mgr
        .get(&table_name)
        .unwrap_or_else(|| panic!("table '{table_name}' not found in schema"));
    let plan = plan_select(table_schema, where_clause);
    let plan_name = match &plan {
        ScanPlan::SeqScan => "SeqScan",
        ScanPlan::PkLookup { .. } => "PkLookup",
        ScanPlan::IndexScan { index_name, .. } => {
            // Return the index name as part of the match
            if let Some(expected_idx) = expected.strip_prefix("IndexScan:") {
                assert_eq!(
                    index_name, expected_idx,
                    "Expected IndexScan on '{expected_idx}', got IndexScan on '{index_name}'"
                );
                return;
            }
            "IndexScan"
        }
    };
    assert_eq!(
        plan_name, expected,
        "For query: {sql}\nExpected plan: {expected}, got: {plan_name}"
    );
}

fn setup_products(conn: &mut Connection) {
    assert_ok(conn.execute(
        "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, category TEXT, price REAL, stock INTEGER)"
    ).unwrap());
    assert_ok(
        conn.execute("CREATE INDEX idx_category ON products (category)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE UNIQUE INDEX idx_name ON products (name)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE INDEX idx_cat_price ON products (category, price)")
            .unwrap(),
    );

    let inserts = [
        "INSERT INTO products VALUES (1, 'Widget', 'Electronics', 29.99, 100)",
        "INSERT INTO products VALUES (2, 'Gadget', 'Electronics', 49.99, 50)",
        "INSERT INTO products VALUES (3, 'Thingamajig', 'Toys', 9.99, 200)",
        "INSERT INTO products VALUES (4, 'Doohickey', 'Toys', 14.99, 150)",
        "INSERT INTO products VALUES (5, 'Gizmo', 'Electronics', 99.99, 25)",
        "INSERT INTO products VALUES (6, 'Contraption', 'Tools', 39.99, 75)",
        "INSERT INTO products VALUES (7, 'Apparatus', 'Tools', 59.99, 30)",
        "INSERT INTO products VALUES (8, 'Mechanism', 'Electronics', 19.99, 120)",
    ];
    for sql in &inserts {
        assert_rows_affected(conn.execute(sql).unwrap(), 1);
    }
}

// ═══════════════════════════════════════════════════════════════════
//  PLAN VERIFICATION: prove the planner picks the right scan type
// ═══════════════════════════════════════════════════════════════════

#[test]
fn plan_pk_lookup_chosen_for_pk_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "SELECT * FROM products WHERE id = 3", "PkLookup");
}

#[test]
fn plan_pk_lookup_reversed_operands() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "SELECT * FROM products WHERE 5 = id", "PkLookup");
}

#[test]
fn plan_unique_index_chosen_for_name_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE name = 'Widget'",
        "IndexScan:idx_name",
    );
}

#[test]
fn plan_non_unique_index_chosen_for_category_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // Should pick idx_cat_price (2 cols) or idx_category (1 col); both have
    // 1 equality column, but idx_cat_price has more columns so could score higher
    // if range is added. With just equality on category, both have same equality count.
    assert_plan(
        &db,
        "SELECT * FROM products WHERE category = 'Toys'",
        "IndexScan",
    );
}

#[test]
fn plan_composite_index_chosen_for_two_column_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category = 'Electronics' AND price = 49.99",
        "IndexScan:idx_cat_price",
    );
}

#[test]
fn plan_composite_index_chosen_for_equality_plus_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category = 'Electronics' AND price > 30.0",
        "IndexScan:idx_cat_price",
    );
}

#[test]
fn plan_seq_scan_for_no_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "SELECT * FROM products", "SeqScan");
}

#[test]
fn plan_seq_scan_for_or_condition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category = 'Toys' OR category = 'Tools'",
        "SeqScan",
    );
}

#[test]
fn plan_seq_scan_for_non_indexed_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "SELECT * FROM products WHERE stock = 100", "SeqScan");
}

#[test]
fn plan_seq_scan_for_non_leading_composite_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // price alone is not the leading column of idx_cat_price
    assert_plan(&db, "SELECT * FROM products WHERE price = 29.99", "SeqScan");
}

#[test]
fn plan_index_scan_for_range_on_leading_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category > 'T'",
        "IndexScan",
    );
}

#[test]
fn plan_pk_lookup_for_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "UPDATE products SET stock = 0 WHERE id = 1",
        "PkLookup",
    );
}

#[test]
fn plan_index_scan_for_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "DELETE FROM products WHERE name = 'Widget'",
        "IndexScan:idx_name",
    );
}

#[test]
fn plan_pk_lookup_for_delete_by_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "DELETE FROM products WHERE id = 7", "PkLookup");
}

#[test]
fn plan_index_scan_for_update_by_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "UPDATE products SET stock = 0 WHERE category = 'Tools'",
        "IndexScan",
    );
}

// ═══════════════════════════════════════════════════════════════════
//  PK LOOKUP CORRECTNESS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pk_lookup_returns_correct_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(conn.execute("SELECT * FROM products WHERE id = 3").unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[0][1], Value::Text("Thingamajig".into()));
    assert_eq!(qr.rows[0][2], Value::Text("Toys".into()));
    assert_eq!(qr.rows[0][3], Value::Real(9.99));
    assert_eq!(qr.rows[0][4], Value::Integer(200));
}

#[test]
fn pk_lookup_nonexistent_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT * FROM products WHERE id = 999")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn pk_lookup_reversed_operands_returns_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE 5 = id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Gizmo".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  UNIQUE INDEX SCAN CORRECTNESS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn unique_index_returns_correct_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT id, price FROM products WHERE name = 'Gadget'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[0][1], Value::Real(49.99));
}

#[test]
fn unique_index_no_match_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT * FROM products WHERE name = 'NoSuchProduct'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════
//  NON-UNIQUE INDEX SCAN CORRECTNESS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn non_unique_index_returns_all_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT id FROM products WHERE category = 'Electronics' ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(5));
    assert_eq!(qr.rows[3][0], Value::Integer(8));
}

#[test]
fn non_unique_index_no_match_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT * FROM products WHERE category = 'Food'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════
//  COMPOSITE INDEX CORRECTNESS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn composite_full_equality_returns_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE category = 'Electronics' AND price = 49.99")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Gadget".into()));
}

#[test]
fn composite_equality_plus_range_gt() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(conn.execute(
        "SELECT name FROM products WHERE category = 'Electronics' AND price > 30.0 ORDER BY name"
    ).unwrap());
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Gadget".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Gizmo".into()));
}

#[test]
fn composite_equality_plus_range_lt() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(conn.execute(
        "SELECT name FROM products WHERE category = 'Electronics' AND price < 30.0 ORDER BY name"
    ).unwrap());
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Mechanism".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Widget".into()));
}

#[test]
fn composite_equality_plus_range_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(conn.execute(
        "SELECT name FROM products WHERE category = 'Electronics' AND price >= 20.0 AND price <= 50.0 ORDER BY name"
    ).unwrap());
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Gadget".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Widget".into()));
}

#[test]
fn composite_leading_eq_no_range_returns_all_in_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // category = 'Toys' via idx_cat_price returns all Toys regardless of price
    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE category = 'Toys' ORDER BY name")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Doohickey".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Thingamajig".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  RANGE SCAN ON LEADING COLUMN
// ═══════════════════════════════════════════════════════════════════

#[test]
fn range_gt_on_leading_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute(
            "SELECT DISTINCT category FROM products WHERE category > 'T' ORDER BY category",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Tools".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Toys".into()));
}

#[test]
fn range_lt_on_leading_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute(
            "SELECT DISTINCT category FROM products WHERE category < 'T' ORDER BY category",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Electronics".into()));
}

#[test]
fn range_gte_on_leading_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute(
            "SELECT DISTINCT category FROM products WHERE category >= 'Tools' ORDER BY category",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Tools".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Toys".into()));
}

#[test]
fn range_lte_on_leading_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(conn.execute(
        "SELECT DISTINCT category FROM products WHERE category <= 'Electronics' ORDER BY category"
    ).unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Electronics".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  RESIDUAL WHERE: index narrows, full WHERE still applied
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_scan_with_residual_and_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute(
            "SELECT name FROM products WHERE category = 'Electronics' AND stock > 50 ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Mechanism".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Widget".into()));
}

#[test]
fn pk_lookup_with_additional_predicate_that_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // PK lookup finds id=1, but stock != 999 -> residual WHERE eliminates it
    let qr = query_result(
        conn.execute("SELECT * FROM products WHERE id = 1 AND stock = 999")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn pk_lookup_with_additional_predicate_that_passes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE id = 1 AND stock = 100")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Widget".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  UPDATE VIA INDEX
// ═══════════════════════════════════════════════════════════════════

#[test]
fn update_via_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "UPDATE products SET price = 24.99 WHERE id = 1",
        "PkLookup",
    );
    assert_rows_affected(
        conn.execute("UPDATE products SET price = 24.99 WHERE id = 1")
            .unwrap(),
        1,
    );

    let qr = query_result(
        conn.execute("SELECT price FROM products WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(24.99));
}

#[test]
fn update_via_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "UPDATE products SET stock = 999 WHERE name = 'Gizmo'",
        "IndexScan:idx_name",
    );
    assert_rows_affected(
        conn.execute("UPDATE products SET stock = 999 WHERE name = 'Gizmo'")
            .unwrap(),
        1,
    );

    let qr = query_result(
        conn.execute("SELECT stock FROM products WHERE id = 5")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(999));
}

#[test]
fn update_via_non_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE products SET stock = 0 WHERE category = 'Toys'")
            .unwrap(),
        2,
    );

    let qr = query_result(
        conn.execute("SELECT stock FROM products WHERE category = 'Toys' ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
    assert_eq!(qr.rows[1][0], Value::Integer(0));
}

#[test]
fn update_indexed_column_maintains_index_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // Move a product to a different category
    assert_rows_affected(
        conn.execute("UPDATE products SET category = 'Tools' WHERE name = 'Widget'")
            .unwrap(),
        1,
    );

    // Widget should no longer appear in Electronics
    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE category = 'Electronics' ORDER BY name")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 3);
    assert!(!qr.rows.iter().any(|r| r[0] == Value::Text("Widget".into())));

    // Widget should appear in Tools
    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE category = 'Tools' ORDER BY name")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 3);
    assert!(qr.rows.iter().any(|r| r[0] == Value::Text("Widget".into())));
}

#[test]
fn update_nonexistent_via_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_rows_affected(
        conn.execute("UPDATE products SET stock = 0 WHERE id = 999")
            .unwrap(),
        0,
    );
}

// ═══════════════════════════════════════════════════════════════════
//  DELETE VIA INDEX
// ═══════════════════════════════════════════════════════════════════

#[test]
fn delete_via_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "DELETE FROM products WHERE id = 4", "PkLookup");
    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE id = 4").unwrap(),
        1,
    );
    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(7));
}

#[test]
fn delete_via_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "DELETE FROM products WHERE name = 'Apparatus'",
        "IndexScan:idx_name",
    );
    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE name = 'Apparatus'")
            .unwrap(),
        1,
    );

    let qr = query_result(
        conn.execute("SELECT * FROM products WHERE name = 'Apparatus'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn delete_via_non_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE category = 'Electronics'")
            .unwrap(),
        4,
    );

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(4));

    let qr = query_result(
        conn.execute("SELECT DISTINCT category FROM products ORDER BY category")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Tools".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Toys".into()));
}

#[test]
fn delete_nonexistent_via_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE id = 999").unwrap(),
        0,
    );
}

#[test]
fn delete_nonexistent_via_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE name = 'NoSuchProduct'")
            .unwrap(),
        0,
    );
}

// ═══════════════════════════════════════════════════════════════════
//  TRANSACTION MODE: index scans within BEGIN/COMMIT/ROLLBACK
// ═══════════════════════════════════════════════════════════════════

#[test]
fn select_via_index_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    conn.execute("BEGIN").unwrap();
    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE category = 'Tools' ORDER BY name")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Apparatus".into()));

    conn.execute("INSERT INTO products VALUES (9, 'Wrench', 'Tools', 12.99, 500)")
        .unwrap();
    let qr = query_result(
        conn.execute("SELECT name FROM products WHERE category = 'Tools' ORDER BY name")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[2][0], Value::Text("Wrench".into()));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn update_via_index_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("UPDATE products SET price = 0.0 WHERE name = 'Widget'")
            .unwrap(),
        1,
    );
    let qr = query_result(
        conn.execute("SELECT price FROM products WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(0.0));
    conn.execute("COMMIT").unwrap();

    let qr = query_result(
        conn.execute("SELECT price FROM products WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(0.0));
}

#[test]
fn delete_via_index_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE category = 'Toys'")
            .unwrap(),
        2,
    );
    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(6));
    conn.execute("COMMIT").unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(6));
}

#[test]
fn rollback_undoes_index_assisted_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("DELETE FROM products WHERE name = 'Gizmo'")
            .unwrap(),
        1,
    );
    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(7));
    conn.execute("ROLLBACK").unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(8));
    let qr = query_result(
        conn.execute("SELECT id FROM products WHERE name = 'Gizmo'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn rollback_undoes_index_assisted_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    conn.execute("BEGIN").unwrap();
    assert_rows_affected(
        conn.execute("UPDATE products SET price = 0.0 WHERE category = 'Electronics'")
            .unwrap(),
        4,
    );
    conn.execute("ROLLBACK").unwrap();

    // Prices should be unchanged
    let qr = query_result(
        conn.execute("SELECT price FROM products WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(29.99));
}

// ═══════════════════════════════════════════════════════════════════
//  INDEX SCAN vs FULL SCAN: prove same results
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_scan_matches_full_scan_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, val TEXT, score INTEGER)",
        )
        .unwrap(),
    );

    for i in 0..50 {
        conn.execute(&format!(
            "INSERT INTO items VALUES ({i}, 'item_{i}', {})",
            i % 10
        ))
        .unwrap();
    }

    // Full scan result (no index)
    let full_scan = query_result(
        conn.execute("SELECT id FROM items WHERE score = 5 ORDER BY id")
            .unwrap(),
    );

    // Create index, now it uses index scan
    assert_ok(
        conn.execute("CREATE INDEX idx_score ON items (score)")
            .unwrap(),
    );
    assert_plan(
        &db,
        "SELECT id FROM items WHERE score = 5 ORDER BY id",
        "IndexScan:idx_score",
    );

    let index_scan = query_result(
        conn.execute("SELECT id FROM items WHERE score = 5 ORDER BY id")
            .unwrap(),
    );

    assert_eq!(full_scan.rows, index_scan.rows);
    assert_eq!(index_scan.rows.len(), 5);
}

#[test]
fn index_range_scan_matches_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE scores (id INTEGER NOT NULL PRIMARY KEY, grade INTEGER NOT NULL)",
        )
        .unwrap(),
    );

    for i in 1..=20 {
        conn.execute(&format!("INSERT INTO scores VALUES ({i}, {})", i * 5))
            .unwrap();
    }

    // Full scan
    let full_scan = query_result(
        conn.execute("SELECT id FROM scores WHERE grade >= 50 AND grade < 80 ORDER BY id")
            .unwrap(),
    );

    assert_ok(
        conn.execute("CREATE INDEX idx_grade ON scores (grade)")
            .unwrap(),
    );
    assert_plan(
        &db,
        "SELECT id FROM scores WHERE grade >= 50 AND grade < 80 ORDER BY id",
        "IndexScan:idx_grade",
    );

    let index_scan = query_result(
        conn.execute("SELECT id FROM scores WHERE grade >= 50 AND grade < 80 ORDER BY id")
            .unwrap(),
    );

    assert_eq!(full_scan.rows, index_scan.rows);
    // grade 50..79 -> ids: 10(50), 11(55), 12(60), 13(65), 14(70), 15(75)
    assert_eq!(index_scan.rows.len(), 6);
}

// ═══════════════════════════════════════════════════════════════════
//  SCALE TEST: 1000 rows - prove index returns correct subset
// ═══════════════════════════════════════════════════════════════════

#[test]
fn scale_1000_rows_index_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, bucket INTEGER NOT NULL, data TEXT)"
    ).unwrap());
    assert_ok(
        conn.execute("CREATE INDEX idx_bucket ON big (bucket)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(&format!(
            "INSERT INTO big VALUES ({i}, {}, 'row_{i}')",
            i % 20
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    assert_plan(
        &db,
        "SELECT id FROM big WHERE bucket = 7",
        "IndexScan:idx_bucket",
    );

    let qr = query_result(
        conn.execute("SELECT id FROM big WHERE bucket = 7 ORDER BY id")
            .unwrap(),
    );
    // bucket=7: ids 7, 27, 47, ... 987 -> 50 rows
    assert_eq!(qr.rows.len(), 50);
    assert_eq!(qr.rows[0][0], Value::Integer(7));
    assert_eq!(qr.rows[49][0], Value::Integer(987));

    // Verify every returned id mod 20 == 7
    for row in &qr.rows {
        if let Value::Integer(id) = row[0] {
            assert_eq!(id % 20, 7, "id {id} should have bucket 7");
        }
    }
}

#[test]
fn scale_1000_rows_index_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON big (val)").unwrap());

    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO big VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    assert_plan(
        &db,
        "SELECT id FROM big WHERE val >= 500 AND val < 510",
        "IndexScan:idx_val",
    );

    let qr = query_result(
        conn.execute("SELECT id FROM big WHERE val >= 500 AND val < 510 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 10);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(500 + i as i64));
    }
}

#[test]
fn scale_1000_rows_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, data TEXT NOT NULL)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO big VALUES ({i}, 'row_{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    assert_plan(&db, "SELECT data FROM big WHERE id = 777", "PkLookup");

    let qr = query_result(conn.execute("SELECT data FROM big WHERE id = 777").unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("row_777".into()));
}

#[test]
fn scale_1000_rows_delete_via_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, bucket INTEGER NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE INDEX idx_bucket ON big (bucket)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO big VALUES ({i}, {})", i % 10))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Delete all rows with bucket=3 (100 rows)
    assert_rows_affected(
        conn.execute("DELETE FROM big WHERE bucket = 3").unwrap(),
        100,
    );

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM big").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(900));

    // Verify no rows with bucket=3 remain
    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM big WHERE bucket = 3")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

#[test]
fn scale_1000_rows_update_via_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE big (id INTEGER NOT NULL PRIMARY KEY, bucket INTEGER NOT NULL, val INTEGER)"
    ).unwrap());
    assert_ok(
        conn.execute("CREATE INDEX idx_bucket ON big (bucket)")
            .unwrap(),
    );

    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO big VALUES ({i}, {}, 0)", i % 10))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Update all rows with bucket=5 (100 rows)
    assert_rows_affected(
        conn.execute("UPDATE big SET val = 999 WHERE bucket = 5")
            .unwrap(),
        100,
    );

    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM big WHERE bucket = 5 AND val = 999")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(100));
}

// ═══════════════════════════════════════════════════════════════════
//  EDGE CASES
// ═══════════════════════════════════════════════════════════════════

#[test]
fn empty_table_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE empty (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON empty (val)").unwrap());

    let qr = query_result(conn.execute("SELECT * FROM empty WHERE val = 'x'").unwrap());
    assert_eq!(qr.rows.len(), 0);

    let qr = query_result(conn.execute("SELECT * FROM empty WHERE id = 1").unwrap());
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn index_on_column_with_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE data (id INTEGER NOT NULL PRIMARY KEY, tag TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_tag ON data (tag)").unwrap());

    conn.execute("INSERT INTO data VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO data VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO data VALUES (3, 'b')").unwrap();
    conn.execute("INSERT INTO data VALUES (4, NULL)").unwrap();
    conn.execute("INSERT INTO data VALUES (5, 'a')").unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM data WHERE tag = 'a' ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(5));
}

#[test]
fn unique_index_with_null_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE data (id INTEGER NOT NULL PRIMARY KEY, code TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE UNIQUE INDEX idx_code ON data (code)")
            .unwrap(),
    );

    // Multiple NULLs allowed in unique index
    conn.execute("INSERT INTO data VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO data VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO data VALUES (3, 'abc')").unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM data WHERE code = 'abc'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn composite_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE orders (cust INTEGER NOT NULL, ord INTEGER NOT NULL, amount REAL, PRIMARY KEY (cust, ord))"
    ).unwrap());
    conn.execute("INSERT INTO orders VALUES (1, 100, 50.0)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (1, 101, 75.0)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (2, 100, 30.0)")
        .unwrap();

    assert_plan(
        &db,
        "SELECT amount FROM orders WHERE cust = 1 AND ord = 101",
        "PkLookup",
    );

    let qr = query_result(
        conn.execute("SELECT amount FROM orders WHERE cust = 1 AND ord = 101")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Real(75.0));
}

#[test]
fn partial_composite_pk_is_not_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE orders (cust INTEGER NOT NULL, ord INTEGER NOT NULL, amount REAL, PRIMARY KEY (cust, ord))"
    ).unwrap());
    conn.execute("INSERT INTO orders VALUES (1, 100, 50.0)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (1, 101, 75.0)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (2, 100, 30.0)")
        .unwrap();

    // Only one PK column specified - can't do PK lookup
    assert_plan(&db, "SELECT * FROM orders WHERE cust = 1", "SeqScan");

    let qr = query_result(
        conn.execute("SELECT amount FROM orders WHERE cust = 1 ORDER BY ord")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Real(50.0));
    assert_eq!(qr.rows[1][0], Value::Real(75.0));
}

#[test]
fn or_condition_correct_with_indexes_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // OR falls back to SeqScan but results must still be correct
    assert_plan(
        &db,
        "SELECT name FROM products WHERE category = 'Toys' OR category = 'Tools' ORDER BY name",
        "SeqScan",
    );

    let qr = query_result(
        conn.execute(
            "SELECT name FROM products WHERE category = 'Toys' OR category = 'Tools' ORDER BY name",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Text("Apparatus".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Contraption".into()));
    assert_eq!(qr.rows[2][0], Value::Text("Doohickey".into()));
    assert_eq!(qr.rows[3][0], Value::Text("Thingamajig".into()));
}

#[test]
fn single_row_table_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE single (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO single VALUES (1, 'only')")
        .unwrap();

    let qr = query_result(conn.execute("SELECT val FROM single WHERE id = 1").unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("only".into()));

    let qr = query_result(conn.execute("SELECT val FROM single WHERE id = 2").unwrap());
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn single_row_table_index_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE single (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE INDEX idx_val ON single (val)")
            .unwrap(),
    );
    conn.execute("INSERT INTO single VALUES (1, 'only')")
        .unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM single WHERE val = 'only'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = query_result(
        conn.execute("SELECT id FROM single WHERE val = 'none'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn delete_all_via_index_then_reinsert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, cat TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_cat ON t (cat)").unwrap());
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'b')").unwrap();

    assert_rows_affected(conn.execute("DELETE FROM t WHERE cat = 'a'").unwrap(), 2);
    assert_rows_affected(conn.execute("DELETE FROM t WHERE cat = 'b'").unwrap(), 1);

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(0));

    // Reinsert and verify index still works
    conn.execute("INSERT INTO t VALUES (10, 'a')").unwrap();
    let qr = query_result(conn.execute("SELECT id FROM t WHERE cat = 'a'").unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(10));
}

// ═══════════════════════════════════════════════════════════════════
//  AGGREGATION + DISTINCT + ORDER BY + LIMIT with index pre-filter
// ═══════════════════════════════════════════════════════════════════

#[test]
fn aggregate_sum_with_index_prefilter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT SUM(price) FROM products WHERE category = 'Electronics'")
            .unwrap(),
    );
    match &qr.rows[0][0] {
        Value::Real(sum) => {
            let expected = 29.99 + 49.99 + 99.99 + 19.99;
            assert!((sum - expected).abs() < 0.01);
        }
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn aggregate_count_with_index_prefilter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM products WHERE category = 'Toys'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn aggregate_min_max_with_index_prefilter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute("SELECT MIN(price), MAX(price) FROM products WHERE category = 'Electronics'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Real(19.99));
    assert_eq!(qr.rows[0][1], Value::Real(99.99));
}

#[test]
fn distinct_with_index_prefilter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute(
            "SELECT DISTINCT stock FROM products WHERE category = 'Electronics' ORDER BY stock",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(25));
    assert_eq!(qr.rows[1][0], Value::Integer(50));
    assert_eq!(qr.rows[2][0], Value::Integer(100));
    assert_eq!(qr.rows[3][0], Value::Integer(120));
}

#[test]
fn order_by_limit_with_index_prefilter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(
        conn.execute(
            "SELECT name FROM products WHERE category = 'Electronics' ORDER BY price LIMIT 2",
        )
        .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Mechanism".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Widget".into()));
}

#[test]
fn order_by_limit_offset_with_index_prefilter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    let qr = query_result(conn.execute(
        "SELECT name FROM products WHERE category = 'Electronics' ORDER BY price LIMIT 2 OFFSET 1"
    ).unwrap());
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("Widget".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Gadget".into()));
}

// ═══════════════════════════════════════════════════════════════════
//  INDEX CONSISTENCY AFTER MUTATIONS
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_consistent_after_insert_update_delete_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // Insert - index should include new row
    conn.execute("INSERT INTO products VALUES (9, 'Sprocket', 'Electronics', 5.99, 1000)")
        .unwrap();
    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM products WHERE category = 'Electronics'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    let qr = query_result(
        conn.execute("SELECT id FROM products WHERE name = 'Sprocket'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(9));

    // Update category - old index entry removed, new one added
    conn.execute("UPDATE products SET category = 'Tools' WHERE name = 'Sprocket'")
        .unwrap();
    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM products WHERE category = 'Electronics'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(4));
    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM products WHERE category = 'Tools'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    // Delete - index entry removed
    conn.execute("DELETE FROM products WHERE name = 'Sprocket'")
        .unwrap();
    let qr = query_result(
        conn.execute("SELECT COUNT(*) FROM products WHERE category = 'Tools'")
            .unwrap(),
    );
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    let qr = query_result(
        conn.execute("SELECT * FROM products WHERE name = 'Sprocket'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn index_scan_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup_products(&mut conn);
    }

    // Reopen
    let db_path = dir.path().join("test.db");
    let db = DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let mut conn = Connection::open(&db).unwrap();

    // Plan should still use index
    assert_plan(
        &db,
        "SELECT * FROM products WHERE name = 'Widget'",
        "IndexScan:idx_name",
    );

    let qr = query_result(
        conn.execute("SELECT id FROM products WHERE name = 'Widget'")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));

    let qr = query_result(
        conn.execute("SELECT id FROM products WHERE category = 'Electronics' ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 4);
}

// ═══════════════════════════════════════════════════════════════════
//  BOUNDARY VALUES
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_scan_with_empty_string() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, '')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'abc')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, '')").unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM t WHERE val = '' ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn index_scan_with_negative_integers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, -100)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 100)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, -100)").unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM t WHERE val = -100 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(4));

    let qr = query_result(
        conn.execute("SELECT id FROM t WHERE val > -50 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn index_scan_with_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 0)").unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM t WHERE val = 0 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn pk_lookup_with_large_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (9223372036854775807, 'max_i64')")
        .unwrap();

    let qr = query_result(
        conn.execute("SELECT val FROM t WHERE id = 9223372036854775807")
            .unwrap(),
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("max_i64".into()));
}

#[test]
fn index_scan_with_real_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_val ON t (val)").unwrap());

    conn.execute("INSERT INTO t VALUES (1, 0.0)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, -0.0)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 0.1)").unwrap();

    let qr = query_result(
        conn.execute("SELECT id FROM t WHERE val = 0.0 ORDER BY id")
            .unwrap(),
    );
    // 0.0 and -0.0 should both match
    assert!(!qr.rows.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
//  MULTIPLE INDEXES: planner picks best one
// ═══════════════════════════════════════════════════════════════════

#[test]
fn prefers_unique_over_non_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, code TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(conn.execute("CREATE INDEX idx_code ON t (code)").unwrap());
    assert_ok(
        conn.execute("CREATE UNIQUE INDEX idx_code_uniq ON t (code)")
            .unwrap(),
    );

    conn.execute("INSERT INTO t VALUES (1, 'X')").unwrap();

    assert_plan(
        &db,
        "SELECT * FROM t WHERE code = 'X'",
        "IndexScan:idx_code_uniq",
    );

    let qr = query_result(conn.execute("SELECT id FROM t WHERE code = 'X'").unwrap());
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn prefers_more_equality_columns_composite() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    // WHERE category = 'X' AND price = Y
    // idx_category has 1 eq col, idx_cat_price has 2 eq cols -> pick idx_cat_price
    assert_plan(
        &db,
        "SELECT * FROM products WHERE category = 'Electronics' AND price = 29.99",
        "IndexScan:idx_cat_price",
    );
}

// ═══════════════════════════════════════════════════════════════════
//  DELETE ALL WITHOUT WHERE (always SeqScan)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn delete_all_is_seq_scan_with_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(&db, "DELETE FROM products", "SeqScan");
    assert_rows_affected(conn.execute("DELETE FROM products").unwrap(), 8);
    let qr = query_result(conn.execute("SELECT COUNT(*) FROM products").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ═══════════════════════════════════════════════════════════════════
//  INDEX NOT USED: IS NULL, IS NOT NULL, expression comparisons
// ═══════════════════════════════════════════════════════════════════

#[test]
fn is_null_falls_back_to_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category IS NULL",
        "SeqScan",
    );
}

#[test]
fn is_not_null_falls_back_to_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category IS NOT NULL",
        "SeqScan",
    );
}

#[test]
fn not_equal_falls_back_to_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_products(&mut conn);

    assert_plan(
        &db,
        "SELECT * FROM products WHERE category <> 'Toys'",
        "SeqScan",
    );
}

// ═══════════════════════════════════════════════════════════════════
//  BOOLEAN INDEX
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_on_boolean_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE flags (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
        )
        .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE INDEX idx_active ON flags (active)")
            .unwrap(),
    );

    for i in 0..20 {
        conn.execute(&format!(
            "INSERT INTO flags VALUES ({i}, {})",
            if i % 3 == 0 { "TRUE" } else { "FALSE" }
        ))
        .unwrap();
    }

    assert_plan(
        &db,
        "SELECT id FROM flags WHERE active = TRUE",
        "IndexScan:idx_active",
    );

    let qr = query_result(
        conn.execute("SELECT id FROM flags WHERE active = TRUE ORDER BY id")
            .unwrap(),
    );
    // ids 0, 3, 6, 9, 12, 15, 18 -> 7 rows
    assert_eq!(qr.rows.len(), 7);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
    assert_eq!(qr.rows[6][0], Value::Integer(18));
}
