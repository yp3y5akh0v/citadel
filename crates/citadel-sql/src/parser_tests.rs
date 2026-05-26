use super::*;

#[test]
fn parse_create_table() {
    let stmt =
        parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
            .unwrap();

    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.name, "users");
            assert_eq!(ct.columns.len(), 3);
            assert_eq!(ct.columns[0].name, "id");
            assert_eq!(ct.columns[0].data_type, DataType::Integer);
            assert!(ct.columns[0].is_primary_key);
            assert!(!ct.columns[0].nullable);
            assert_eq!(ct.columns[1].name, "name");
            assert_eq!(ct.columns[1].data_type, DataType::Text);
            assert!(!ct.columns[1].nullable);
            assert_eq!(ct.columns[2].name, "age");
            assert!(ct.columns[2].nullable);
            assert_eq!(ct.primary_key, vec!["id"]);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_create_table_if_not_exists() {
    let stmt = parse_sql("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)").unwrap();
    match stmt {
        Statement::CreateTable(ct) => assert!(ct.if_not_exists),
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_drop_table() {
    let stmt = parse_sql("DROP TABLE users").unwrap();
    match stmt {
        Statement::DropTable(dt) => {
            assert_eq!(dt.name, "users");
            assert!(!dt.if_exists);
        }
        _ => panic!("expected DropTable"),
    }
}

#[test]
fn parse_drop_table_if_exists() {
    let stmt = parse_sql("DROP TABLE IF EXISTS users").unwrap();
    match stmt {
        Statement::DropTable(dt) => assert!(dt.if_exists),
        _ => panic!("expected DropTable"),
    }
}

#[test]
fn parse_insert() {
    let stmt = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.table, "users");
            assert_eq!(ins.columns, vec!["id", "name"]);
            let values = match &ins.source {
                InsertSource::Values(v) => v,
                _ => panic!("expected Values"),
            };
            assert_eq!(values.len(), 2);
            assert!(matches!(values[0][0], Expr::Literal(Value::Integer(1))));
            assert!(matches!(&values[0][1], Expr::Literal(Value::Text(s)) if s == "Alice"));
            assert!(ins.on_conflict.is_none());
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_do_nothing() {
    let stmt =
        parse_sql("INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            match oc.target.expect("target") {
                ConflictTarget::Columns(cols) => assert_eq!(cols, vec!["id"]),
                _ => panic!("expected Columns target"),
            }
            assert!(matches!(oc.action, OnConflictAction::DoNothing));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_do_nothing_no_target() {
    let stmt = parse_sql("INSERT INTO t VALUES (1, 'a') ON CONFLICT DO NOTHING").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            assert!(oc.target.is_none());
            assert!(matches!(oc.action, OnConflictAction::DoNothing));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_do_update_simple() {
    let stmt =
        parse_sql("INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = 'b'")
            .unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            match oc.action {
                OnConflictAction::DoUpdate {
                    assignments,
                    where_clause,
                } => {
                    assert_eq!(assignments.len(), 1);
                    assert_eq!(assignments[0].0, "v");
                    assert!(where_clause.is_none());
                }
                _ => panic!("expected DoUpdate"),
            }
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_do_update_excluded() {
    let stmt = parse_sql(
        "INSERT INTO t (id, v) VALUES (1, 'a') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    )
    .unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            let assignments = match oc.action {
                OnConflictAction::DoUpdate { assignments, .. } => assignments,
                _ => panic!("expected DoUpdate"),
            };
            match &assignments[0].1 {
                Expr::QualifiedColumn { table, column } => {
                    assert_eq!(table, "excluded");
                    assert_eq!(column, "v");
                }
                _ => panic!("expected QualifiedColumn"),
            }
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_do_update_where() {
    let stmt = parse_sql(
        "INSERT INTO t (id, v) VALUES (1, 'a') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE t.v < 'z'",
    )
    .unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            match oc.action {
                OnConflictAction::DoUpdate { where_clause, .. } => {
                    assert!(where_clause.is_some());
                }
                _ => panic!("expected DoUpdate"),
            }
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_on_constraint_named() {
    let stmt = parse_sql(
        "INSERT INTO t (id, v) VALUES (1, 'a') \
         ON CONFLICT ON CONSTRAINT t_v_idx DO NOTHING",
    )
    .unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            match oc.target.expect("target") {
                ConflictTarget::Constraint(name) => assert_eq!(name, "t_v_idx"),
                _ => panic!("expected Constraint target"),
            }
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_upsert_rejects_duplicate_key_update() {
    let err = parse_sql("INSERT INTO t (id) VALUES (1) ON DUPLICATE KEY UPDATE id = 2")
        .expect_err("should reject MySQL syntax");
    let msg = format!("{err}");
    assert!(msg.contains("ON DUPLICATE KEY UPDATE") || msg.contains("MySQL"));
}

#[test]
fn parse_upsert_lowercases_conflict_target() {
    let stmt = parse_sql("INSERT INTO t (id) VALUES (1) ON CONFLICT (ID) DO NOTHING").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let oc = ins.on_conflict.expect("expected on_conflict");
            match oc.target.expect("target") {
                ConflictTarget::Columns(cols) => assert_eq!(cols, vec!["id"]),
                _ => panic!("expected Columns"),
            }
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_select_all() {
    let stmt = parse_sql("SELECT * FROM users").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.from, "users");
                assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
                assert!(sel.where_clause.is_none());
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_where() {
    let stmt = parse_sql("SELECT id, name FROM users WHERE age > 18").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                assert!(sel.where_clause.is_some());
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_order_limit() {
    let stmt = parse_sql("SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 5").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(!sel.order_by[0].descending);
                assert!(sel.limit.is_some());
                assert!(sel.offset.is_some());
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_update() {
    let stmt = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
    match stmt {
        Statement::Update(upd) => {
            assert_eq!(upd.table, "users");
            assert_eq!(upd.assignments.len(), 1);
            assert_eq!(upd.assignments[0].0, "name");
            assert!(upd.where_clause.is_some());
        }
        _ => panic!("expected Update"),
    }
}

#[test]
fn parse_delete() {
    let stmt = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
    match stmt {
        Statement::Delete(del) => {
            assert_eq!(del.table, "users");
            assert!(del.where_clause.is_some());
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn parse_aggregate() {
    let stmt = parse_sql("SELECT COUNT(*), SUM(age) FROM users").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                match &sel.columns[0] {
                    SelectColumn::Expr {
                        expr: Expr::CountStar,
                        ..
                    } => {}
                    other => panic!("expected CountStar, got {other:?}"),
                }
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_group_by_having() {
    let stmt = parse_sql(
        "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
    )
    .unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert!(sel.having.is_some());
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_expressions() {
    let stmt = parse_sql("SELECT id + 1, -price, NOT active FROM items").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.columns.len(), 3);
                match &sel.columns[0] {
                    SelectColumn::Expr {
                        expr: Expr::BinaryOp { op: BinOp::Add, .. },
                        ..
                    } => {}
                    other => panic!("expected BinaryOp Add, got {other:?}"),
                }
                match &sel.columns[1] {
                    SelectColumn::Expr {
                        expr:
                            Expr::UnaryOp {
                                op: UnaryOp::Neg, ..
                            },
                        ..
                    } => {}
                    other => panic!("expected UnaryOp Neg, got {other:?}"),
                }
                match &sel.columns[2] {
                    SelectColumn::Expr {
                        expr:
                            Expr::UnaryOp {
                                op: UnaryOp::Not, ..
                            },
                        ..
                    } => {}
                    other => panic!("expected UnaryOp Not, got {other:?}"),
                }
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_is_null() {
    let stmt = parse_sql("SELECT * FROM t WHERE x IS NULL").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert!(matches!(sel.where_clause, Some(Expr::IsNull(_))));
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_inner_join() {
    let stmt = parse_sql("SELECT * FROM a JOIN b ON a.id = b.id").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.from, "a");
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Inner);
                assert_eq!(sel.joins[0].table.name, "b");
                assert!(sel.joins[0].on_clause.is_some());
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_inner_join_explicit() {
    let stmt = parse_sql("SELECT * FROM a INNER JOIN b ON a.id = b.a_id").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Inner);
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_cross_join() {
    let stmt = parse_sql("SELECT * FROM a CROSS JOIN b").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Cross);
                assert!(sel.joins[0].on_clause.is_none());
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_left_join() {
    let stmt = parse_sql("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert_eq!(sel.joins[0].join_type, JoinType::Left);
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_table_alias() {
    let stmt = parse_sql("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.from, "users");
                assert_eq!(sel.from_alias.as_deref(), Some("u"));
                assert_eq!(sel.joins[0].table.name, "orders");
                assert_eq!(sel.joins[0].table.alias.as_deref(), Some("o"));
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_multi_join() {
    let stmt =
        parse_sql("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert_eq!(sel.joins.len(), 2);
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_qualified_column() {
    let stmt = parse_sql("SELECT u.id, u.name FROM users u").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => match &sel.columns[0] {
                SelectColumn::Expr {
                    expr: Expr::QualifiedColumn { table, column },
                    ..
                } => {
                    assert_eq!(table, "u");
                    assert_eq!(column, "id");
                }
                other => panic!("expected QualifiedColumn, got {other:?}"),
            },
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn reject_subquery() {
    assert!(parse_sql("SELECT * FROM (SELECT 1)").is_err());
}

#[test]
fn parse_type_mapping() {
    let stmt = parse_sql(
        "CREATE TABLE t (a INT PRIMARY KEY, b BIGINT, c SMALLINT, d REAL, e DOUBLE PRECISION, f VARCHAR(255), g BOOLEAN, h BLOB, i BYTEA)"
    ).unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns[0].data_type, DataType::Integer); // INT
            assert_eq!(ct.columns[1].data_type, DataType::Integer); // BIGINT
            assert_eq!(ct.columns[2].data_type, DataType::Integer); // SMALLINT
            assert_eq!(ct.columns[3].data_type, DataType::Real); // REAL
            assert_eq!(ct.columns[4].data_type, DataType::Real); // DOUBLE
            assert_eq!(ct.columns[5].data_type, DataType::Text); // VARCHAR
            assert_eq!(ct.columns[6].data_type, DataType::Boolean); // BOOLEAN
            assert_eq!(ct.columns[7].data_type, DataType::Blob); // BLOB
            assert_eq!(ct.columns[8].data_type, DataType::Blob); // BYTEA
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_boolean_literals() {
    let stmt = parse_sql("INSERT INTO t (a, b) VALUES (true, false)").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let values = match &ins.source {
                InsertSource::Values(v) => v,
                _ => panic!("expected Values"),
            };
            assert!(matches!(values[0][0], Expr::Literal(Value::Boolean(true))));
            assert!(matches!(values[0][1], Expr::Literal(Value::Boolean(false))));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_null_literal() {
    let stmt = parse_sql("INSERT INTO t (a) VALUES (NULL)").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let values = match &ins.source {
                InsertSource::Values(v) => v,
                _ => panic!("expected Values"),
            };
            assert!(matches!(values[0][0], Expr::Literal(Value::Null)));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_alias() {
    let stmt = parse_sql("SELECT id AS user_id FROM users").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => match &sel.columns[0] {
                SelectColumn::Expr { alias: Some(a), .. } => assert_eq!(a, "user_id"),
                other => panic!("expected alias, got {other:?}"),
            },
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_begin() {
    let stmt = parse_sql("BEGIN").unwrap();
    assert!(matches!(
        stmt,
        Statement::Begin {
            access_mode: BeginAccessMode::Default
        }
    ));
}

#[test]
fn parse_begin_transaction() {
    let stmt = parse_sql("BEGIN TRANSACTION").unwrap();
    assert!(matches!(
        stmt,
        Statement::Begin {
            access_mode: BeginAccessMode::Default
        }
    ));
}

#[test]
fn parse_begin_read_only() {
    let stmt = parse_sql("BEGIN READ ONLY").unwrap();
    assert!(matches!(
        stmt,
        Statement::Begin {
            access_mode: BeginAccessMode::ReadOnly
        }
    ));
}

#[test]
fn parse_begin_read_write() {
    let stmt = parse_sql("BEGIN READ WRITE").unwrap();
    assert!(matches!(
        stmt,
        Statement::Begin {
            access_mode: BeginAccessMode::ReadWrite
        }
    ));
}

#[test]
fn parse_commit() {
    let stmt = parse_sql("COMMIT").unwrap();
    assert!(matches!(stmt, Statement::Commit));
}

#[test]
fn parse_rollback() {
    let stmt = parse_sql("ROLLBACK").unwrap();
    assert!(matches!(stmt, Statement::Rollback));
}

#[test]
fn parse_savepoint() {
    let stmt = parse_sql("SAVEPOINT sp1").unwrap();
    match stmt {
        Statement::Savepoint(name) => assert_eq!(name, "sp1"),
        other => panic!("expected Savepoint, got {other:?}"),
    }
}

#[test]
fn parse_savepoint_case_insensitive() {
    let stmt = parse_sql("SAVEPOINT My_SP").unwrap();
    match stmt {
        Statement::Savepoint(name) => assert_eq!(name, "my_sp"),
        other => panic!("expected Savepoint, got {other:?}"),
    }
}

#[test]
fn parse_release_savepoint() {
    let stmt = parse_sql("RELEASE SAVEPOINT sp1").unwrap();
    match stmt {
        Statement::ReleaseSavepoint(name) => assert_eq!(name, "sp1"),
        other => panic!("expected ReleaseSavepoint, got {other:?}"),
    }
}

#[test]
fn parse_release_without_savepoint_keyword() {
    let stmt = parse_sql("RELEASE sp1").unwrap();
    match stmt {
        Statement::ReleaseSavepoint(name) => assert_eq!(name, "sp1"),
        other => panic!("expected ReleaseSavepoint, got {other:?}"),
    }
}

#[test]
fn parse_rollback_to_savepoint() {
    let stmt = parse_sql("ROLLBACK TO SAVEPOINT sp1").unwrap();
    match stmt {
        Statement::RollbackTo(name) => assert_eq!(name, "sp1"),
        other => panic!("expected RollbackTo, got {other:?}"),
    }
}

#[test]
fn parse_rollback_to_without_savepoint_keyword() {
    let stmt = parse_sql("ROLLBACK TO sp1").unwrap();
    match stmt {
        Statement::RollbackTo(name) => assert_eq!(name, "sp1"),
        other => panic!("expected RollbackTo, got {other:?}"),
    }
}

#[test]
fn parse_rollback_to_case_insensitive() {
    let stmt = parse_sql("ROLLBACK TO My_SP").unwrap();
    match stmt {
        Statement::RollbackTo(name) => assert_eq!(name, "my_sp"),
        other => panic!("expected RollbackTo, got {other:?}"),
    }
}

#[test]
fn parse_commit_and_chain_rejected() {
    let err = parse_sql("COMMIT AND CHAIN").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn parse_rollback_and_chain_rejected() {
    let err = parse_sql("ROLLBACK AND CHAIN").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn parse_select_distinct() {
    let stmt = parse_sql("SELECT DISTINCT name FROM users").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert!(sel.distinct);
                assert_eq!(sel.columns.len(), 1);
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_without_distinct() {
    let stmt = parse_sql("SELECT name FROM users").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert!(!sel.distinct);
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_distinct_all_columns() {
    let stmt = parse_sql("SELECT DISTINCT * FROM users").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => {
                assert!(sel.distinct);
                assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
            }
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn reject_distinct_on() {
    assert!(parse_sql("SELECT DISTINCT ON (id) * FROM users").is_err());
}

#[test]
fn parse_create_index() {
    let stmt = parse_sql("CREATE INDEX idx_name ON users (name)").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert_eq!(ci.index_name, "idx_name");
            assert_eq!(ci.table_name, "users");
            assert_eq!(ci.columns, vec!["name"]);
            assert!(!ci.unique);
            assert!(!ci.if_not_exists);
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_unique_index() {
    let stmt = parse_sql("CREATE UNIQUE INDEX idx_email ON users (email)").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert!(ci.unique);
            assert_eq!(ci.columns, vec!["email"]);
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_if_not_exists() {
    let stmt = parse_sql("CREATE INDEX IF NOT EXISTS idx_x ON t (a)").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => assert!(ci.if_not_exists),
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_multi_column() {
    let stmt = parse_sql("CREATE INDEX idx_multi ON t (a, b, c)").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert_eq!(ci.columns, vec!["a", "b", "c"]);
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_drop_index() {
    let stmt = parse_sql("DROP INDEX idx_name").unwrap();
    match stmt {
        Statement::DropIndex(di) => {
            assert_eq!(di.index_name, "idx_name");
            assert!(!di.if_exists);
        }
        _ => panic!("expected DropIndex"),
    }
}

#[test]
fn parse_drop_index_if_exists() {
    let stmt = parse_sql("DROP INDEX IF EXISTS idx_name").unwrap();
    match stmt {
        Statement::DropIndex(di) => {
            assert!(di.if_exists);
        }
        _ => panic!("expected DropIndex"),
    }
}

#[test]
fn parse_explain_select() {
    let stmt = parse_sql("EXPLAIN SELECT * FROM users WHERE id = 1").unwrap();
    match stmt {
        Statement::Explain(inner) => {
            assert!(matches!(*inner, Statement::Select(_)));
        }
        _ => panic!("expected Explain"),
    }
}

#[test]
fn parse_explain_insert() {
    let stmt = parse_sql("EXPLAIN INSERT INTO t (a) VALUES (1)").unwrap();
    assert!(matches!(stmt, Statement::Explain(_)));
}

#[test]
fn reject_explain_analyze() {
    assert!(parse_sql("EXPLAIN ANALYZE SELECT * FROM t").is_err());
}

#[test]
fn parse_parameter_placeholder() {
    let stmt = parse_sql("SELECT * FROM t WHERE id = $1").unwrap();
    match stmt {
        Statement::Select(sq) => match sq.body {
            QueryBody::Select(sel) => match &sel.where_clause {
                Some(Expr::BinaryOp { right, .. }) => {
                    assert!(matches!(right.as_ref(), Expr::Parameter(1)));
                }
                other => panic!("expected BinaryOp with Parameter, got {other:?}"),
            },
            _ => panic!("expected QueryBody::Select"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_multiple_parameters() {
    let stmt = parse_sql("INSERT INTO t (a, b) VALUES ($1, $2)").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            let values = match &ins.source {
                InsertSource::Values(v) => v,
                _ => panic!("expected Values"),
            };
            assert!(matches!(values[0][0], Expr::Parameter(1)));
            assert!(matches!(values[0][1], Expr::Parameter(2)));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_insert_select() {
    let stmt = parse_sql("INSERT INTO t2 (id, name) SELECT id, name FROM t1 WHERE id > 5").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.table, "t2");
            assert_eq!(ins.columns, vec!["id", "name"]);
            match &ins.source {
                InsertSource::Select(sq) => match &sq.body {
                    QueryBody::Select(sel) => {
                        assert_eq!(sel.from, "t1");
                        assert_eq!(sel.columns.len(), 2);
                        assert!(sel.where_clause.is_some());
                    }
                    _ => panic!("expected QueryBody::Select"),
                },
                _ => panic!("expected InsertSource::Select"),
            }
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_insert_select_no_columns() {
    let stmt = parse_sql("INSERT INTO t2 SELECT * FROM t1").unwrap();
    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.table, "t2");
            assert!(ins.columns.is_empty());
            assert!(matches!(&ins.source, InsertSource::Select(_)));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn reject_zero_parameter() {
    assert!(parse_sql("SELECT $0 FROM t").is_err());
}

#[test]
fn count_params_basic() {
    let stmt = parse_sql("SELECT * FROM t WHERE a = $1 AND b = $3").unwrap();
    assert_eq!(count_params(&stmt), 3);
}

#[test]
fn count_params_none() {
    let stmt = parse_sql("SELECT * FROM t WHERE a = 1").unwrap();
    assert_eq!(count_params(&stmt), 0);
}

#[test]
fn parse_table_constraint_pk() {
    let stmt = parse_sql("CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a))").unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.primary_key, vec!["a"]);
            assert!(ct.columns[0].is_primary_key);
            assert!(!ct.columns[0].nullable);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_truncate_single() {
    let stmt = parse_sql("TRUNCATE TABLE t").unwrap();
    match stmt {
        Statement::Truncate(t) => assert_eq!(t.tables, vec!["t"]),
        _ => panic!("expected Truncate"),
    }
}

#[test]
fn parse_truncate_table_keyword_optional() {
    let stmt = parse_sql("TRUNCATE t").unwrap();
    match stmt {
        Statement::Truncate(t) => assert_eq!(t.tables, vec!["t"]),
        _ => panic!("expected Truncate"),
    }
}

#[test]
fn parse_truncate_multi_table() {
    let stmt = parse_sql("TRUNCATE TABLE a, b, c").unwrap();
    match stmt {
        Statement::Truncate(t) => assert_eq!(t.tables, vec!["a", "b", "c"]),
        _ => panic!("expected Truncate"),
    }
}

#[test]
fn parse_truncate_only_keyword() {
    let stmt = parse_sql("TRUNCATE TABLE ONLY t").unwrap();
    match stmt {
        Statement::Truncate(t) => assert_eq!(t.tables, vec!["t"]),
        _ => panic!("expected Truncate"),
    }
}

#[test]
fn parse_truncate_restart_identity() {
    let stmt = parse_sql("TRUNCATE TABLE t RESTART IDENTITY").unwrap();
    assert!(matches!(stmt, Statement::Truncate(_)));
}

#[test]
fn parse_truncate_continue_identity() {
    let stmt = parse_sql("TRUNCATE TABLE t CONTINUE IDENTITY").unwrap();
    assert!(matches!(stmt, Statement::Truncate(_)));
}

#[test]
fn parse_truncate_cascade_unsupported() {
    let err = parse_sql("TRUNCATE TABLE t CASCADE").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("v0.13")));
}

#[test]
fn parse_truncate_restrict_accepted() {
    let stmt = parse_sql("TRUNCATE TABLE t RESTRICT").unwrap();
    assert!(matches!(stmt, Statement::Truncate(_)));
}

#[test]
fn parse_create_index_with_predicate() {
    let stmt = parse_sql("CREATE INDEX i ON t (c) WHERE c IS NOT NULL").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert!(ci.predicate_sql.is_some());
            assert!(ci.predicate_expr.is_some());
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_without_predicate() {
    let stmt = parse_sql("CREATE INDEX i ON t (c)").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert!(ci.predicate_sql.is_none());
            assert!(ci.predicate_expr.is_none());
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_unique_index_with_predicate() {
    let stmt = parse_sql("CREATE UNIQUE INDEX i ON t (email) WHERE deleted_at IS NULL").unwrap();
    match stmt {
        Statement::CreateIndex(ci) => {
            assert!(ci.unique);
            assert!(ci.predicate_sql.is_some());
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn create_index_predicate_rejects_now() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE ts > now()").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("non-deterministic")));
}

#[test]
fn create_index_predicate_rejects_random() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE c > random()").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn create_index_predicate_rejects_aggregate() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE c > sum(c)").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("aggregates")));
}

#[test]
fn create_index_predicate_rejects_subquery() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE c IN (SELECT id FROM u)").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("subqueries")));
}

#[test]
fn parse_fk_default_no_action() {
    let stmt = parse_sql(
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES parent(id))",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            let fk = &ct.foreign_keys[0];
            assert_eq!(fk.on_delete, ReferentialAction::NoAction);
            assert_eq!(fk.on_update, ReferentialAction::NoAction);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_fk_on_delete_cascade() {
    let stmt = parse_sql(
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            let fk = &ct.foreign_keys[0];
            assert_eq!(fk.on_delete, ReferentialAction::Cascade);
            assert_eq!(fk.on_update, ReferentialAction::NoAction);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_fk_on_delete_set_null() {
    let stmt = parse_sql(
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE SET NULL)",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.foreign_keys[0].on_delete, ReferentialAction::SetNull);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_fk_on_delete_set_default() {
    let stmt = parse_sql(
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE SET DEFAULT)",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.foreign_keys[0].on_delete, ReferentialAction::SetDefault);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_fk_on_delete_restrict() {
    let stmt = parse_sql(
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, \
         FOREIGN KEY (p) REFERENCES parent(id) ON DELETE RESTRICT)",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.foreign_keys[0].on_delete, ReferentialAction::Restrict);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_fk_mixed_actions() {
    let stmt = parse_sql(
        "CREATE TABLE c (id INT PRIMARY KEY, p INT, \
         FOREIGN KEY (p) REFERENCES parent(id) \
         ON DELETE CASCADE ON UPDATE RESTRICT)",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(ct) => {
            let fk = &ct.foreign_keys[0];
            assert_eq!(fk.on_delete, ReferentialAction::Cascade);
            assert_eq!(fk.on_update, ReferentialAction::Restrict);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_full_outer_join() {
    let stmt = parse_sql("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id").unwrap();
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => panic!("expected Select"),
    };
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected Select body"),
    };
    assert_eq!(sel.joins.len(), 1);
    assert_eq!(sel.joins[0].join_type, JoinType::FullOuter);
}

#[test]
fn parse_left_outer_join_maps_to_left() {
    let stmt = parse_sql("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id").unwrap();
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => panic!("expected Select"),
    };
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected Select body"),
    };
    assert_eq!(sel.joins[0].join_type, JoinType::Left);
}

#[test]
fn parse_right_outer_join_maps_to_right() {
    let stmt = parse_sql("SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.a_id").unwrap();
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => panic!("expected Select"),
    };
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected Select body"),
    };
    assert_eq!(sel.joins[0].join_type, JoinType::Right);
}

#[test]
fn parse_derived_table_in_from() {
    let stmt = parse_sql("SELECT sub.x FROM (SELECT 1 AS x) sub").unwrap();
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => panic!("expected Select"),
    };
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected Select body"),
    };
    assert!(sel.from_subquery.is_some());
    let d = sel.from_subquery.as_ref().unwrap();
    assert_eq!(d.alias, "sub");
    assert!(!d.lateral);
}

#[test]
fn parse_lateral_in_join() {
    let stmt = parse_sql(
        "SELECT * FROM a LEFT JOIN LATERAL (SELECT * FROM b WHERE b.x = a.x) sub ON true",
    )
    .unwrap();
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => panic!("expected Select"),
    };
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected Select body"),
    };
    assert_eq!(sel.joins.len(), 1);
    let join_sub = sel.joins[0].subquery.as_ref().unwrap();
    assert_eq!(join_sub.alias, "sub");
    assert!(join_sub.lateral);
}

#[test]
fn parse_lateral_comma_form() {
    let stmt = parse_sql("SELECT * FROM a, LATERAL (SELECT * FROM b WHERE b.x = a.x) sub").unwrap();
    let sq = match stmt {
        Statement::Select(sq) => sq,
        _ => panic!("expected Select"),
    };
    let sel = match &sq.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected Select body"),
    };
    assert_eq!(sel.joins.len(), 1);
    assert_eq!(sel.joins[0].join_type, JoinType::Cross);
    let join_sub = sel.joins[0].subquery.as_ref().unwrap();
    assert!(join_sub.lateral);
}

#[test]
fn parse_lateral_with_right_join_rejected() {
    let result = parse_sql("SELECT * FROM a RIGHT JOIN LATERAL (SELECT * FROM b) sub ON true");
    assert!(matches!(result, Err(SqlError::Unsupported(_))));
}

#[test]
fn parse_lateral_with_full_outer_rejected() {
    let result = parse_sql("SELECT * FROM a FULL OUTER JOIN LATERAL (SELECT * FROM b) sub ON true");
    assert!(matches!(result, Err(SqlError::Unsupported(_))));
}
