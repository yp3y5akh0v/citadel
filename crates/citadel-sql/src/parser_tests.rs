use super::*;

#[test]
fn minimum_integer_literal_stays_integer() {
    for sql in ["-9223372036854775808", "-09223372036854775808"] {
        assert!(
            matches!(
                parse_sql_expr(sql).unwrap(),
                Expr::Literal(Value::Integer(i64::MIN))
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        parse_sql_expr("9223372036854775807").unwrap(),
        Expr::Literal(Value::Integer(i64::MAX))
    ));
    assert!(matches!(
        parse_sql_expr("9223372036854775808").unwrap(),
        Expr::Literal(Value::Real(_))
    ));
    for sql in ["-9223372036854775809", "-9223372036854775808.0"] {
        assert!(
            matches!(parse_sql_expr(sql).unwrap(), Expr::UnaryOp { expr, .. } if matches!(*expr, Expr::Literal(Value::Real(_)))),
            "{sql}"
        );
    }
}

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
        Statement::Explain { inner, analyze } => {
            assert!(matches!(*inner, Statement::Select(_)));
            assert!(!analyze, "plain EXPLAIN does not analyze");
        }
        _ => panic!("expected Explain"),
    }
}

#[test]
fn parse_explain_insert() {
    let stmt = parse_sql("EXPLAIN INSERT INTO t (a) VALUES (1)").unwrap();
    assert!(matches!(stmt, Statement::Explain { .. }));
}

/// ANALYZE used to be refused outright; now it parses and carries the flag.
#[test]
fn parse_explain_analyze_carries_the_flag() {
    let stmt = parse_sql("EXPLAIN ANALYZE SELECT * FROM users").unwrap();
    match stmt {
        Statement::Explain { inner, analyze } => {
            assert!(matches!(*inner, Statement::Select(_)));
            assert!(analyze, "ANALYZE must be recorded, not dropped");
        }
        _ => panic!("expected Explain"),
    }
}

/// EXPLAIN ANALYZE used to be rejected outright. The parser now accepts it;
/// `parse_explain_analyze_carries_the_flag` above checks
/// that the flag survives, and the executor tests check that it measures.
#[test]
fn accept_explain_analyze() {
    assert!(parse_sql("EXPLAIN ANALYZE SELECT * FROM t").is_ok());
}

#[test]
fn parse_parenthesized_explain_analyze_options() {
    let enabled = parse_sql("EXPLAIN (ANALYZE TRUE) SELECT * FROM users").unwrap();
    assert!(matches!(enabled, Statement::Explain { analyze: true, .. }));

    let disabled = parse_sql("EXPLAIN (ANALYZE FALSE) SELECT * FROM users").unwrap();
    assert!(matches!(
        disabled,
        Statement::Explain { analyze: false, .. }
    ));
}

#[test]
fn reject_explain_options_that_are_not_implemented() {
    for sql in [
        "EXPLAIN (VERBOSE TRUE) SELECT * FROM users",
        "EXPLAIN (FORMAT JSON) SELECT * FROM users",
        "EXPLAIN FORMAT JSON SELECT * FROM users",
    ] {
        assert!(parse_sql(sql).is_err(), "silently accepted {sql}");
    }
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
fn count_params_insert_conflict_assignments() {
    for (sql, expected) in [
        (
            "INSERT INTO t VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = $3",
            3,
        ),
        (
            "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET v = COALESCE($4, v) + $2",
            4,
        ),
        (
            "INSERT INTO t VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            2,
        ),
    ] {
        let stmt = parse_sql(sql).unwrap();
        assert_eq!(count_params(&stmt), expected, "{sql}");
    }
}

#[test]
fn count_params_insert_conflict_where() {
    let stmt = parse_sql(
        "INSERT INTO t VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE v < ($3 + 1)",
    )
    .unwrap();
    assert_eq!(count_params(&stmt), 3);
}

#[test]
fn count_params_insert_conflict_nested_subqueries() {
    for (assignment, predicate, expected) in [
        (
            "COALESCE($3, (SELECT $6))",
            "EXISTS (SELECT 1 FROM gate WHERE mark = $4)",
            6,
        ),
        (
            "COALESCE($3, (SELECT $4))",
            "EXISTS (SELECT 1 FROM gate WHERE mark = $7)",
            7,
        ),
    ] {
        // Parameter traversal covers the public AST even when execution does
        // not support a particular expression in ON CONFLICT.
        let mut stmt =
            parse_sql("INSERT INTO t VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = $3")
                .unwrap();
        let Statement::Insert(insert) = &mut stmt else {
            panic!("expected INSERT")
        };
        insert.on_conflict.as_mut().unwrap().action = OnConflictAction::DoUpdate {
            assignments: vec![("v".into(), parse_sql_expr(assignment).unwrap())],
            where_clause: Some(parse_sql_expr(predicate).unwrap()),
        };
        assert_eq!(count_params(&stmt), expected, "{assignment}; {predicate}");
    }
}

#[test]
fn count_params_insert_conflict_select_source() {
    let stmt = parse_sql(
        "INSERT INTO t (id, v) SELECT id, v FROM source WHERE id = $1 \
         ON CONFLICT (id) DO UPDATE SET v = $2 WHERE v < $3",
    )
    .unwrap();
    assert_eq!(count_params(&stmt), 3);
}

#[test]
fn count_params_insert_conflict_explain_wrapper() {
    let stmt = parse_sql(
        "EXPLAIN INSERT INTO t VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET v = ($3 + 1) WHERE v < $4",
    )
    .unwrap();
    assert_eq!(count_params(&stmt), 4);
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
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("CASCADE")));
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
fn create_index_predicate_rejects_clock_timestamp() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE ts > clock_timestamp()").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("non-deterministic")));
}

#[test]
fn create_index_predicate_rejects_one_arg_age() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE age(ts) > 100").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("non-deterministic")));
}

#[test]
fn create_index_predicate_accepts_two_arg_age() {
    assert!(parse_sql("CREATE INDEX i ON t (c) WHERE age(a, b) > 100").is_ok());
}

#[test]
fn create_index_predicate_rejects_date_now_literal() {
    let err = parse_sql("CREATE INDEX i ON t (c) WHERE ts > date('now')").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("non-deterministic")));
}

#[test]
fn create_index_predicate_accepts_date_of_column() {
    assert!(parse_sql("CREATE INDEX i ON t (c) WHERE date(ts) > '2020-01-01'").is_ok());
}

#[test]
fn generated_rejects_date_now_literal() {
    let err = parse_sql(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT GENERATED ALWAYS AS (date('now')) STORED)",
    )
    .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("volatile")));
}

#[test]
fn generated_rejects_one_arg_age() {
    let err = parse_sql(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP, \
         g INTERVAL GENERATED ALWAYS AS (age(ts)) STORED)",
    )
    .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(msg) if msg.contains("volatile")));
}

#[test]
fn generated_accepts_date_of_column() {
    assert!(parse_sql(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP, \
         g DATE GENERATED ALWAYS AS (date(ts)) STORED)",
    )
    .is_ok());
}

#[test]
fn generated_rejects_session_dependent_jsonpath() {
    let err = parse_sql(
        r#"CREATE TABLE t (
            id INTEGER PRIMARY KEY,
            j JSONB,
            g BOOLEAN GENERATED ALWAYS AS (
                JSONB_PATH_MATCH_TZ(
                    j,
                    '$.timestamp() == "2023-08-15T04:00:00+00:00".timestamp_tz()'
                )
            ) STORED
        )"#,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        SqlError::Unsupported(msg)
            if msg.contains("session-time-zone-dependent JSON path")
                && msg.contains("GENERATED expression")
    ));
}

#[test]
fn expression_index_rejects_volatile_and_session_dependent_keys() {
    let volatile = parse_sql("CREATE INDEX i ON t (RANDOM())").unwrap_err();
    assert!(matches!(
        volatile,
        SqlError::Unsupported(msg)
            if msg.contains("volatile function") && msg.contains("index expression")
    ));

    for sql in [
        r#"CREATE INDEX i ON t (
            (j @@ '$.time_tz().string() == "17:04:56+10:00"')
        )"#,
        r#"CREATE INDEX i ON t (
            JSONB_PATH_QUERY_FIRST_TZ(j, '$.time()') COLLATE BINARY
        )"#,
    ] {
        let err = parse_sql(sql).unwrap_err();
        assert!(matches!(
            err,
            SqlError::Unsupported(msg)
                if msg.contains("session-time-zone-dependent JSON path")
                    && msg.contains("index expression")
        ));
    }
}

#[test]
fn partial_index_rejects_standard_and_tz_jsonpath_evaluation() {
    for sql in [
        r#"CREATE INDEX i ON t (id) WHERE
            JSONB_PATH_MATCH(
                j,
                '$.time_tz().string() == "17:04:56+10:00"'
            )"#,
        r#"CREATE INDEX i ON t (id) WHERE
            j @@_tz '$.timestamp() == "2023-08-15T04:00:00+00:00".timestamp_tz()'"#,
    ] {
        let err = parse_sql(sql).unwrap_err();
        assert!(matches!(
            err,
            SqlError::Unsupported(msg)
                if msg.contains("session-time-zone-dependent JSON path")
                    && msg.contains("partial index predicate")
        ));
    }
}

#[test]
fn immutable_expression_and_json_predicate_remain_indexable() {
    assert!(parse_sql("CREATE INDEX i_lower ON t (LOWER(email))").is_ok());
    assert!(parse_sql(r#"CREATE INDEX i_json ON t (id) WHERE j @> '{"active": true}'"#).is_ok());
    assert!(
        parse_sql("CREATE INDEX i_path_key ON t ((JSONB_PATH_QUERY_FIRST(j, '$.profile')))")
            .is_ok()
    );
    assert!(
        parse_sql("CREATE INDEX i_path_pred ON t (id) WHERE JSONB_PATH_EXISTS(j, '$.active')")
            .is_ok()
    );
    assert!(parse_sql(
        "CREATE INDEX i_path_compare ON t (id) WHERE \
         JSONB_PATH_MATCH(j, '$.priority == 1')"
    )
    .is_ok());
    assert!(parse_sql(
        "CREATE INDEX i_path_operator_compare ON t (id) WHERE \
         j @@ '$.priority == 1'"
    )
    .is_ok());
    assert!(
        parse_sql("CREATE INDEX i_path_date ON t ((JSONB_PATH_QUERY_FIRST(j, '$.date()')))")
            .is_ok()
    );
    assert!(parse_sql(
        "CREATE INDEX i_path_date_compare ON t (id) WHERE \
         JSONB_PATH_MATCH(j, '$.a.date() < $.b.date()')"
    )
    .is_ok());
    assert!(parse_sql(
        "CREATE INDEX i_variable_path ON t (id) WHERE \
         JSONB_PATH_MATCH(j, '$.priority == $minimum', '{\"minimum\":1}'::JSONB)"
    )
    .is_ok());
    assert!(parse_sql(
        "CREATE TABLE path_generated (id INTEGER PRIMARY KEY, j JSONB, \
         g BOOLEAN GENERATED ALWAYS AS (JSONB_PATH_EXISTS(j, '$.active')) STORED)"
    )
    .is_ok());
}

#[test]
fn dynamic_jsonpath_is_not_accepted_as_immutable() {
    let error =
        parse_sql("CREATE INDEX i_dynamic_path ON t ((JSONB_PATH_QUERY_FIRST(j, path_column)))")
            .unwrap_err();
    assert!(matches!(
        error,
        SqlError::Unsupported(message)
            if message.contains("session-time-zone-dependent JSON path")
                && message.contains("index expression")
    ));
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

#[test]
fn parse_create_matview_with_no_data_sets_flag_false() {
    let stmt = parse_sql("CREATE MATERIALIZED VIEW v AS SELECT 1 AS x WITH NO DATA").unwrap();
    match stmt {
        Statement::CreateMaterializedView(mv) => {
            assert_eq!(mv.name, "v");
            assert!(!mv.with_data);
        }
        _ => panic!("expected CreateMaterializedView"),
    }
}

#[test]
fn parse_create_matview_without_clause_sets_flag_true() {
    let stmt = parse_sql("CREATE MATERIALIZED VIEW v AS SELECT 1 AS x").unwrap();
    match stmt {
        Statement::CreateMaterializedView(mv) => {
            assert!(mv.with_data);
        }
        _ => panic!("expected CreateMaterializedView"),
    }
}

#[test]
fn parse_sql_multi_two_matviews_one_with_no_data() {
    let sql = "CREATE MATERIALIZED VIEW a AS SELECT 1 AS x WITH NO DATA; \
               CREATE MATERIALIZED VIEW b AS SELECT 2 AS y;";
    let stmts = parse_sql_multi(sql).unwrap();
    assert_eq!(stmts.len(), 2);
    match &stmts[0] {
        Statement::CreateMaterializedView(mv) => assert!(!mv.with_data, "first should be NoData"),
        _ => panic!("expected CreateMaterializedView for first stmt"),
    }
    match &stmts[1] {
        Statement::CreateMaterializedView(mv) => {
            assert!(mv.with_data, "second should be populated")
        }
        _ => panic!("expected CreateMaterializedView for second stmt"),
    }
}

#[test]
fn parse_create_matview_with_no_data_case_insensitive() {
    for variant in [
        "with no data",
        "WITH NO DATA",
        "With No Data",
        "WiTh   nO\tdAtA",
    ] {
        let sql = format!("CREATE MATERIALIZED VIEW v AS SELECT 1 AS x {variant}");
        let stmt = parse_sql(&sql).unwrap();
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert!(!mv.with_data, "variant {variant:?} should strip");
            }
            _ => panic!("expected CreateMaterializedView for variant {variant:?}"),
        }
    }
}

#[test]
fn parse_create_matview_with_no_data_inside_string_literal_unchanged() {
    let stmt = parse_sql("CREATE MATERIALIZED VIEW v AS SELECT 'WITH NO DATA' AS x").unwrap();
    match stmt {
        Statement::CreateMaterializedView(mv) => {
            assert!(mv.with_data, "literal must not be stripped");
            assert!(
                mv.select_sql.contains("WITH NO DATA"),
                "literal preserved in stored sql"
            );
        }
        _ => panic!("expected CreateMaterializedView"),
    }
}

#[test]
fn parse_create_matview_trailing_semicolon_with_no_data() {
    let stmt = parse_sql("CREATE MATERIALIZED VIEW v AS SELECT 1 AS x WITH NO DATA;").unwrap();
    match stmt {
        Statement::CreateMaterializedView(mv) => assert!(!mv.with_data),
        _ => panic!("expected CreateMaterializedView"),
    }
}

#[test]
fn parse_create_matview_or_replace_with_no_data() {
    let stmt = parse_sql("CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT 1 AS x WITH NO DATA");
    if let Ok(Statement::CreateMaterializedView(mv)) = stmt {
        assert!(!mv.with_data);
    }
}

#[test]
fn split_spans_empty_string() {
    assert!(split_statement_spans("").is_empty());
}

#[test]
fn split_spans_whitespace_only_is_empty() {
    assert!(split_statement_spans("   \t\n\r  ").is_empty());
}

#[test]
fn split_spans_single_statement_no_semicolon() {
    let s = "SELECT 1";
    let spans = split_statement_spans(s);
    assert_eq!(spans, vec![(0, s.len())]);
}

#[test]
fn split_spans_single_statement_with_trailing_semicolon() {
    let s = "SELECT 1;";
    let spans = split_statement_spans(s);
    assert_eq!(spans, vec![(0, 8)]);
    assert_eq!(&s[spans[0].0..spans[0].1], "SELECT 1");
}

#[test]
fn split_spans_two_statements() {
    let s = "SELECT 1;SELECT 2";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert_eq!(&s[spans[0].0..spans[0].1], "SELECT 1");
    assert_eq!(&s[spans[1].0..spans[1].1], "SELECT 2");
}

#[test]
fn split_spans_three_statements_with_whitespace() {
    let s = "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t;";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 3);
    assert!(s[spans[0].0..spans[0].1].contains("CREATE"));
    assert!(s[spans[1].0..spans[1].1].contains("INSERT"));
    assert!(s[spans[2].0..spans[2].1].contains("SELECT"));
}

#[test]
fn split_spans_consecutive_semicolons_skip_empty() {
    let s = "SELECT 1;;SELECT 2;";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("SELECT 1"));
    assert!(s[spans[1].0..spans[1].1].contains("SELECT 2"));
}

#[test]
fn split_spans_semicolon_inside_single_quoted_string_kept_in_statement() {
    let s = "INSERT INTO t VALUES ('a;b'); SELECT * FROM t";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("'a;b'"));
}

#[test]
fn split_spans_escaped_single_quote_inside_string() {
    let s = "INSERT INTO t VALUES ('a''b;c'); SELECT 1";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("'a''b;c'"));
}

#[test]
fn split_spans_semicolon_inside_double_quoted_identifier() {
    let s = "SELECT \"col;name\" FROM t; SELECT 2";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("\"col;name\""));
}

#[test]
fn split_spans_semicolon_inside_line_comment() {
    let s = "SELECT 1 -- comment with ; in it\n; SELECT 2";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("-- comment with ; in it"));
}

#[test]
fn split_spans_semicolon_inside_block_comment() {
    let s = "SELECT 1 /* block ; with ; semis */; SELECT 2";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("/* block ; with ; semis */"));
}

#[test]
fn split_spans_semicolon_inside_dollar_quoted_string() {
    let s = "SELECT $tag$body;with;semis$tag$ AS s; SELECT 2";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("$tag$body;with;semis$tag$"));
}

#[test]
fn split_spans_anonymous_dollar_quote() {
    let s = "SELECT $$has;semi$$ AS s; SELECT 2";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("$$has;semi$$"));
}

#[test]
fn split_spans_dollar_sign_not_a_quote_passes_through() {
    let s = "SELECT $1 + 2; SELECT $1";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("$1 + 2"));
}

#[test]
fn split_spans_multiline_statement() {
    let s = "CREATE TABLE t (\n  id INTEGER PRIMARY KEY,\n  name TEXT\n); SELECT 1";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("CREATE TABLE t"));
    assert!(s[spans[0].0..spans[0].1].contains("name TEXT"));
}

#[test]
fn split_spans_leading_whitespace_preserved_in_first_span() {
    let s = "   SELECT 1";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0], (0, s.len()));
}

#[test]
fn split_spans_utf8_payload_byte_offsets_correct() {
    let s = "INSERT INTO t VALUES ('café'); SELECT 1";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 2);
    assert!(s[spans[0].0..spans[0].1].contains("café"));
    assert!(s[spans[1].0..spans[1].1].contains("SELECT 1"));
}

#[test]
fn split_spans_unterminated_string_keeps_rest_in_one_span() {
    let s = "SELECT 'oops; never closed";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 1);
    assert_eq!(&s[spans[0].0..spans[0].1], s);
}

#[test]
fn split_spans_refresh_in_middle_of_script() {
    let s =
        "CREATE MATERIALIZED VIEW mv AS SELECT 1; REFRESH MATERIALIZED VIEW mv; SELECT * FROM mv";
    let spans = split_statement_spans(s);
    assert_eq!(spans.len(), 3);
    assert!(s[spans[0].0..spans[0].1].contains("CREATE MATERIALIZED VIEW"));
    assert!(s[spans[1].0..spans[1].1].trim().starts_with("REFRESH"));
    assert!(s[spans[2].0..spans[2].1].contains("SELECT * FROM mv"));
}

#[test]
fn has_subquery_follows_collation_and_window_expression_wrappers() {
    for sql in [
        "(SELECT 1) COLLATE BINARY",
        "COALESCE(((SELECT 1) COLLATE BINARY), 0)",
        "FIRST_VALUE((SELECT 1)) OVER ()",
        "SUM(1) OVER (PARTITION BY (SELECT 1))",
        "SUM(1) OVER (ORDER BY (SELECT 1))",
        "1 = ANY (SELECT 1)",
        "ARRAY[(SELECT 1)]",
        "CAST((SELECT 1) AS INTEGER)",
    ] {
        let expression = parse_sql_expr(sql).unwrap();
        assert!(has_subquery(&expression), "{sql}");
    }
    for sql in [
        "'plain' COLLATE BINARY",
        "COALESCE($1, 0)",
        "FIRST_VALUE(v) OVER (ORDER BY id)",
        "SUM(v) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "1 = ANY (ARRAY[1, 2])",
        "ARRAY[1, 2]",
        "CAST(1 AS INTEGER)",
    ] {
        let expression = parse_sql_expr(sql).unwrap();
        assert!(!has_subquery(&expression), "{sql}");
    }
}

#[test]
fn has_subquery_follows_both_window_frame_bounds() {
    let subquery = parse_sql_expr("(SELECT 1)").unwrap();
    for use_start in [false, true] {
        let mut expression =
            parse_sql_expr("SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)")
                .unwrap();
        let Expr::WindowFunction { spec, .. } = &mut expression else {
            unreachable!()
        };
        let frame = spec.frame.as_mut().unwrap();
        if use_start {
            frame.start = WindowFrameBound::Preceding(Box::new(subquery.clone()));
        } else {
            frame.end = WindowFrameBound::Following(Box::new(subquery.clone()));
        }
        assert!(has_subquery(&expression), "start bound: {use_start}");
    }
}

#[test]
fn parameter_visitor_covers_query_inputs_and_returning() {
    for (sql, expected) in [
        ("SELECT d.v FROM (SELECT $4 AS v) d", 4),
        (
            "SELECT d.v FROM (WITH c AS (SELECT $7 AS v) SELECT v FROM c) d",
            7,
        ),
        ("SELECT value FROM json_array_elements($5)", 5),
        (
            "SELECT * FROM JSON_TABLE($6, '$[*]' COLUMNS (v INT PATH '$'))",
            6,
        ),
        ("SELECT 1 FROM t JOIN (SELECT $8 AS v) d ON TRUE", 8),
        ("SELECT 1 FROM t JOIN json_array_elements($9) j ON TRUE", 9),
        ("INSERT INTO t VALUES (1) RETURNING $10", 10),
        ("UPDATE t SET v=1 RETURNING $11", 11),
        ("DELETE FROM t RETURNING $12", 12),
    ] {
        assert_eq!(count_params(&parse_sql(sql).unwrap()), expected, "{sql}");
    }
}

fn upstream_statement(sql: &str) -> sp::Statement {
    upstream_statement_in(sql, &sqlparser::dialect::GenericDialect {})
}

fn upstream_statement_in(sql: &str, dialect: &dyn sqlparser::dialect::Dialect) -> sp::Statement {
    sqlparser::parser::Parser::parse_sql(dialect, sql)
        .unwrap_or_else(|error| panic!("upstream parser rejected {sql}: {error}"))
        .remove(0)
}

#[test]
fn converter_rejects_unsupported_dml_fields() {
    for sql in [
        "UPDATE q SET n = 1 FROM other",
        "UPDATE q SET n = 1 LIMIT 1",
        "UPDATE OR IGNORE q SET n = 1",
        "UPDATE q JOIN other ON q.id = other.id SET n = 1",
        "DELETE FROM q LIMIT 1",
        "DELETE FROM q ORDER BY id",
        "DELETE FROM q USING other",
        "DELETE FROM q JOIN other ON q.id = other.id",
        "INSERT OR IGNORE INTO q VALUES (1)",
        "INSERT OR REPLACE INTO q VALUES (1)",
        "INSERT IGNORE INTO q VALUES (1)",
        "REPLACE INTO q VALUES (1)",
        "INSERT OVERWRITE TABLE q VALUES (1)",
        "INSERT INTO q VALUES (1), (2) LIMIT 1",
        "INSERT INTO q VALUES (1), (2) ORDER BY 1",
        "INSERT INTO q VALUES (1) RETURNING * EXCLUDE (id)",
    ] {
        let result = convert_statement(upstream_statement(sql));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn converter_rejects_dialect_specific_semantic_modifiers() {
    for (sql, dialect) in [
        (
            "DELETE q FROM q JOIN other ON q.id = other.id",
            &sqlparser::dialect::MySqlDialect {} as &dyn sqlparser::dialect::Dialect,
        ),
        (
            "WITH t AS MATERIALIZED (SELECT 1) SELECT * FROM t",
            &sqlparser::dialect::PostgreSqlDialect {} as &dyn sqlparser::dialect::Dialect,
        ),
    ] {
        let result = convert_statement(upstream_statement_in(sql, dialect));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn converter_rejects_unsupported_query_and_source_fields() {
    for sql in [
        "SELECT TOP 1 id FROM q",
        "SELECT id FROM q FETCH FIRST 1 ROWS ONLY",
        "SELECT id FROM q QUALIFY id = 1",
        "SELECT id INTO backup FROM q",
        "SELECT id FROM q FOR UPDATE",
        "SELECT id FROM q PREWHERE id = 1",
        "SELECT id FROM q GROUP BY id WITH ROLLUP",
        "SELECT id FROM q WINDOW w AS (ORDER BY id)",
        "SELECT * EXCLUDE (id) FROM q",
        "SELECT * REPLACE (1 AS id) FROM q",
        "SELECT * FROM q AS renamed (id)",
        "SELECT * FROM (SELECT id FROM q) AS renamed (other_id)",
        "SELECT * FROM q TABLESAMPLE (10)",
        "SELECT * FROM q LEFT SEMI JOIN other ON q.id = other.id",
        "SELECT * FROM q LEFT ANTI JOIN other ON q.id = other.id",
        "SELECT * FROM q RIGHT SEMI JOIN other ON q.id = other.id",
        "SELECT * FROM q RIGHT ANTI JOIN other ON q.id = other.id",
        "SELECT * FROM q GLOBAL JOIN other ON q.id = other.id",
        "SELECT id FROM q LIMIT 1 BY id",
        "SELECT id FROM q ORDER BY id WITH FILL",
        "WITH t AS (WITH u AS (SELECT 1) SELECT * FROM u) SELECT * FROM t",
    ] {
        let result = convert_statement(upstream_statement(sql));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn converter_rejects_unenforced_unique_null_semantics() {
    for sql in [
        "CREATE UNIQUE INDEX uq ON q (id) NULLS NOT DISTINCT",
        "CREATE TABLE q (id INT, UNIQUE NULLS NOT DISTINCT (id))",
    ] {
        let result = convert_statement(upstream_statement(sql));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
    for sql in [
        "CREATE UNIQUE INDEX uq ON q (id) NULLS DISTINCT",
        "CREATE TABLE q (id INT, UNIQUE NULLS DISTINCT (id))",
    ] {
        assert!(convert_statement(upstream_statement(sql)).is_ok(), "{sql}");
    }
}

#[test]
fn converter_checks_column_unique_null_option_from_ast() {
    // The upstream AST can represent this option on a column even though its
    // parser currently accepts the syntax only on table constraints.
    for nulls_distinct in [
        sp::NullsDistinctOption::NotDistinct,
        sp::NullsDistinctOption::Distinct,
    ] {
        let sp::Statement::CreateTable(mut table) =
            upstream_statement("CREATE TABLE q (id INT UNIQUE)")
        else {
            panic!("expected CREATE TABLE");
        };
        let sp::ColumnOption::Unique(unique) = &mut table.columns[0].options[0].option else {
            panic!("expected UNIQUE");
        };
        unique.nulls_distinct = nulls_distinct;
        let result = convert_create_table(table);
        if nulls_distinct == sp::NullsDistinctOption::NotDistinct {
            assert!(matches!(result, Err(SqlError::Unsupported(_))));
        } else {
            assert!(result.is_ok());
        }
    }
}

#[test]
fn converter_checks_query_options_on_values_and_nested_statements() {
    let sp::Statement::Query(mut query) = upstream_statement("SELECT 1 FETCH FIRST 1 ROWS ONLY")
    else {
        panic!("expected query");
    };
    let sp::Statement::Insert(mut insert) = upstream_statement("INSERT INTO q VALUES (1)") else {
        panic!("expected insert");
    };
    insert.source.as_mut().unwrap().fetch = query.fetch.take();
    assert!(matches!(
        convert_insert(insert),
        Err(SqlError::Unsupported(_))
    ));

    let sp::Statement::Query(mut query) = upstream_statement("SELECT 1") else {
        panic!("expected query");
    };
    let sp::SetExpr::Select(select) = query.body.as_mut() else {
        panic!("expected SELECT");
    };
    select.qualify = Some(sp::Expr::Value(sp::Value::Boolean(false).into()));
    assert!(matches!(
        convert_query(*query),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn converter_rejects_implicit_projection_from_ast() {
    let sp::Statement::Query(mut query) = upstream_statement("SELECT * FROM q") else {
        panic!("expected query");
    };
    let sp::SetExpr::Select(select) = query.body.as_mut() else {
        panic!("expected SELECT");
    };
    select.projection.clear();
    select.flavor = sp::SelectFlavor::FromFirstNoSelect;
    assert!(matches!(
        convert_query(*query),
        Err(SqlError::Unsupported(_))
    ));
}

fn upstream_function(sql: &str) -> sp::Function {
    let sp::Statement::Query(query) = upstream_statement(&format!("SELECT {sql}")) else {
        panic!("expected query");
    };
    let sp::SetExpr::Select(mut select) = *query.body else {
        panic!("expected SELECT");
    };
    let sp::SelectItem::UnnamedExpr(sp::Expr::Function(function)) = select.projection.remove(0)
    else {
        panic!("expected function: {sql}");
    };
    function
}

#[test]
fn converter_rejects_unsupported_function_modifiers() {
    for sql in [
        "COUNT(*) FILTER (WHERE false)",
        "SUM(id) FILTER (WHERE id > 0)",
        "FIRST_VALUE(id) IGNORE NULLS OVER (ORDER BY id)",
        "SUM(id) RESPECT NULLS OVER (ORDER BY id)",
        "NTH_VALUE(id, 1) RESPECT NULLS OVER (ORDER BY id)",
        "SUM(id) WITHIN GROUP (ORDER BY id DESC)",
        "SUM(0.5)(id)",
        "SUM(id ORDER BY id DESC)",
        "SUM(id LIMIT 1)",
        "FIRST_VALUE(id IGNORE NULLS) OVER (ORDER BY id)",
        "SUM(id HAVING MAX id)",
        "GROUP_CONCAT(id SEPARATOR ';')",
        "SUM(id ON OVERFLOW ERROR)",
        "JSON_AGG(id ABSENT ON NULL)",
        "JSON_AGG(id RETURNING TEXT)",
    ] {
        let result = convert_function(&upstream_function(sql));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn converter_checks_function_only_ast_options() {
    let mut function = upstream_function("ABS(-1)");
    function.uses_odbc_syntax = true;
    let Expr::Function {
        name,
        args,
        distinct,
    } = convert_function(&function).unwrap()
    else {
        panic!("expected function");
    };
    assert_eq!(name, "ABS");
    assert!(!distinct);
    assert!(
        matches!(args.as_slice(), [Expr::UnaryOp { op: UnaryOp::Neg, expr }]
        if matches!(**expr, Expr::Literal(Value::Integer(1))))
    );

    let mut function = upstream_function("SUM(id)");
    function.parameters = sp::FunctionArguments::List(sp::FunctionArgumentList {
        duplicate_treatment: None,
        args: vec![],
        clauses: vec![],
    });
    assert!(matches!(
        convert_function(&function),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn converter_rejects_distinct_when_result_cannot_represent_it() {
    for sql in [
        "SUM(DISTINCT id) OVER ()",
        "COUNT(DISTINCT id) OVER (ORDER BY id)",
        "COUNT(DISTINCT *)",
        "COALESCE(DISTINCT id, 0)",
        "NULLIF(DISTINCT id, 0)",
        "IIF(DISTINCT id > 0, 1, 0)",
        "COUNT(*, id)",
        "COUNT(id, *)",
    ] {
        let result = convert_function(&upstream_function(sql));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn converter_rejects_named_base_windows() {
    for sql in [
        "SUM(id) OVER base_window",
        "SUM(id) OVER (base_window)",
        "SUM(id) OVER (base_window ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
    ] {
        let result = convert_function(&upstream_function(sql));
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn supported_aggregate_and_window_options_remain_represented() {
    for name in ["COUNT", "SUM", "AVG", "MIN", "MAX", "JSON_AGG"] {
        let expr = parse_sql_expr(&format!("{name}(DISTINCT id)")).unwrap();
        assert!(
            matches!(expr, Expr::Function { distinct: true, .. }),
            "{name}: {expr:?}"
        );
        let expr = parse_sql_expr(&format!("{name}(ALL id)")).unwrap();
        assert!(
            matches!(
                expr,
                Expr::Function {
                    distinct: false,
                    ..
                }
            ),
            "{name}: {expr:?}"
        );
    }
    for sql in ["COUNT(*)", "COUNT(ALL *)", "COUNT()"] {
        assert!(
            matches!(parse_sql_expr(sql).unwrap(), Expr::CountStar),
            "{sql}"
        );
    }
    let Expr::WindowFunction { name, args, spec } = parse_sql_expr(
        "SUM(ALL id) OVER (PARTITION BY bucket ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
    ).unwrap() else { panic!("expected window function"); };
    assert_eq!(name, "SUM");
    assert_eq!(args.len(), 1);
    assert_eq!(spec.partition_by.len(), 1);
    assert!(spec.order_by[0].descending);
    assert!(matches!(
        spec.frame,
        Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowFrameBound::Preceding(_),
            end: WindowFrameBound::CurrentRow
        })
    ));
    let Expr::WindowFunction { name, args, .. } = parse_sql_expr("COUNT(*) OVER ()").unwrap()
    else {
        panic!("expected window function");
    };
    assert_eq!(name, "COUNT");
    assert!(args.is_empty());
}

#[test]
fn explicit_respect_nulls_matches_supported_value_windows() {
    for name in ["FIRST_VALUE", "LAST_VALUE", "LAG", "LEAD"] {
        for sql in [
            format!("{name}(id) OVER (ORDER BY id)"),
            format!("{name}(id) RESPECT NULLS OVER (ORDER BY id)"),
            format!("{name}(id RESPECT NULLS) OVER (ORDER BY id)"),
        ] {
            let Expr::WindowFunction {
                name: converted_name,
                args,
                spec,
            } = convert_function(&upstream_function(&sql)).unwrap()
            else {
                panic!("expected window function: {sql}");
            };
            assert_eq!(converted_name, name);
            assert!(matches!(args.as_slice(), [Expr::Column(column)] if column == "id"));
            assert!(spec.partition_by.is_empty());
            assert!(spec.frame.is_none());
            assert_eq!(spec.order_by.len(), 1);
            assert!(matches!(&spec.order_by[0].expr, Expr::Column(column) if column == "id"));
            assert!(!spec.order_by[0].descending);
        }
    }
}
