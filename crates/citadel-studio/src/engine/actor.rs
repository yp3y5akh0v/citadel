//! Session-owning actor. `Connection` is `!Send`, so only owned data crosses the channel.

use std::sync::mpsc::{Receiver, Sender};

use citadel_sql::TableSchema;

use super::error::{Kind, StudioError};
use super::session::{Run, Session};
use super::{Answer, ColumnFacts, Command, Mutation, Opened, Reply, TableFacts, VectorFacts, Work};

pub fn run(commands: &Receiver<Work>, replies: &Sender<Answer>, cancel: &super::CancelState) {
    let mut session: Option<Session> = None;
    while let Ok(work) = commands.recv() {
        // Activate the token registered before this command entered the channel.
        let token = cancel.lock().expect("cancel state").activate(work.id);
        if let Some(open) = session.as_ref() {
            open.database().set_cancel(Some(token.clone()));
        }
        let reply = serve(&mut session, work.command, &token);
        cancel.lock().expect("cancel state").finish(work.id);
        if replies.send(Answer { id: work.id, reply }).is_err() {
            break;
        }
    }
    drop(session);
}

fn serve(session: &mut Option<Session>, command: Command, token: &citadel::CancelToken) -> Reply {
    match command {
        Command::Open { path, passphrase } => Reply::Opened(Box::new(replace_session(
            session,
            Session::open(&path, &passphrase),
            token,
        ))),
        Command::CheckIntegrity => Reply::Checked(Box::new(match session.as_ref() {
            Some(session) => session.integrity(),
            None => Err(no_vault("check integrity")),
        })),
        Command::ChangePassphrase { current, next } => {
            Reply::PassphraseChanged(Box::new(mutate_and_refresh(
                session,
                |open| open.change_passphrase(&current, &next),
                "change a passphrase",
            )))
        }
        Command::Create(spec) => Reply::Opened(Box::new(replace_session(
            session,
            Session::create(&spec),
            token,
        ))),
        Command::OpenDemo => {
            Reply::Opened(Box::new(replace_session(session, Session::demo(), token)))
        }
        Command::Close => {
            *session = None;
            Reply::Closed
        }
        Command::Run(sql) => match session.as_mut() {
            Some(session) => {
                let run = session.run(&sql);
                let refreshed = run
                    .storage_changed
                    .then(|| Box::new(describe_fresh(session)));
                Reply::Ran {
                    run: Box::new(run),
                    refreshed,
                }
            }
            None => Reply::Ran {
                run: Box::new(Run {
                    statements: Vec::new(),
                    failed: Some(no_vault("run a statement")),
                    storage_changed: false,
                }),
                refreshed: None,
            },
        },
        Command::Import(sql) => Reply::Imported(Box::new(mutate_and_refresh(
            session,
            |open| open.run_atomic(&sql),
            "import into",
        ))),
        Command::Explain { sql, analyze } => match session.as_mut() {
            Some(session) => {
                let before = session.database().manager().commit_generation();
                let plan = session.explain(&sql, analyze);
                let changed = session.database().manager().commit_generation() != before;
                let refreshed = changed.then(|| Box::new(describe_fresh(session)));
                Reply::Explained {
                    plan: Box::new(plan),
                    refreshed,
                }
            }
            None => Reply::Explained {
                plan: Box::new(Err(no_vault("explain a statement"))),
                refreshed: None,
            },
        },
        Command::Browse { table, limit } => {
            let rows = match session.as_ref() {
                Some(session) => session.query(&browse_sql(&table, limit)),
                None => Err(no_vault("browse a table")),
            };
            Reply::Browsed {
                table,
                rows: Box::new(rows),
            }
        }
        Command::SampleVector { table, column } => {
            let sample = match session.as_ref() {
                Some(session) => sample_vector(session, &table, &column),
                None => Err(no_vault("sample a vector column")),
            };
            Reply::VectorSampled {
                table,
                column,
                sample: Box::new(sample),
            }
        }
        Command::Atoms { region, after } => {
            let page = match session.as_mut() {
                Some(session) => super::memory::atoms(session, &region, after),
                None => Err(no_vault("read a region")),
            };
            Reply::Atoms {
                region,
                page: Box::new(page),
            }
        }
        Command::Verify { region, ids } => {
            let verdicts = match session.as_mut() {
                Some(session) => super::memory::verify(session, &region, &ids),
                None => Err(no_vault("verify visible rows")),
            };
            Reply::Verified {
                region,
                asked: ids,
                verdicts: Box::new(verdicts),
            }
        }
        Command::Forget { region, ids } => {
            let mutation = mutate_and_refresh(
                session,
                |open| super::memory::forget(open, &region, &ids),
                "forget an atom",
            );
            Reply::Forgotten {
                region,
                asked: ids,
                mutation: Box::new(mutation),
            }
        }
    }
}

/// Open a replacement before dropping the current session, preserving it on failure.
fn replace_session(
    slot: &mut Option<Session>,
    candidate: Result<Session, StudioError>,
    token: &citadel::CancelToken,
) -> Result<Opened, StudioError> {
    let mut candidate = candidate?;
    candidate.database().set_cancel(Some(token.clone()));
    let opened = describe_opened(&mut candidate)?;
    *slot = Some(candidate);
    Ok(opened)
}

fn no_vault(what: &str) -> StudioError {
    StudioError::new(
        Kind::Usage,
        format!("no vault is open, so nothing can {what}"),
    )
}

fn mutate_and_refresh<T>(
    session: &mut Option<Session>,
    mutate: impl FnOnce(&mut Session) -> Result<T, StudioError>,
    what: &str,
) -> Mutation<T> {
    let Some(open) = session.as_mut() else {
        return Mutation {
            outcome: Err(no_vault(what)),
            refreshed: Err(no_vault("refresh")),
        };
    };
    let outcome = mutate(open);
    let refreshed = describe_fresh(open);
    Mutation { outcome, refreshed }
}

/// Reconciliation must not inherit the token that interrupted the mutation it describes.
fn describe_fresh(session: &mut Session) -> Result<Opened, StudioError> {
    session.database().set_cancel(None);
    describe_opened(session)
}

/// Compose a bounded table query with a safely quoted identifier.
fn browse_sql(table: &str, limit: usize) -> String {
    format!(
        "SELECT * FROM \"{}\" LIMIT {limit}",
        table.replace('"', "\"\"")
    )
}

fn describe_opened(session: &mut Session) -> Result<Opened, StudioError> {
    // Only when the vault already has one. Listing regions opens the memory engine, which
    // writes its bootstrap schema, so asking a plain SQL vault what regions it has would
    // give it the tables to answer with.
    let regions = if session.has_memory() {
        super::memory::regions(session)?
    } else {
        Vec::new()
    };
    let tables = describe(session)?;
    let opened = Opened {
        label: session.label(),
        facts: session.facts()?,
        path: session.path().to_owned(),
        is_demo: session.is_demo(),
        vectors: vector_descriptors(&tables),
        tables,
        regions,
    };
    Ok(opened)
}

/// Maximum rows sampled for the two-dimension vector view.
const VECTOR_SAMPLE: usize = 20_000;
/// Bound temporary full-vector materialization before retaining only two dimensions.
const VECTOR_SAMPLE_BYTES: usize = 8 * 1024 * 1024;

/// Describe generic vector columns without reading their values while the vault opens.
fn vector_descriptors(tables: &[TableFacts]) -> Vec<VectorFacts> {
    tables
        .iter()
        // Internal vectors already have an attested memory-region view.
        .filter(|table| !table.engine_owned)
        .flat_map(|table| {
            table.columns.iter().filter_map(|column| {
                let dim = column.vector_dim.filter(|dim| *dim >= 2)?;
                Some(VectorFacts {
                    table: table.name.clone(),
                    column: column.name.clone(),
                    dim: u32::from(dim),
                    total: table.rows,
                    points: Vec::new(),
                })
            })
        })
        .collect()
}

fn sample_vector(session: &Session, table: &str, column: &str) -> Result<VectorFacts, StudioError> {
    let schema = session.table_schema(table).ok_or_else(|| {
        StudioError::new(
            Kind::Missing,
            format!("table '{table}' disappeared before its vector sample was read"),
        )
    })?;
    let column = columns(&schema)
        .into_iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| {
            StudioError::new(
                Kind::Missing,
                format!("column '{table}.{column}' disappeared before its vector sample was read"),
            )
        })?;
    let rows = count(session, table)?;
    sample_vectors(session, table, &column, rows)
}

/// Sample and normalize the first two stored dimensions without fitting a projection model.
fn sample_vectors(
    session: &Session,
    table: &str,
    column: &ColumnFacts,
    rows: u64,
) -> Result<VectorFacts, StudioError> {
    let Some(dim) = column.vector_dim else {
        return Err(StudioError::new(
            Kind::Usage,
            format!("{table}.{} is not a vector column", column.name),
        ));
    };
    // A one-dimensional vector has no second axis to plot against.
    if dim < 2 {
        return Err(StudioError::new(
            Kind::Usage,
            format!("{table}.{} has only one display dimension", column.name),
        ));
    }
    let sample_rows = vector_sample_rows(dim);
    let sql = format!(
        "SELECT \"{}\" FROM \"{}\" LIMIT {sample_rows}",
        column.name.replace('"', "\"\""),
        table.replace('"', "\"\"")
    );
    let raw = session.sample_vector_xy(&sql)?;
    for (offset, &(x, y)) in raw.iter().enumerate() {
        if !x.is_finite() || !y.is_finite() {
            return Err(StudioError::new(
                Kind::Data,
                format!(
                    "{table}.{} contains a non-finite display coordinate in sampled row {}",
                    column.name,
                    offset + 1
                ),
            ));
        }
    }
    Ok(VectorFacts {
        table: table.to_owned(),
        column: column.name.clone(),
        dim: u32::from(dim),
        total: rows,
        points: normalise(raw),
    })
}

fn vector_sample_rows(dim: u16) -> usize {
    let row_bytes = usize::from(dim) * std::mem::size_of::<f32>();
    VECTOR_SAMPLE.min(VECTOR_SAMPLE_BYTES / row_bytes).max(1)
}

/// Normalize into canvas space; a degenerate axis maps to the midpoint.
fn normalise(points: Vec<(f32, f32)>) -> Vec<(f32, f32)> {
    let (mut lo_x, mut hi_x) = (f32::INFINITY, f32::NEG_INFINITY);
    let (mut lo_y, mut hi_y) = (f32::INFINITY, f32::NEG_INFINITY);
    for &(x, y) in &points {
        lo_x = lo_x.min(x);
        hi_x = hi_x.max(x);
        lo_y = lo_y.min(y);
        hi_y = hi_y.max(y);
    }
    let span = |lo: f32, hi: f32| {
        let s = hi - lo;
        if s.is_finite() && s > f32::EPSILON {
            Some(s)
        } else {
            None
        }
    };
    let (sx, sy) = (span(lo_x, hi_x), span(lo_y, hi_y));
    points
        .into_iter()
        .map(|(x, y)| {
            (
                sx.map_or(0.5, |s| (x - lo_x) / s),
                sy.map_or(0.5, |s| (y - lo_y) / s),
            )
        })
        .collect()
}

fn describe(session: &Session) -> Result<Vec<TableFacts>, StudioError> {
    session
        .tables()
        .into_iter()
        .map(|name| {
            let schema = session.table_schema(&name).ok_or_else(|| {
                StudioError::new(
                    Kind::Missing,
                    format!("table '{name}' disappeared while its schema was being read"),
                )
            })?;
            Ok(TableFacts {
                rows: count(session, &name)?,
                columns: columns(&schema),
                engine_owned: citadel_mem::owns_table(&name),
                name,
            })
        })
        .collect()
}

fn count(session: &Session, table: &str) -> Result<u64, StudioError> {
    let sql = format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\""));
    let result = session.query(&sql)?;
    match result.rows.first().and_then(|row| row.first()) {
        Some(citadel_sql::Value::Integer(n)) if *n >= 0 => Ok(*n as u64),
        other => Err(StudioError::new(
            Kind::Data,
            format!("COUNT for table '{table}' returned {other:?}"),
        )),
    }
}

fn columns(schema: &TableSchema) -> Vec<ColumnFacts> {
    schema
        .columns
        .iter()
        .map(|column| ColumnFacts {
            name: column.name.clone(),
            data_type: column.data_type.to_string(),
            nullable: column.nullable,
            primary_key: schema.primary_key_columns.contains(&column.position),
            default_sql: column.default_sql.clone(),
            check_sql: column.check_sql.clone(),
            generated_sql: column.generated_sql.clone(),
            vector_dim: match column.data_type {
                citadel_sql::DataType::Vector { dim } => Some(dim),
                _ => None,
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_spec(path: std::path::PathBuf) -> super::super::CreateSpec {
        super::super::CreateSpec {
            path,
            passphrase: "a-long-enough-passphrase".into(),
            kdf: citadel::KdfAlgorithm::Argon2id,
            profile: citadel::Argon2Profile::Iot,
        }
    }

    #[test]
    fn a_browse_quotes_the_table_name() {
        assert_eq!(
            browse_sql("documents", 200),
            "SELECT * FROM \"documents\" LIMIT 200"
        );
        assert_eq!(
            browse_sql("say \"hi\"", 10),
            "SELECT * FROM \"say \"\"hi\"\"\" LIMIT 10"
        );
    }

    #[test]
    fn failed_open_and_create_leave_the_existing_session_installed() {
        let dir = tempfile::tempdir().unwrap();
        let first = scratch_spec(dir.path().join("first.cdl"));
        let second = scratch_spec(dir.path().join("second.cdl"));
        let current = Session::create(&first).unwrap();
        current
            .run_atomic("CREATE TABLE keep (id INTEGER PRIMARY KEY);")
            .unwrap();
        drop(Session::create(&second).unwrap());
        let mut slot = Some(current);
        let token = citadel::CancelToken::new();

        let Reply::Opened(opened) = serve(
            &mut slot,
            Command::Open {
                path: second.path.clone(),
                passphrase: "definitely-wrong".into(),
            },
            &token,
        ) else {
            panic!("Open should answer with Opened");
        };
        assert!(opened.is_err());
        assert_eq!(slot.as_ref().unwrap().path(), first.path);
        assert!(slot
            .as_ref()
            .unwrap()
            .tables()
            .iter()
            .any(|name| name == "keep"));

        let Reply::Opened(created) = serve(&mut slot, Command::Create(Box::new(second)), &token)
        else {
            panic!("Create should answer with Opened");
        };
        assert!(created.is_err());
        assert_eq!(slot.as_ref().unwrap().path(), first.path);
        assert!(slot
            .as_ref()
            .unwrap()
            .tables()
            .iter()
            .any(|name| name == "keep"));
    }

    #[test]
    fn cancellation_while_counting_is_reported_instead_of_becoming_zero_rows() {
        let session = super::super::session::DemoVault::new();
        let token = citadel::CancelToken::new();
        token.cancel();
        session.database().set_cancel(Some(token));

        let error = describe(&session).expect_err("a cancelled count must abort the description");
        assert_eq!(error.kind, Kind::Cancelled);
    }

    #[test]
    fn vector_sample_errors_are_not_silently_treated_as_no_points() {
        let session = super::super::session::DemoVault::new();
        let token = citadel::CancelToken::new();
        token.cancel();
        session.database().set_cancel(Some(token));
        let column = ColumnFacts {
            name: "embedding".to_owned(),
            data_type: "VECTOR(8)".to_owned(),
            nullable: false,
            primary_key: false,
            default_sql: None,
            check_sql: None,
            generated_sql: None,
            vector_dim: Some(8),
        };

        let error = sample_vectors(&session, "documents", &column, 1_500)
            .expect_err("a cancelled on-demand sample must stop");
        assert_eq!(error.kind, Kind::Cancelled);
    }

    #[test]
    fn a_bad_vector_sample_does_not_prevent_the_vault_from_opening() {
        let mut session = super::super::session::DemoVault::new();
        let write = session.run(
            "CREATE TABLE bad_vectors (id INTEGER PRIMARY KEY, v VECTOR(2));
             INSERT INTO bad_vectors VALUES (1, '[NaN, 1]'::VECTOR(2));",
        );
        assert!(
            write.failed.is_none(),
            "fixture setup failed: {:?}",
            write.failed
        );

        let opened =
            describe_opened(&mut session).expect("catalog discovery does not read vectors");
        let descriptor = opened
            .vectors
            .iter()
            .find(|vector| vector.table == "bad_vectors" && vector.column == "v")
            .expect("the invalid vector column is still discoverable");
        assert!(
            descriptor.points.is_empty(),
            "open returns metadata, not a sample"
        );

        let error = sample_vector(&session, "bad_vectors", "v")
            .expect_err("opening the vector document reports its own sample failure");
        assert_eq!(error.kind, Kind::Data);
    }

    #[test]
    fn high_dimensional_samples_have_a_fixed_materialization_bound() {
        assert_eq!(vector_sample_rows(2), VECTOR_SAMPLE);
        let rows = vector_sample_rows(u16::MAX);
        assert!(rows > 0);
        assert!(rows * usize::from(u16::MAX) * std::mem::size_of::<f32>() <= VECTOR_SAMPLE_BYTES);
    }

    #[test]
    fn vector_sample_refuses_non_finite_gpu_coordinates() {
        let session = super::super::session::DemoVault::new();
        let write = session.run(
            "CREATE TABLE bad_vectors (id INTEGER PRIMARY KEY, v VECTOR(2));
             INSERT INTO bad_vectors VALUES (1, '[NaN, 1]'::VECTOR(2));",
        );
        assert!(
            write.failed.is_none(),
            "fixture setup failed: {:?}",
            write.failed
        );
        let column = ColumnFacts {
            name: "v".to_owned(),
            data_type: "VECTOR(2)".to_owned(),
            nullable: true,
            primary_key: false,
            default_sql: None,
            check_sql: None,
            generated_sql: None,
            vector_dim: Some(2),
        };

        let error = sample_vectors(&session, "bad_vectors", &column, 1)
            .expect_err("NaN must not reach the visibility index or WGSL");
        assert_eq!(error.kind, Kind::Data);
        assert!(error.detail.contains("non-finite display coordinate"));
    }
}
