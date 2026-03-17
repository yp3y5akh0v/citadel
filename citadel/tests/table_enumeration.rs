use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"enum-test")
        .argon2_profile(Argon2Profile::Iot)
}

#[test]
fn list_tables_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();
    let tables = db.manager().list_tables().unwrap();
    assert!(tables.is_empty());
}

#[test]
fn list_tables_single() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"users").unwrap();
    wtx.table_insert(b"users", b"k1", b"v1").unwrap();
    wtx.commit().unwrap();

    let tables = db.manager().list_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].0, b"users");
    assert!(tables[0].1.root_page.is_valid());
}

#[test]
fn list_tables_multiple() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"alpha").unwrap();
    wtx.create_table(b"beta").unwrap();
    wtx.create_table(b"gamma").unwrap();
    wtx.table_insert(b"alpha", b"k", b"v").unwrap();
    wtx.table_insert(b"beta", b"k", b"v").unwrap();
    wtx.table_insert(b"gamma", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    let mut tables = db.manager().list_tables().unwrap();
    assert_eq!(tables.len(), 3);
    tables.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(tables[0].0, b"alpha");
    assert_eq!(tables[1].0, b"beta");
    assert_eq!(tables[2].0, b"gamma");
}

#[test]
fn list_tables_after_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"temp").unwrap();
    wtx.create_table(b"keep").unwrap();
    wtx.table_insert(b"temp", b"k", b"v").unwrap();
    wtx.table_insert(b"keep", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.drop_table(b"temp").unwrap();
    wtx.commit().unwrap();

    let tables = db.manager().list_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].0, b"keep");
}

#[test]
fn table_root_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.table_insert(b"data", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    let root = db.manager().table_root(b"data").unwrap();
    assert!(root.is_some());
    assert!(root.unwrap().is_valid());
}

#[test]
fn table_root_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let root = db.manager().table_root(b"nonexistent").unwrap();
    assert!(root.is_none());
}

#[test]
fn table_root_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    {
        let db = fast_builder(&path).create().unwrap();
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"persist").unwrap();
        wtx.table_insert(b"persist", b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }

    let db = DatabaseBuilder::new(&path)
        .passphrase(b"enum-test")
        .open()
        .unwrap();
    let tables = db.manager().list_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].0, b"persist");

    let root = db.manager().table_root(b"persist").unwrap();
    assert!(root.is_some());
}
