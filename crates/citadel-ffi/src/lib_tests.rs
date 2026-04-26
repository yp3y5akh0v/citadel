use super::*;
use std::ffi::CString;

fn temp_path() -> (tempfile::TempDir, CString) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.citadel");
    let cpath = CString::new(path.to_str().unwrap()).unwrap();
    (dir, cpath)
}

#[test]
fn create_open_close() {
    let (_dir, cpath) = temp_path();
    let pass = b"secret";
    let mut db: *mut CitadelDb = ptr::null_mut();

    let rc = citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );
    assert_eq!(rc, CitadelError::Ok);
    assert!(!db.is_null());
    citadel_close(db);

    let mut db2: *mut CitadelDb = ptr::null_mut();
    let rc = citadel_open(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db2,
    );
    assert_eq!(rc, CitadelError::Ok);
    assert!(!db2.is_null());
    citadel_close(db2);
}

#[test]
fn wrong_passphrase() {
    let (_dir, cpath) = temp_path();
    let pass = b"correct";
    let mut db: *mut CitadelDb = ptr::null_mut();

    let rc = citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );
    assert_eq!(rc, CitadelError::Ok);
    citadel_close(db);

    let wrong = b"wrong";
    let mut db2: *mut CitadelDb = ptr::null_mut();
    let rc = citadel_open(
        cpath.as_ptr(),
        wrong.as_ptr(),
        wrong.len(),
        ptr::null(),
        &mut db2,
    );
    assert_eq!(rc, CitadelError::BadPassphrase);
    assert!(db2.is_null());

    let msg = citadel_last_error_message();
    assert!(!msg.is_null());
}

#[test]
fn null_pointer_safety() {
    assert_eq!(
        citadel_create(ptr::null(), ptr::null(), 0, ptr::null(), ptr::null_mut()),
        CitadelError::InvalidArgument,
    );
    citadel_close(ptr::null_mut());
    citadel_read_end(ptr::null_mut());
    citadel_write_abort(ptr::null_mut());
    citadel_sql_close(ptr::null_mut());
    citadel_sql_result_free(ptr::null_mut());
    citadel_free_bytes(ptr::null_mut(), 0);
}

#[test]
fn kv_roundtrip() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();

    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    assert_eq!(citadel_write_begin(db, &mut wtxn), CitadelError::Ok);

    let key = b"hello";
    let val = b"world";
    let mut was_new: i32 = 0;
    assert_eq!(
        citadel_write_put(
            wtxn,
            key.as_ptr(),
            key.len(),
            val.as_ptr(),
            val.len(),
            &mut was_new,
        ),
        CitadelError::Ok,
    );
    assert_eq!(was_new, 1);

    assert_eq!(citadel_write_commit(wtxn), CitadelError::Ok);

    let mut rtxn: *mut CitadelReadTxn = ptr::null_mut();
    assert_eq!(citadel_read_begin(db, &mut rtxn), CitadelError::Ok);

    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    assert_eq!(
        citadel_read_get(rtxn, key.as_ptr(), key.len(), &mut out_val, &mut out_len,),
        CitadelError::Ok,
    );
    assert!(!out_val.is_null());
    assert_eq!(out_len, 5);

    let result = unsafe { slice::from_raw_parts(out_val, out_len) };
    assert_eq!(result, b"world");
    citadel_free_bytes(out_val, out_len);

    let missing = b"missing";
    let mut out_val2: *mut u8 = ptr::null_mut();
    let mut out_len2: usize = 0;
    assert_eq!(
        citadel_read_get(
            rtxn,
            missing.as_ptr(),
            missing.len(),
            &mut out_val2,
            &mut out_len2,
        ),
        CitadelError::Ok,
    );
    assert!(out_val2.is_null());
    assert_eq!(out_len2, 0);

    citadel_read_end(rtxn);
    citadel_close(db);
}

#[test]
fn write_abort() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn);

    let key = b"aborted";
    let val = b"data";
    citadel_write_put(
        wtxn,
        key.as_ptr(),
        key.len(),
        val.as_ptr(),
        val.len(),
        ptr::null_mut(),
    );
    citadel_write_abort(wtxn);

    let mut rtxn: *mut CitadelReadTxn = ptr::null_mut();
    citadel_read_begin(db, &mut rtxn);

    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    citadel_read_get(rtxn, key.as_ptr(), key.len(), &mut out_val, &mut out_len);
    assert!(out_val.is_null());

    citadel_read_end(rtxn);
    citadel_close(db);
}

#[test]
fn named_table_roundtrip() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn);

    let table = b"users";
    assert_eq!(
        citadel_write_create_table(wtxn, table.as_ptr(), table.len()),
        CitadelError::Ok,
    );

    let key = b"alice";
    let val = b"admin";
    citadel_write_table_put(
        wtxn,
        table.as_ptr(),
        table.len(),
        key.as_ptr(),
        key.len(),
        val.as_ptr(),
        val.len(),
        ptr::null_mut(),
    );
    citadel_write_commit(wtxn);

    let mut rtxn: *mut CitadelReadTxn = ptr::null_mut();
    citadel_read_begin(db, &mut rtxn);

    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    assert_eq!(
        citadel_read_table_get(
            rtxn,
            table.as_ptr(),
            table.len(),
            key.as_ptr(),
            key.len(),
            &mut out_val,
            &mut out_len,
        ),
        CitadelError::Ok,
    );
    let result = unsafe { slice::from_raw_parts(out_val, out_len) };
    assert_eq!(result, b"admin");
    citadel_free_bytes(out_val, out_len);

    citadel_read_end(rtxn);
    citadel_close(db);
}

#[test]
fn sql_roundtrip() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut conn: *mut CitadelSqlConn = ptr::null_mut();
    assert_eq!(citadel_sql_open(db, &mut conn), CitadelError::Ok);

    let sql1 =
        CString::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    let mut result: *mut CitadelSqlResult = ptr::null_mut();
    assert_eq!(
        citadel_sql_execute(conn, sql1.as_ptr(), &mut result),
        CitadelError::Ok,
    );
    citadel_sql_result_free(result);

    let sql2 = CString::new("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    result = ptr::null_mut();
    assert_eq!(
        citadel_sql_execute(conn, sql2.as_ptr(), &mut result),
        CitadelError::Ok,
    );
    assert_eq!(citadel_sql_rows_affected(result), 1);
    citadel_sql_result_free(result);

    let sql3 = CString::new("SELECT id, name FROM users").unwrap();
    result = ptr::null_mut();
    assert_eq!(
        citadel_sql_execute(conn, sql3.as_ptr(), &mut result),
        CitadelError::Ok,
    );
    assert_eq!(citadel_sql_is_query(result), 1);
    assert_eq!(citadel_sql_column_count(result), 2);
    assert_eq!(citadel_sql_row_count(result), 1);

    let col0 = citadel_sql_column_name(result, 0);
    assert!(!col0.is_null());
    let col0_str = unsafe { CStr::from_ptr(col0) }.to_str().unwrap();
    assert_eq!(col0_str, "id");

    assert!(matches!(
        citadel_sql_value_type(result, 0, 0),
        CitadelValueType::Integer,
    ));
    assert_eq!(citadel_sql_value_int(result, 0, 0), 1);

    assert!(matches!(
        citadel_sql_value_type(result, 0, 1),
        CitadelValueType::Text,
    ));
    let mut text_len: usize = 0;
    let text_ptr = citadel_sql_value_text(result, 0, 1, &mut text_len);
    assert!(!text_ptr.is_null());
    let text = unsafe { CStr::from_ptr(text_ptr) }.to_str().unwrap();
    assert_eq!(text, "Alice");
    assert_eq!(text_len, 5);

    citadel_sql_result_free(result);
    citadel_sql_close(conn);
    citadel_close(db);
}

#[test]
fn stats() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut entry_count: u64 = 0;
    let mut total_pages: u32 = 0;
    let mut tree_depth: u16 = 0;

    assert_eq!(
        citadel_stats(db, &mut entry_count, &mut total_pages, &mut tree_depth),
        CitadelError::Ok,
    );
    assert_eq!(entry_count, 0);

    citadel_close(db);
}

#[test]
fn version_string() {
    let v = citadel_version();
    assert!(!v.is_null());
    let s = unsafe { CStr::from_ptr(v) }.to_str().unwrap();
    assert_eq!(s, env!("CARGO_PKG_VERSION"));
}

#[test]
fn error_message() {
    let rc = citadel_create(ptr::null(), ptr::null(), 0, ptr::null(), ptr::null_mut());
    assert_eq!(rc, CitadelError::InvalidArgument);
    let msg = citadel_last_error_message();
    assert!(!msg.is_null());
    let s = unsafe { CStr::from_ptr(msg) }.to_str().unwrap();
    assert!(s.contains("null pointer"));

    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    let rc = citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );
    assert_eq!(rc, CitadelError::Ok);
    let msg_after = citadel_last_error_message();
    assert!(msg_after.is_null());
    citadel_close(db);
}

#[test]
fn delete_roundtrip() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn);
    let key = b"delete_me";
    let val = b"data";
    citadel_write_put(
        wtxn,
        key.as_ptr(),
        key.len(),
        val.as_ptr(),
        val.len(),
        ptr::null_mut(),
    );
    citadel_write_commit(wtxn);

    let mut wtxn2: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn2);
    let mut existed: i32 = 0;
    assert_eq!(
        citadel_write_delete(wtxn2, key.as_ptr(), key.len(), &mut existed),
        CitadelError::Ok,
    );
    assert_eq!(existed, 1);
    citadel_write_commit(wtxn2);

    let mut rtxn: *mut CitadelReadTxn = ptr::null_mut();
    citadel_read_begin(db, &mut rtxn);
    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    citadel_read_get(rtxn, key.as_ptr(), key.len(), &mut out_val, &mut out_len);
    assert!(out_val.is_null());
    citadel_read_end(rtxn);
    citadel_close(db);
}

#[test]
fn write_get_within_txn() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn);

    let key = b"k1";
    let val = b"v1";
    citadel_write_put(
        wtxn,
        key.as_ptr(),
        key.len(),
        val.as_ptr(),
        val.len(),
        ptr::null_mut(),
    );

    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    assert_eq!(
        citadel_write_get(wtxn, key.as_ptr(), key.len(), &mut out_val, &mut out_len,),
        CitadelError::Ok,
    );
    assert!(!out_val.is_null());
    let result = unsafe { slice::from_raw_parts(out_val, out_len) };
    assert_eq!(result, b"v1");
    citadel_free_bytes(out_val, out_len);

    citadel_write_commit(wtxn);
    citadel_close(db);
}

#[test]
fn change_passphrase_ffi() {
    let (_dir, cpath) = temp_path();
    let pass = b"old_pass";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn);
    let key = b"key";
    let val = b"val";
    citadel_write_put(
        wtxn,
        key.as_ptr(),
        key.len(),
        val.as_ptr(),
        val.len(),
        ptr::null_mut(),
    );
    citadel_write_commit(wtxn);

    let new_pass = b"new_pass";
    assert_eq!(
        citadel_change_passphrase(
            db,
            pass.as_ptr(),
            pass.len(),
            new_pass.as_ptr(),
            new_pass.len(),
        ),
        CitadelError::Ok,
    );
    citadel_close(db);

    let mut db2: *mut CitadelDb = ptr::null_mut();
    assert_eq!(
        citadel_open(
            cpath.as_ptr(),
            new_pass.as_ptr(),
            new_pass.len(),
            ptr::null(),
            &mut db2,
        ),
        CitadelError::Ok,
    );

    let mut rtxn: *mut CitadelReadTxn = ptr::null_mut();
    citadel_read_begin(db2, &mut rtxn);
    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    citadel_read_get(rtxn, key.as_ptr(), key.len(), &mut out_val, &mut out_len);
    let result = unsafe { slice::from_raw_parts(out_val, out_len) };
    assert_eq!(result, b"val");
    citadel_free_bytes(out_val, out_len);
    citadel_read_end(rtxn);

    citadel_close(db2);
    let mut db3: *mut CitadelDb = ptr::null_mut();
    assert_eq!(
        citadel_open(
            cpath.as_ptr(),
            pass.as_ptr(),
            pass.len(),
            ptr::null(),
            &mut db3,
        ),
        CitadelError::BadPassphrase,
    );
}

#[test]
fn many_entries() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut wtxn: *mut CitadelWriteTxn = ptr::null_mut();
    citadel_write_begin(db, &mut wtxn);

    for i in 0..200u32 {
        let key = format!("key-{i:05}");
        let val = format!("val-{i:05}");
        citadel_write_put(
            wtxn,
            key.as_bytes().as_ptr(),
            key.len(),
            val.as_bytes().as_ptr(),
            val.len(),
            ptr::null_mut(),
        );
    }
    citadel_write_commit(wtxn);

    let mut entry_count: u64 = 0;
    citadel_stats(db, &mut entry_count, ptr::null_mut(), ptr::null_mut());
    assert_eq!(entry_count, 200);

    let mut rtxn: *mut CitadelReadTxn = ptr::null_mut();
    citadel_read_begin(db, &mut rtxn);

    let key = b"key-00000";
    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_len: usize = 0;
    citadel_read_get(rtxn, key.as_ptr(), key.len(), &mut out_val, &mut out_len);
    let result = unsafe { slice::from_raw_parts(out_val, out_len) };
    assert_eq!(result, b"val-00000");
    citadel_free_bytes(out_val, out_len);

    citadel_read_end(rtxn);
    citadel_close(db);
}

#[test]
fn sql_error_handling() {
    let (_dir, cpath) = temp_path();
    let pass = b"test";
    let mut db: *mut CitadelDb = ptr::null_mut();
    citadel_create(
        cpath.as_ptr(),
        pass.as_ptr(),
        pass.len(),
        ptr::null(),
        &mut db,
    );

    let mut conn: *mut CitadelSqlConn = ptr::null_mut();
    citadel_sql_open(db, &mut conn);

    let bad_sql = CString::new("NOT VALID SQL AT ALL!!!").unwrap();
    let mut result: *mut CitadelSqlResult = ptr::null_mut();
    let rc = citadel_sql_execute(conn, bad_sql.as_ptr(), &mut result);
    assert_eq!(rc, CitadelError::SqlError);
    assert!(result.is_null());

    let msg = citadel_last_error_message();
    assert!(!msg.is_null());

    citadel_sql_close(conn);
    citadel_close(db);
}

#[test]
fn config_defaults() {
    let cfg = CitadelConfig::default();
    assert_eq!(cfg.cache_size, 256);
    assert_eq!(cfg.argon2_profile, 1);
    assert_eq!(cfg.cipher_id, 0);
}

#[test]
fn close_null_safety() {
    citadel_close(ptr::null_mut());
    citadel_read_end(ptr::null_mut());
    citadel_write_abort(ptr::null_mut());
    citadel_sql_close(ptr::null_mut());
    citadel_sql_result_free(ptr::null_mut());
    citadel_free_bytes(ptr::null_mut(), 0);
}
