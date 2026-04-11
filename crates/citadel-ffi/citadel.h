#ifndef CITADEL_H
#define CITADEL_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Error codes returned by all citadel_* functions.
 */
enum citadel_error_t
#ifdef __cplusplus
  : int32_t
#endif // __cplusplus
 {
    Ok = 0,
    InvalidArgument = -1,
    IoError = -2,
    BadPassphrase = -3,
    DatabaseLocked = -4,
    DatabaseCorrupted = -5,
    PageTampered = -6,
    TransactionTooLarge = -7,
    KeyTooLarge = -8,
    ValueTooLarge = -9,
    TableNotFound = -10,
    TableAlreadyExists = -11,
    KeyFileMismatch = -12,
    PassphraseRequired = -13,
    NoWriteTransaction = -14,
    WriteTransactionActive = -15,
    SqlError = -16,
    InternalPanic = -99,
};
#ifndef __cplusplus
typedef int32_t citadel_error_t;
#endif // __cplusplus

/**
 * Value type tag for SQL result cells.
 */
enum CitadelValueType
#ifdef __cplusplus
  : int32_t
#endif // __cplusplus
 {
    Null = 0,
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
    Boolean = 5,
};
#ifndef __cplusplus
typedef int32_t CitadelValueType;
#endif // __cplusplus

/**
 * Opaque database handle.
 */
typedef struct CitadelDb CitadelDb;

/**
 * Opaque read transaction handle.
 */
typedef struct CitadelReadTxn CitadelReadTxn;

/**
 * Opaque SQL connection handle.
 */
typedef struct CitadelSqlConn CitadelSqlConn;

/**
 * Opaque SQL result handle.
 */
typedef struct CitadelSqlResult CitadelSqlResult;

/**
 * Opaque write transaction handle.
 */
typedef struct CitadelWriteTxn CitadelWriteTxn;

/**
 * Opaque database configuration.
 */
typedef struct CitadelConfig {
    uint32_t cache_size;
    uint8_t argon2_profile;
    uint8_t cipher_id;
    uint8_t _reserved[26];
} CitadelConfig;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Get the last error message for the current thread.
 *
 * Returns a pointer to a null-terminated UTF-8 string. The pointer is
 * valid until the next citadel_* call on this thread. Returns NULL if
 * no error occurred.
 */
CITADEL_API const char *citadel_last_error_message(void);

/**
 * Get the library version string.
 *
 * Returns a pointer to a static null-terminated string.
 */
CITADEL_API const char *citadel_version(void);

/**
 * Create a new encrypted database.
 *
 * # Parameters
 * - `path`: null-terminated UTF-8 path to the data file
 * - `passphrase`: passphrase bytes (not null-terminated)
 * - `passphrase_len`: length of the passphrase
 * - `config`: optional configuration (NULL for defaults)
 * - `out`: receives the database handle on success
 *
 * # Returns
 * `CITADEL_OK` on success, error code on failure.
 */
CITADEL_API
citadel_error_t citadel_create(const char *path,
                               const uint8_t *passphrase,
                               uintptr_t passphrase_len,
                               const struct CitadelConfig *config,
                               struct CitadelDb **out);

/**
 * Open an existing encrypted database.
 *
 * # Parameters
 * - `path`: null-terminated UTF-8 path to the data file
 * - `passphrase`: passphrase bytes (not null-terminated)
 * - `passphrase_len`: length of the passphrase
 * - `config`: optional configuration (NULL for defaults)
 * - `out`: receives the database handle on success
 *
 * # Returns
 * `CITADEL_OK` on success, error code on failure.
 */
CITADEL_API
citadel_error_t citadel_open(const char *path,
                             const uint8_t *passphrase,
                             uintptr_t passphrase_len,
                             const struct CitadelConfig *config,
                             struct CitadelDb **out);

/**
 * Close a database and free its resources.
 *
 * Accepts NULL (no-op). After this call the handle is invalid.
 */
CITADEL_API void citadel_close(struct CitadelDb *db);

/**
 * Begin a read-only transaction.
 *
 * Multiple read transactions can be active simultaneously.
 */
CITADEL_API citadel_error_t citadel_read_begin(struct CitadelDb *db, struct CitadelReadTxn **out);

/**
 * End a read transaction and free its resources.
 *
 * Accepts NULL (no-op).
 */
CITADEL_API void citadel_read_end(struct CitadelReadTxn *txn);

/**
 * Get a value by key in a read transaction.
 *
 * On success, `*out_val` and `*out_val_len` are set. The memory is
 * allocated by Citadel and must be freed with `citadel_free_bytes`.
 * If the key is not found, `*out_val` is set to NULL and
 * `*out_val_len` to 0, and the function returns `CITADEL_OK`.
 */
CITADEL_API
citadel_error_t citadel_read_get(struct CitadelReadTxn *txn,
                                 const uint8_t *key,
                                 uintptr_t key_len,
                                 uint8_t **out_val,
                                 uintptr_t *out_val_len);

/**
 * Get a value by key from a named table in a read transaction.
 */
CITADEL_API
citadel_error_t citadel_read_table_get(struct CitadelReadTxn *txn,
                                       const uint8_t *table,
                                       uintptr_t table_len,
                                       const uint8_t *key,
                                       uintptr_t key_len,
                                       uint8_t **out_val,
                                       uintptr_t *out_val_len);

/**
 * Begin a read-write transaction.
 *
 * Only one write transaction can be active at a time.
 */
CITADEL_API citadel_error_t citadel_write_begin(struct CitadelDb *db, struct CitadelWriteTxn **out);

/**
 * Commit a write transaction.
 *
 * On success the handle is consumed and freed. On failure the
 * transaction is still valid and can be retried or aborted.
 */
CITADEL_API citadel_error_t citadel_write_commit(struct CitadelWriteTxn *txn);

/**
 * Abort a write transaction and discard all changes.
 *
 * Accepts NULL (no-op). The handle is freed.
 */
CITADEL_API void citadel_write_abort(struct CitadelWriteTxn *txn);

/**
 * Insert or update a key-value pair in the default table.
 *
 * `*was_new` is set to 1 if the key was new, 0 if it was updated.
 * `was_new` can be NULL if the caller doesn't care.
 */
CITADEL_API
citadel_error_t citadel_write_put(struct CitadelWriteTxn *txn,
                                  const uint8_t *key,
                                  uintptr_t key_len,
                                  const uint8_t *val,
                                  uintptr_t val_len,
                                  int32_t *was_new);

/**
 * Delete a key from the default table.
 *
 * `*existed` is set to 1 if the key existed, 0 otherwise.
 * `existed` can be NULL if the caller doesn't care.
 */
CITADEL_API
citadel_error_t citadel_write_delete(struct CitadelWriteTxn *txn,
                                     const uint8_t *key,
                                     uintptr_t key_len,
                                     int32_t *existed);

/**
 * Get a value by key within a write transaction.
 *
 * Same semantics as `citadel_read_get` but within an active write txn.
 */
CITADEL_API
citadel_error_t citadel_write_get(struct CitadelWriteTxn *txn,
                                  const uint8_t *key,
                                  uintptr_t key_len,
                                  uint8_t **out_val,
                                  uintptr_t *out_val_len);

/**
 * Create a named table within a write transaction.
 */
CITADEL_API
citadel_error_t citadel_write_create_table(struct CitadelWriteTxn *txn,
                                           const uint8_t *name,
                                           uintptr_t name_len);

/**
 * Drop a named table within a write transaction.
 */
CITADEL_API
citadel_error_t citadel_write_drop_table(struct CitadelWriteTxn *txn,
                                         const uint8_t *name,
                                         uintptr_t name_len);

/**
 * Insert or update a key-value pair in a named table.
 */
CITADEL_API
citadel_error_t citadel_write_table_put(struct CitadelWriteTxn *txn,
                                        const uint8_t *table,
                                        uintptr_t table_len,
                                        const uint8_t *key,
                                        uintptr_t key_len,
                                        const uint8_t *val,
                                        uintptr_t val_len,
                                        int32_t *was_new);

/**
 * Delete a key from a named table.
 */
CITADEL_API
citadel_error_t citadel_write_table_delete(struct CitadelWriteTxn *txn,
                                           const uint8_t *table,
                                           uintptr_t table_len,
                                           const uint8_t *key,
                                           uintptr_t key_len,
                                           int32_t *existed);

/**
 * Get a value by key from a named table within a write transaction.
 */
CITADEL_API
citadel_error_t citadel_write_table_get(struct CitadelWriteTxn *txn,
                                        const uint8_t *table,
                                        uintptr_t table_len,
                                        const uint8_t *key,
                                        uintptr_t key_len,
                                        uint8_t **out_val,
                                        uintptr_t *out_val_len);

/**
 * Open a SQL connection on a database.
 *
 * The connection borrows the database - the database must outlive the
 * connection.
 */
CITADEL_API citadel_error_t citadel_sql_open(struct CitadelDb *db, struct CitadelSqlConn **out);

/**
 * Close a SQL connection and free its resources.
 *
 * Accepts NULL (no-op).
 */
CITADEL_API void citadel_sql_close(struct CitadelSqlConn *conn);

/**
 * Execute a SQL statement.
 *
 * For DDL/DML statements, `*out` receives a result handle that can be
 * queried with `citadel_sql_rows_affected`. For SELECT queries, the
 * result handle provides column/row access. The result must be freed
 * with `citadel_sql_result_free`.
 *
 * `out` can be NULL if the caller doesn't need the result.
 */
CITADEL_API
citadel_error_t citadel_sql_execute(struct CitadelSqlConn *conn,
                                    const char *sql,
                                    struct CitadelSqlResult **out);

/**
 * Free a SQL result.
 *
 * Accepts NULL (no-op).
 */
CITADEL_API void citadel_sql_result_free(struct CitadelSqlResult *result);

/**
 * Get the number of rows affected by a DML statement.
 */
CITADEL_API uint64_t citadel_sql_rows_affected(const struct CitadelSqlResult *result);

/**
 * Check if a result is a query result (SELECT).
 */
CITADEL_API int32_t citadel_sql_is_query(const struct CitadelSqlResult *result);

/**
 * Get the number of columns in a query result.
 */
CITADEL_API uint32_t citadel_sql_column_count(const struct CitadelSqlResult *result);

/**
 * Get a column name by index.
 *
 * Returns a pointer to a null-terminated UTF-8 string. The pointer is
 * valid for the lifetime of the result. Returns NULL on invalid index.
 */
CITADEL_API
const char *citadel_sql_column_name(const struct CitadelSqlResult *result,
                                    uint32_t col);

/**
 * Get the number of rows in a query result.
 */
CITADEL_API uint64_t citadel_sql_row_count(const struct CitadelSqlResult *result);

/**
 * Get the type of a value in a query result cell.
 *
 * Returns `CITADEL_VALUE_NULL` for out-of-bounds access.
 */
CITADEL_API
CitadelValueType citadel_sql_value_type(const struct CitadelSqlResult *result,
                                        uint64_t row,
                                        uint32_t col);

/**
 * Get an integer value from a query result cell.
 *
 * Returns 0 for NULL or type mismatch.
 */
CITADEL_API
int64_t citadel_sql_value_int(const struct CitadelSqlResult *result,
                              uint64_t row,
                              uint32_t col);

/**
 * Get a real (double) value from a query result cell.
 *
 * Returns 0.0 for NULL or type mismatch.
 */
CITADEL_API
double citadel_sql_value_real(const struct CitadelSqlResult *result,
                              uint64_t row,
                              uint32_t col);

/**
 * Get a text value from a query result cell.
 *
 * Returns a pointer to a null-terminated UTF-8 string. The pointer is
 * valid for the lifetime of the result. Returns NULL for NULL values
 * or type mismatch. `*out_len` is set to the string length (excluding
 * null terminator). `out_len` can be NULL.
 */
CITADEL_API
const char *citadel_sql_value_text(const struct CitadelSqlResult *result,
                                   uint64_t row,
                                   uint32_t col,
                                   uintptr_t *out_len);

/**
 * Get a blob value from a query result cell.
 *
 * Returns a pointer to the blob data. The pointer is valid for the
 * lifetime of the result. Returns NULL for NULL values or type
 * mismatch. `*out_len` is set to the blob length. `out_len` must
 * not be NULL.
 */
CITADEL_API
const uint8_t *citadel_sql_value_blob(const struct CitadelSqlResult *result,
                                      uint64_t row,
                                      uint32_t col,
                                      uintptr_t *out_len);

/**
 * Free bytes allocated by Citadel (e.g., from citadel_read_get).
 *
 * Accepts NULL (no-op). `len` must be the exact length returned by
 * the allocating function.
 */
CITADEL_API void citadel_free_bytes(uint8_t *ptr, uintptr_t len);

/**
 * Get database statistics.
 *
 * On success, the out-parameters are filled. Any out-parameter can be
 * NULL if the caller doesn't want that value.
 */
CITADEL_API
citadel_error_t citadel_stats(const struct CitadelDb *db,
                              uint64_t *out_entry_count,
                              uint32_t *out_total_pages,
                              uint16_t *out_tree_depth);

/**
 * Change the database passphrase (fast key rotation).
 *
 * Re-wraps the Root Encryption Key with a new Master Key derived from
 * the new passphrase. No page re-encryption needed.
 */
CITADEL_API
citadel_error_t citadel_change_passphrase(const struct CitadelDb *db,
                                          const uint8_t *old_passphrase,
                                          uintptr_t old_len,
                                          const uint8_t *new_passphrase,
                                          uintptr_t new_len);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* CITADEL_H */
