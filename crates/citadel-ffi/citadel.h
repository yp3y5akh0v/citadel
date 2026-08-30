#ifndef CITADEL_H
#define CITADEL_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifndef CITADEL_API
#define CITADEL_API
#endif

/**
 * Error codes returned by all citadel_* functions.
 */
enum citadel_error_t
#ifdef __cplusplus
  : int32_t
#endif // __cplusplus
 {
    CITADEL_ERROR_T_OK = 0,
    CITADEL_ERROR_T_INVALID_ARGUMENT = -1,
    CITADEL_ERROR_T_IO_ERROR = -2,
    CITADEL_ERROR_T_BAD_PASSPHRASE = -3,
    CITADEL_ERROR_T_DATABASE_LOCKED = -4,
    CITADEL_ERROR_T_DATABASE_CORRUPTED = -5,
    CITADEL_ERROR_T_PAGE_TAMPERED = -6,
    CITADEL_ERROR_T_TRANSACTION_TOO_LARGE = -7,
    CITADEL_ERROR_T_KEY_TOO_LARGE = -8,
    CITADEL_ERROR_T_VALUE_TOO_LARGE = -9,
    CITADEL_ERROR_T_TABLE_NOT_FOUND = -10,
    CITADEL_ERROR_T_TABLE_ALREADY_EXISTS = -11,
    CITADEL_ERROR_T_KEY_FILE_MISMATCH = -12,
    CITADEL_ERROR_T_PASSPHRASE_REQUIRED = -13,
    CITADEL_ERROR_T_NO_WRITE_TRANSACTION = -14,
    CITADEL_ERROR_T_WRITE_TRANSACTION_ACTIVE = -15,
    CITADEL_ERROR_T_SQL_ERROR = -16,
    CITADEL_ERROR_T_NAMED_TABLE_HASH_COLLISION = -17,
    CITADEL_ERROR_T_INTERRUPTED = -18,
    CITADEL_ERROR_T_TRANSACTION_FAILED = -19,
    CITADEL_ERROR_T_REGION_IN_USE = -20,
    CITADEL_ERROR_T_ATOM_IN_USE = -21,
    CITADEL_ERROR_T_INTERNAL_PANIC = -99,
};
#ifndef __cplusplus
typedef int32_t citadel_error_t;
#endif // __cplusplus

/**
 * Stable category for one integrity finding.
 */
enum CitadelIntegrityErrorKind
#ifdef __cplusplus
  : int32_t
#endif // __cplusplus
 {
    CITADEL_INTEGRITY_ERROR_KIND_UNKNOWN = -1,
    CITADEL_INTEGRITY_ERROR_KIND_COMMIT_SLOT_CHECKSUM_MISMATCH = 0,
    CITADEL_INTEGRITY_ERROR_KIND_COMMIT_SLOT_MAC_MISMATCH = 1,
    CITADEL_INTEGRITY_ERROR_KIND_COMMIT_SLOT_DOWNGRADE = 2,
    CITADEL_INTEGRITY_ERROR_KIND_COMMIT_SLOT_UNKNOWN_FORMAT = 3,
    CITADEL_INTEGRITY_ERROR_KIND_PAGE_READ_FAILED = 4,
    CITADEL_INTEGRITY_ERROR_KIND_PAGE_TAMPERED = 5,
    CITADEL_INTEGRITY_ERROR_KIND_CHECKSUM_MISMATCH = 6,
    CITADEL_INTEGRITY_ERROR_KIND_KEY_ORDER_VIOLATION = 7,
    CITADEL_INTEGRITY_ERROR_KIND_DUPLICATE_PAGE_REF = 8,
    CITADEL_INTEGRITY_ERROR_KIND_ENTRY_COUNT_MISMATCH = 9,
    CITADEL_INTEGRITY_ERROR_KIND_NAMED_TABLE_ENTRY_COUNT_MISMATCH = 10,
    CITADEL_INTEGRITY_ERROR_KIND_MALFORMED_TABLE_DESCRIPTOR = 11,
    CITADEL_INTEGRITY_ERROR_KIND_NAMED_TABLE_HASH_COLLISION = 12,
    CITADEL_INTEGRITY_ERROR_KIND_DUPLICATE_NAMED_TABLE_SLOT_HASH = 13,
    CITADEL_INTEGRITY_ERROR_KIND_INVALID_PAGE_TYPE = 14,
    CITADEL_INTEGRITY_ERROR_KIND_PENDING_FREE_ENTRY_COUNT_OUT_OF_BOUNDS = 15,
    CITADEL_INTEGRITY_ERROR_KIND_KEY_RANGE_VIOLATION = 16,
    CITADEL_INTEGRITY_ERROR_KIND_MALFORMED_PAGE = 17,
    CITADEL_INTEGRITY_ERROR_KIND_PAGE_ID_MISMATCH = 18,
    CITADEL_INTEGRITY_ERROR_KIND_PAGE_TRANSACTION_OUT_OF_BOUNDS = 19,
    CITADEL_INTEGRITY_ERROR_KIND_REACHABLE_PAGE_OUT_OF_BOUNDS = 20,
    CITADEL_INTEGRITY_ERROR_KIND_MALFORMED_OVERFLOW_REFERENCE = 21,
    CITADEL_INTEGRITY_ERROR_KIND_OVERFLOW_LENGTH_OUT_OF_BOUNDS = 22,
    CITADEL_INTEGRITY_ERROR_KIND_OVERFLOW_PAGE_DATA_LENGTH_OUT_OF_BOUNDS = 23,
    CITADEL_INTEGRITY_ERROR_KIND_OVERFLOW_CHAIN_LENGTH_MISMATCH = 24,
    CITADEL_INTEGRITY_ERROR_KIND_OVERFLOW_CHAIN_PAGE_COUNT_OUT_OF_BOUNDS = 25,
    CITADEL_INTEGRITY_ERROR_KIND_INVALID_TABLE_DESCRIPTOR = 26,
    CITADEL_INTEGRITY_ERROR_KIND_PENDING_FREE_PAGE_OUT_OF_BOUNDS = 27,
    CITADEL_INTEGRITY_ERROR_KIND_PENDING_FREE_ENTRY_OUT_OF_BOUNDS = 28,
    CITADEL_INTEGRITY_ERROR_KIND_PENDING_FREE_TRANSACTION_OUT_OF_BOUNDS = 29,
    CITADEL_INTEGRITY_ERROR_KIND_DUPLICATE_PENDING_FREE_ENTRY = 30,
    CITADEL_INTEGRITY_ERROR_KIND_PENDING_FREE_ENTRY_STILL_REACHABLE = 31,
    CITADEL_INTEGRITY_ERROR_KIND_TREE_DEPTH_MISMATCH = 32,
    CITADEL_INTEGRITY_ERROR_KIND_PAGE_MERKLE_MISMATCH = 33,
    CITADEL_INTEGRITY_ERROR_KIND_SLOT_MERKLE_ROOT_MISMATCH = 34,
    CITADEL_INTEGRITY_ERROR_KIND_PAGE_COUNT_METADATA_MISMATCH = 35,
    CITADEL_INTEGRITY_ERROR_KIND_OVERFLOW_DIGEST_MISMATCH = 36,
    CITADEL_INTEGRITY_ERROR_KIND_COMMIT_SLOT_UNKNOWN_MERKLE_SCHEME = 37,
};
#ifndef __cplusplus
typedef int32_t CitadelIntegrityErrorKind;
#endif // __cplusplus

/**
 * Value type tag for SQL result cells.
 */
enum CitadelValueType
#ifdef __cplusplus
  : int32_t
#endif // __cplusplus
 {
    CITADEL_VALUE_TYPE_NULL = 0,
    CITADEL_VALUE_TYPE_INTEGER = 1,
    CITADEL_VALUE_TYPE_REAL = 2,
    CITADEL_VALUE_TYPE_TEXT = 3,
    CITADEL_VALUE_TYPE_BLOB = 4,
    CITADEL_VALUE_TYPE_BOOLEAN = 5,
    CITADEL_VALUE_TYPE_DATE = 6,
    CITADEL_VALUE_TYPE_TIME = 7,
    CITADEL_VALUE_TYPE_TIMESTAMP = 8,
    CITADEL_VALUE_TYPE_INTERVAL = 9,
    CITADEL_VALUE_TYPE_JSON = 10,
    CITADEL_VALUE_TYPE_JSONB = 11,
    CITADEL_VALUE_TYPE_TS_VECTOR = 12,
    CITADEL_VALUE_TYPE_TS_QUERY = 13,
    CITADEL_VALUE_TYPE_ARRAY = 14,
    CITADEL_VALUE_TYPE_VECTOR = 15,
};
#ifndef __cplusplus
typedef int32_t CitadelValueType;
#endif // __cplusplus

/**
 * Opaque, thread-safe, one-shot cancellation token.
 */
typedef struct CitadelCancelToken CitadelCancelToken;

/**
 * Opaque database handle.
 */
typedef struct CitadelDb CitadelDb;

/**
 * Opaque result from an integrity walk.
 */
typedef struct CitadelIntegrityResult CitadelIntegrityResult;

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
 * Database configuration. Zero-initialize this structure before setting
 * fields; every reserved byte must remain zero.
 */
typedef struct CitadelConfig {
    uint32_t cache_size;
    uint8_t argon2_profile;
    uint8_t _reserved[27];
} CitadelConfig;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Get the last error message for the current thread.
 *
 * Returns a pointer to a null-terminated UTF-8 string. The pointer is
 * valid until the next function returning `citadel_error_t` is called
 * on this thread. Returns NULL if no error occurred.
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
 * `CITADEL_ERROR_T_OK` on success, error code on failure.
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
 * `CITADEL_ERROR_T_OK` on success, error code on failure.
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
 * Allocate a fresh, untripped cancellation token.
 */
CITADEL_API citadel_error_t citadel_cancel_token_new(struct CitadelCancelToken **out);

/**
 * Trip a live cancellation token. Safe to call more than once or from another thread.
 */
CITADEL_API citadel_error_t citadel_cancel_token_cancel(const struct CitadelCancelToken *token);

/**
 * Return 1 when a cancellation token has been tripped, otherwise 0.
 */
CITADEL_API int32_t citadel_cancel_token_is_cancelled(const struct CitadelCancelToken *token);

/**
 * Free a cancellation token. Accepts NULL. The caller must ensure no other
 * thread is accessing the handle.
 */
CITADEL_API void citadel_cancel_token_free(struct CitadelCancelToken *token);

/**
 * Install a clone of `token` for subsequent work, or clear it with NULL.
 *
 * The caller may free its token after this call. Install a fresh token before
 * beginning the operation or direct transaction it should govern.
 */
CITADEL_API
citadel_error_t citadel_set_cancel(const struct CitadelDb *db,
                                   const struct CitadelCancelToken *token);

/**
 * Begin a read-only transaction.
 *
 * Multiple read transactions can be active simultaneously.
 * The returned handle retains the database; `db` may be closed first.
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
 * `*out_val_len` to 0, and the function returns `CITADEL_ERROR_T_OK`.
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
 * The returned handle retains the database; `db` may be closed first.
 */
CITADEL_API citadel_error_t citadel_write_begin(struct CitadelDb *db, struct CitadelWriteTxn **out);

/**
 * Commit a write transaction.
 *
 * The handle is consumed and freed whether commit succeeds or fails. A failed
 * commit has already rolled back; do not pass the pointer to another function.
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
 * The returned connection retains the database; `db` may be closed first.
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
 * Returns `CITADEL_VALUE_TYPE_NULL` for out-of-bounds access.
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
 * Returns UTF-8 bytes followed by a terminal NUL. The pointer is valid
 * for the lifetime of the result. Text may contain embedded NULs, so use
 * `out_len` rather than `strlen`. Returns NULL for NULL values or type
 * mismatch. `out_len` can be NULL.
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
 * Walk both commit slots and all reachable pages.
 *
 * Integrity findings are returned in `out` with `CITADEL_ERROR_T_OK`; failures that
 * prevent the walk from running are returned as an error code. Pass nonzero
 * `quiet` to avoid writing an audit-log entry for the inspection itself.
 */
CITADEL_API
citadel_error_t citadel_integrity_check(const struct CitadelDb *db,
                                        int32_t quiet,
                                        struct CitadelIntegrityResult **out);

/**
 * Free an integrity result. Accepts NULL.
 */
CITADEL_API void citadel_integrity_result_free(struct CitadelIntegrityResult *result);

/**
 * Return the number of pages examined by an integrity walk.
 */
CITADEL_API uint64_t citadel_integrity_pages_checked(const struct CitadelIntegrityResult *result);

/**
 * Return the number of integrity findings.
 */
CITADEL_API uintptr_t citadel_integrity_error_count(const struct CitadelIntegrityResult *result);

/**
 * Return the number of findings classified as byte tampering.
 */
CITADEL_API uintptr_t citadel_integrity_tampered_count(const struct CitadelIntegrityResult *result);

/**
 * Return one finding's stable category. Out-of-range indexes return
 * `CITADEL_INTEGRITY_ERROR_KIND_UNKNOWN`.
 */
CITADEL_API
CitadelIntegrityErrorKind citadel_integrity_error_kind(const struct CitadelIntegrityResult *result,
                                                       uintptr_t index);

/**
 * Return one finding's message, valid until the result is freed.
 */
CITADEL_API
const char *citadel_integrity_error_message(const struct CitadelIntegrityResult *result,
                                            uintptr_t index);

/**
 * Return 1 when one finding represents altered bytes, otherwise 0.
 */
CITADEL_API
int32_t citadel_integrity_error_is_tampered(const struct CitadelIntegrityResult *result,
                                            uintptr_t index);

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
