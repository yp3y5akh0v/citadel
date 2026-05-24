# @citadeldb/wasm

WebAssembly bindings for [Citadel](https://github.com/yp3y5akh0v/citadel) - an encrypted-first embedded database engine that outperforms unencrypted SQLite.

Every page is encrypted at rest with AES-256-CTR + HMAC-SHA256. Runs entirely in the browser or Node.js with no server required.

## Install

```bash
npm install @citadeldb/wasm
```

## Usage

```js
import init, { CitadelDb } from "@citadeldb/wasm";

await init();

const db = new CitadelDb("my-passphrase");

// Single statement
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
const result = db.query("SELECT * FROM users;");
// { columns: ["id", "name"], rows: [[1, "Alice"], [2, "Bob"]] }

// Multi-statement script — returns one outcome per statement
const outcomes = db.run(`
    CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
    INSERT INTO posts VALUES (1, 'Hello'), (2, 'World');
    SELECT * FROM posts;
`);
// [
//   { type: "ok" },
//   { type: "rowsAffected", value: 2 },
//   { type: "query", columns: ["id", "title"], rows: [[1, "Hello"], [2, "World"]] }
// ]

// Key-value
db.put(new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]));
const value = db.get(new Uint8Array([1, 2, 3]));

// Named tables
db.tablePut("sessions", new Uint8Array([1]), new Uint8Array([2]));

// Stats
const stats = db.stats();
// { entryCount, totalPages, treeDepth }

// Cleanup
db.free();
```

## API

| Method | Description |
|--------|-------------|
| `new CitadelDb(passphrase)` | Create an in-memory encrypted database |
| `execute(sql)` | Execute single DDL/DML statement, returns rows affected |
| `query(sql)` | Execute single SELECT, returns `{ columns, rows }` |
| `run(sql)` | Execute `;`-separated statements, returns `[{type, ...}, ...]` |
| `executeBatch(sql)` | Execute `;`-separated statements, discards results |
| `put(key, value)` | Insert into default table |
| `get(key)` | Get from default table |
| `delete(key)` | Delete from default table |
| `tablePut(table, key, value)` | Insert into named table |
| `tableGet(table, key)` | Get from named table |
| `tableDelete(table, key)` | Delete from named table |
| `stats()` | Database statistics |
| `free()` | Release resources |

## SQL Support

**Statements** - CREATE/DROP TABLE (incl. `STRICT`), ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE), CREATE/DROP INDEX (UNIQUE + multi-column + partial `WHERE` + per-column `COLLATE` + `USING gin` for JSONB), CREATE/DROP VIEW, INSERT (VALUES, SELECT, ON CONFLICT DO NOTHING/DO UPDATE), SELECT, UPDATE, DELETE, TRUNCATE TABLE, RETURNING (with `OLD`/`NEW` row aliases), generated columns (`GENERATED ALWAYS AS (...) STORED|VIRTUAL`), BEGIN/COMMIT/ROLLBACK, SAVEPOINT/RELEASE/ROLLBACK TO, SET TIME ZONE, EXPLAIN

**Types** - INTEGER, REAL, TEXT, BLOB, BOOLEAN, DATE, TIME, TIMESTAMP (with timezone), INTERVAL, **JSON** (lossless text), **JSONB** (canonical binary). Large values spill to overflow page chains transparently.

**JSON / JSONB** - 12 PG operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `#-`, `@?`, `@@`), 16 scalar functions (`jsonb_typeof`, `jsonb_extract_path`, `jsonb_set`, `jsonb_pretty`, `to_jsonb`, etc.), 4 aggregates (`json_agg`, `jsonb_agg`, `json_object_agg`, `jsonb_object_agg`), 5 set-returning functions (`jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`, etc.), `JSON_TABLE` / `JSON_EXISTS` / `JSON_VALUE` / `JSON_QUERY` with full SQL/JSON predicate path language (e.g. `$.items[*] ? (@.x > 5)`), GIN inverted indexes (`CREATE INDEX … USING gin`) for accelerated `@>` containment

**Full-text search** - `tsvector` / `tsquery` types, `to_tsvector` / `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery` builders, `@@` match operator, `ts_rank` / `ts_rank_cd` ranking with weighted positions (A/B/C/D), prefix matching (`term:*`), phrase distance (`<N>`), inverted indexes via `CREATE INDEX … USING fts` for sub-millisecond search at scale

**Constraints** - PRIMARY KEY, NOT NULL, UNIQUE (column + table level, inline or `CREATE UNIQUE INDEX`), DEFAULT, CHECK (column + table level), FOREIGN KEY with full referential actions (`ON DELETE` / `ON UPDATE` `CASCADE` / `SET NULL` / `SET DEFAULT` / `RESTRICT` / `NO ACTION`)

**Clauses** - JOINs (INNER, LEFT, RIGHT, CROSS, FULL OUTER, LATERAL), subqueries (scalar, IN, EXISTS, correlated), CTEs (`WITH` / `WITH RECURSIVE` / WITH-DML: `WITH x AS (INSERT/UPDATE/DELETE … [RETURNING *]) SELECT …`), UNION/INTERSECT/EXCEPT [ALL], CASE, BETWEEN, LIKE, DISTINCT, GROUP BY/HAVING, ORDER BY (incl. `COLLATE`), LIMIT/OFFSET, `COLLATE` (BINARY/NOCASE/RTRIM)

**Window functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER with PARTITION BY, ORDER BY, ROWS/RANGE frames

**Views** - CREATE/DROP VIEW, OR REPLACE, IF NOT EXISTS/IF EXISTS, column aliases, nested views

**Scalar functions**
- Aggregate: COUNT, SUM, AVG, MIN, MAX
- String: LENGTH, UPPER, LOWER, SUBSTR/SUBSTRING, TRIM/LTRIM/RTRIM, REPLACE, INSTR, CONCAT, HEX
- Math: ABS, ROUND, CEIL/CEILING, FLOOR, SIGN, SQRT, RANDOM
- Conditional: COALESCE, NULLIF, IIF, CAST, TYPEOF

**Date / Time functions**
- Current: NOW, CURRENT_DATE, CURRENT_TIME/LOCALTIME, CURRENT_TIMESTAMP/LOCALTIMESTAMP, CLOCK_TIMESTAMP, STATEMENT_TIMESTAMP, TRANSACTION_TIMESTAMP
- Construction: MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_INTERVAL
- Extraction / truncation: EXTRACT, DATE_PART, DATEPART, DATE_TRUNC, DATE_BIN
- Conversion (SQLite-compatible): DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH
- Arithmetic: AGE, TIMEDIFF, AT_TIMEZONE, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL, ISFINITE
- IANA zone support, BC dates, `+infinity`/`-infinity` sentinels, PG-normalized INTERVAL comparison

**Prepared statements** - `$1, $2, ...` positional parameters with LRU statement cache plus snapshot-tagged plan caching for joins and compound queries (cache invalidates only on commit, never per-call)

## License

MIT OR Apache-2.0
