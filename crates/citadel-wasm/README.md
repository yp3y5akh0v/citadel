# @citadeldb/wasm

WebAssembly bindings for [Citadel](https://github.com/yp3y5akh0v/citadel) — an encrypted-first embedded database engine that outperforms unencrypted SQLite.

Every value is encrypted at rest with AES-256-CTR + HMAC-SHA256. Runs entirely in the browser or Node.js with no server required.

## Install

```bash
npm install @citadeldb/wasm
```

## Usage

```js
import init, { CitadelDb } from "@citadeldb/wasm";

await init();

const db = new CitadelDb("my-passphrase");

// SQL
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
const result = db.query("SELECT * FROM users;");
// { columns: ["id", "name"], rows: [[1, "Alice"], [2, "Bob"]] }

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
| `execute(sql)` | Execute DDL/DML, returns rows affected |
| `executeBatch(sql)` | Execute multiple statements |
| `query(sql)` | Execute SELECT, returns `{ columns, rows }` |
| `put(key, value)` | Insert into default table |
| `get(key)` | Get from default table |
| `delete(key)` | Delete from default table |
| `tablePut(table, key, value)` | Insert into named table |
| `tableGet(table, key)` | Get from named table |
| `tableDelete(table, key)` | Delete from named table |
| `stats()` | Database statistics |
| `free()` | Release resources |

## SQL Support

CREATE/DROP TABLE, CREATE/DROP INDEX, INSERT, SELECT, UPDATE, DELETE, JOINs (INNER, LEFT, RIGHT, CROSS), subqueries, aggregates, DISTINCT, GROUP BY, ORDER BY, LIMIT/OFFSET, BETWEEN, LIKE, CASE, prepared statements.

Types: INTEGER, REAL, TEXT, BLOB, BOOLEAN.

## License

MIT OR Apache-2.0
