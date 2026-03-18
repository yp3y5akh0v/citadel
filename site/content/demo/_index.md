+++
title = "SQL Playground"
template = "section.html"
+++

Try Citadel's encrypted database engine right in your browser. Everything runs locally via WebAssembly - no server, no data leaves your machine.

Press **Ctrl+Enter** to run. Try this:

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

{{ wasm_demo() }}
