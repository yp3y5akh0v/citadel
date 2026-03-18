+++
title = "Citadel"
+++

## Quick Start

```bash
cargo add citadel citadel-sql
```

```rust
use citadel::{DatabaseBuilder, Argon2Profile};
use citadel_sql::Connection;

let db = DatabaseBuilder::new("my.db")
    .passphrase(b"secret")
    .argon2_profile(Argon2Profile::Interactive)
    .create()?;

let mut conn = Connection::open(&db)?;
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")?;
conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
let result = conn.query("SELECT * FROM users;")?;
```
