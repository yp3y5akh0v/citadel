# citadeldb-cli

Interactive SQL shell for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine.

## Install

```sh
cargo install --locked citadeldb-cli
```

Building from source requires Rust 1.95 or later. Prebuilt CLI archives are listed on
the [downloads page](https://citadeldb.dev/download/).

## Usage

```bash
# Create and open a database
citadel --create my.db

# Run SQL
citadel> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
citadel> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
citadel> SELECT * FROM users;
```

The interactive shell prompts for the passphrase when it is omitted. Use `.help` to list the
27 dot-commands, including `.backup`, `.verify`, `.audit`, `.rekey`, `.stats`, and `.sync`.
It supports tab completion, syntax highlighting, and box, table, CSV, JSON, and line
output modes.

`.upgrade` migrates legacy commit slots to the authenticated format. This is a one-way
operation; make a backup first.

## License

Apache-2.0
