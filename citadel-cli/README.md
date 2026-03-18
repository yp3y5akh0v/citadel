# citadeldb-cli

Interactive SQL shell for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded database engine.

## Install

```
cargo install citadeldb-cli
```

## Usage

```bash
# Create and open a database
citadel --create my.db

# Run SQL
citadel> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
citadel> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
citadel> SELECT * FROM users;
```

Supports 27 dot-commands (`.backup`, `.verify`, `.stats`, `.sync`, `.keygen`, etc.), tab completion, syntax highlighting, and multiple output modes.

## License

MIT OR Apache-2.0
