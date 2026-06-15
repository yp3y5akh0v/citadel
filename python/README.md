# citadeldb

Encrypted-first embedded database for Python: SQL (with JSON and full-text
search), filtered vector search, a memory engine, and an agent runtime, all over a
single-file store encrypted with AES-256-CTR + HMAC-SHA256.

## Install

```
pip install citadeldb
```

The only runtime dependency is NumPy; embeddings are bring-your-own.

```python
import citadeldb

db = citadeldb.connect("app.cdl", key="passphrase", create=True)
db.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)")
db.execute("INSERT INTO notes VALUES (1, $1)", ["hello"])
print(db.query("SELECT body FROM notes").to_dicts())  # [{'body': 'hello'}]
```

The Python distribution of [Citadel](https://github.com/yp3y5akh0v/citadel). Full
documentation and the SQL / vector / memory / agent APIs are at
[citadeldb.dev](https://citadeldb.dev) and in the main repository.

## License

MIT OR Apache-2.0
