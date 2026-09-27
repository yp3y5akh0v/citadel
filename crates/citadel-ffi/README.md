# citadeldb-ffi

C FFI bindings for the [Citadel](https://github.com/yp3y5akh0v/citadel) encrypted embedded
database engine. Exposes a panic-safe C API with an auto-generated `citadel.h` (cbindgen).

This crate is part of the Citadel workspace and is not published to crates.io; build it from
the [Citadel](https://github.com/yp3y5akh0v/citadel) repository.

## Errors and recovery

Check each returned error code; `citadel_last_error_message()` provides details.
`citadel_write_commit()` consumes the transaction handle on both success and failure.
Do not reuse or abort that pointer after calling commit.

A final sync failure can leave the commit's durability unknown. Subsequent writes
return `CITADEL_ERROR_T_REOPEN_REQUIRED` (`-22`). End retained transactions and close
SQL connections and the database before reopening it. Inspect the recovered state
before retrying the failed operation; an error does not guarantee that the commit
was lost.

## License

Apache-2.0
