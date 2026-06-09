# mini-SWE benchmark tasks

Each subdirectory is a self-contained, standalone Rust crate with a single
planted bug. The agent is given the crate (minus the held-out test and the gold
fix), must fix the bug in `src/`, and is graded by an independent `cargo test`.

## Layout

```
<task-id>/
  Cargo.toml          standalone crate (its own [workspace]; no dependencies)
  src/lib.rs          the buggy source - the ONLY thing the agent may write
  tests/visible.rs    a FAIL_TO_PASS test the agent SEES failing
  tests/pass_to_pass.rs  adjacent behavior that already passes on the buggy crate
  hidden/hidden.rs    held-out FAIL_TO_PASS test (stronger; the agent never sees it)
  gold/lib.rs         the correct src/lib.rs (oracle; used by the self-test only)
  task.json           manifest (see below)
```

At run time the harness copies `Cargo.toml`, `src/`, and the two `tests/` files
into a tempdir; `hidden/`, `gold/`, and `task.json` are left behind. Writes are
sandboxed to `src/`, commands to `cargo`. After the agent finishes, the harness
restores the pristine tests, injects `hidden/hidden.rs` as `tests/hidden.rs`, and
re-runs `cargo test` - so editing a test can never change the grade.

## Manifest (`task.json`)

```json
{
  "id": "<dir name>",
  "difficulty": "easy | medium | hard",
  "symptom": "one-line description of the WRONG behavior (never the fix)",
  "fail_to_pass": ["visible", "hidden"],
  "pass_to_pass": ["pass_to_pass"]
}
```

`fail_to_pass`/`pass_to_pass` are `cargo test --test <name>` binary names.

## Invariants (enforced by `gold_self_test_all_tasks`)

1. The buggy crate **compiles** (so failures are at runtime, not build time).
2. Gold fix applied -> every `fail_to_pass` and `pass_to_pass` test is green.
3. Pristine buggy -> at least one `fail_to_pass` **fails**, and every
   `pass_to_pass` **passes** (a valid regression guard tests behavior the bug
   does not break).

Build artifacts (`target/`, `Cargo.lock`) are gitignored; the benchmark builds
each task in a tempdir, so they never appear from a normal run.
