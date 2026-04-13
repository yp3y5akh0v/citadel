+++
title = "Introducing Citadel"
date = 2026-04-11
description = "Why I built an encrypted-first embedded database engine in Rust."
authors = ["Yuriy Peysakhov"]

[taxonomies]
tags = ["announcement"]
+++

Citadel is an embedded database engine where every page is encrypted at rest. There is no "unencrypted mode" - the database file is always opaque.

## Why?

SQLite doesn't ship with encryption. The official extension (SEE) is paid and closed-source. Most key-value stores skip encryption entirely. If your app runs on a phone, a Raspberry Pi, or anything that might get lost or stolen, you're on your own.

I wanted a database that encrypts by default, not one where I bolt encryption on after the fact and hope nothing leaks.

## How it works

Citadel uses a 3-tier key hierarchy:

1. Your **passphrase** derives a master key via Argon2id (or PBKDF2 for FIPS)
2. The master key unwraps a **root encryption key** via AES Key Wrap
3. The root key derives per-purpose **data encryption** and **MAC keys** via HKDF

Every page on disk is 8,208 bytes: a 16-byte IV, 8,160 bytes of AES-256-CTR ciphertext, and a 32-byte HMAC-SHA256. The MAC is verified before decryption - AES-CTR is malleable, so we never decrypt unauthenticated data.

## No WAL

Most databases use a write-ahead log for crash recovery. Citadel uses **shadow paging**: dirty pages are written to new locations (copy-on-write), and a single "god byte" flip atomically switches the active commit slot. This means no WAL to checkpoint, instant crash recovery, and every reader gets a consistent snapshot for free.

## Faster than unencrypted SQLite

Citadel beats SQLite in all 13 head-to-head benchmarks, even though every page goes through AES-256-CTR + HMAC-SHA256 and SQLite runs without any encryption.

Full results are in the <a href="https://github.com/yp3y5akh0v/citadel#benchmarks" target="_blank" rel="noopener">README</a>.

## What it supports

- **SQL** - JOINs, subqueries, CTEs (recursive too), UNION/INTERSECT/EXCEPT, window functions, aggregates, indexes, constraints (DEFAULT, CHECK, FOREIGN KEY), ALTER TABLE, prepared statements
- **ACID transactions** - snapshot isolation, concurrent readers, no WAL
- **P2P encrypted sync** - Merkle-based diffing over Noise protocol
- **Cross-platform** - C FFI, WebAssembly, CLI with tab completion

Head over to the [playground](@/demo/_index.md) to try it in your browser, or check out the <a href="https://github.com/yp3y5akh0v/citadel" target="_blank" rel="noopener">source on GitHub</a>.
