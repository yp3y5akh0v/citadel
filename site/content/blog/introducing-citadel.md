+++
title = "Introducing Citadel"
date = 2026-03-15
description = "Why I built an encrypted-first embedded database engine in Rust."
authors = ["Yuriy Peysakhov"]

[taxonomies]
tags = ["announcement"]
+++

Citadel is an embedded database engine where every page is encrypted at rest. There is no "unencrypted mode" - the database file is always opaque.

## Why?

Existing embedded databases treat encryption as an afterthought. SQLite has SEE (paid, closed-source). Most key-value stores have no encryption at all. If you're building for mobile, IoT, or any environment where the device might be lost or stolen, you need encryption by default - not as an optional plugin.

## How it works

Citadel uses a 3-tier key hierarchy:

1. Your **passphrase** derives a master key via Argon2id (or PBKDF2 in FIPS mode)
2. The master key unwraps a **root encryption key** via AES Key Wrap
3. The root key derives per-purpose **data encryption** and **MAC keys** via HKDF

Every page on disk is 8,208 bytes: a 16-byte IV, 8,160 bytes of ciphertext, and a 32-byte HMAC. The MAC is verified before decryption - because AES-CTR is malleable, we never decrypt unauthenticated data.

## No WAL

Most databases use a write-ahead log for crash recovery. Citadel uses **shadow paging** instead: dirty pages are written to new locations (copy-on-write), and a single "god byte" flip atomically switches which commit slot is active. This means:

- No WAL to manage, truncate, or checkpoint
- Instant crash recovery (just read the god byte)
- Every reader sees a consistent snapshot

## What's next

Citadel already supports full SQL (JOINs, subqueries, aggregates, indexes), ACID transactions, P2P encrypted sync, C FFI, and WebAssembly. Head over to the [playground](/demo/) to try it in your browser, or check out the <a href="https://github.com/yp3y5akh0v/citadel" target="_blank" rel="noopener">GitHub repo</a> for the source.
