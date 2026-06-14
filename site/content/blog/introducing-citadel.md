+++
title = "Introducing Citadel"
date = 2026-06-07
description = "An embedded database that encrypts every page, with a built-in memory engine."
authors = ["Yuriy Peysakhov"]

[taxonomies]
tags = ["announcement"]
+++

Citadel is an embedded database. Every page is encrypted before it reaches disk, so the file is always ciphertext - there is no unencrypted mode. It also includes a memory engine on the same encrypted storage.

## Why encryption first

SQLite has no built-in encryption. The official extension (SEE) is paid and closed-source, and most embedded key-value stores have none at all. That is fine until the device is lost or stolen.

I did not want encryption as a layer on top. In Citadel there is no code path that writes a plaintext page.

## How it works

Citadel derives its keys in three steps:

1. Your **passphrase** derives a master key with Argon2id (or PBKDF2 for FIPS).
2. The master key unwraps a **root key** with AES Key Wrap.
3. The root key derives the **data** and **MAC** keys with HKDF.

Each page on disk is 8,208 bytes: a 16-byte IV, 8,160 bytes of AES-256-CTR ciphertext, and a 32-byte HMAC-SHA256. The MAC is checked before anything is decrypted, so tampered pages are rejected.

## No WAL

Most databases keep a write-ahead log for crash recovery. Citadel does not. Changed pages are written to new locations, and one byte flip switches to the new version. Recovery is immediate, and readers always see a consistent snapshot.

## Faster than unencrypted SQLite

Citadel beats SQLite on all 50 head-to-head benchmarks, even with encryption on every page. The numbers are in the <a href="https://github.com/yp3y5akh0v/citadel#benchmarks" target="_blank" rel="noopener">README</a>.

## The memory engine

Apps and agents keep a lot of long-lived, private context, and you do not want that in plaintext. Citadel's memory layer covers:

- **Typed memory** - `citadel-mem` stores **atoms** in **regions**, linked by typed **edges**. They are stored and encrypted like normal rows.
- **Vector recall** - a `VECTOR(N)` column with a filtered ANN index from <a href="https://github.com/yp3y5akh0v/prism" target="_blank" rel="noopener">PRISM</a>. Recall mixes vector distance, keyword match, and recency, with an optional reranker.
- **MCP server** - `citadel-mcp` serves a memory region over MCP (JSON-RPC on stdio, 13 tools), so Claude Desktop or any MCP client can use it.
- **Forgetting** - to delete data you destroy its key instead of overwriting it. This works per atom, per region, or for the whole store, and returns a receipt. The ciphertext left behind cannot be read.

It uses no LLM to build or search memory - it stores raw turns and recalls with vectors, keywords, and a reranker - yet scores {{ locomo() }}% on the LoCoMo memory benchmark with everything encrypted. With a stronger Gemini 3.5 Flash reader the same encrypted retrieval reaches {{ locomo_gemini() }}% (both 3-run means).

## What it supports

- **SQL** - every join type (including FULL OUTER and LATERAL), recursive CTEs, window functions, JSON/JSONB with Postgres operators, full-text search, triggers, materialized views, and generated columns. Full list on the [features page](/#sql).
- **Vectors and memory** - `VECTOR(N)` with a PRISM ANN index, the `citadel-mem` atom/edge store, and the `citadel-mcp` server.
- **Transactions** - ACID with snapshot isolation and concurrent readers.
- **Sync** - encrypted peer-to-peer diffing over the Noise protocol.
- **Bindings** - Rust, WebAssembly, a C API, and a CLI.

Citadel is a Rust workspace with thousands of tests, all in one encrypted file. Try it in the [playground](@/demo/_index.md), or read the <a href="https://github.com/yp3y5akh0v/citadel" target="_blank" rel="noopener">source</a>.
