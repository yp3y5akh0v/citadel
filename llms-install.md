# Citadel MCP Server Installation Guide

This guide is designed for AI agents like Cline to install and configure the Citadel
MCP server (`citadeldb-mcp`).

## Overview

`citadeldb-mcp` serves an encrypted, local-first memory region as MCP tools: store,
recall, and cryptographically forget facts across sessions, backed by an embedded
AES-256-encrypted SQL + vector database. Everything runs locally; no cloud, no API
keys.

## Prerequisites

1. Install `uv`, which provides `uvx`: https://docs.astral.sh/uv/getting-started/installation/
2. Choose a strong passphrase for `CITADEL_KEY`. It derives the AES-256 key that
   encrypts the memory; reuse the same value to reopen it later.

## Recommended: semantic embedder + reranker

Without them, recall is keyword-only through a mock embedder. For semantic recall,
pull both models once (models are never downloaded automatically):

```sh
uvx citadeldb-mcp pull e5-large
uvx citadeldb-mcp pull ms-marco-minilm
```

Other embedder names: `e5-large-v2`, `bge-small`, `bge-base`, `bge-large`, `minilm`.

## Configuration

Add this to the MCP settings file (for Cline: `cline_mcp_settings.json`):

```json
{
  "mcpServers": {
    "citadel": {
      "command": "uvx",
      "args": ["citadeldb-mcp", "--db", "memory.cdl", "--embedder", "e5-large", "--reranker", "ms-marco-minilm"],
      "env": { "CITADEL_KEY": "choose-a-strong-passphrase" },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

- `CITADEL_KEY` (required): the encryption passphrase.
- `--db` (required): path to the encrypted region file, created on first run. Use an
  absolute path to keep it stable.
- `--embedder e5-large`: semantic recall. Omit it (and skip the pulls) for
  keyword-only recall.
- `--reranker ms-marco-minilm`: cross-encoder reranking for higher recall precision.
  Requires the pull above.

## Tools

- `mem_remember` / `mem_remember_batch` - store atoms (payload, importance, TTL, immutability)
- `mem_recall` - hybrid retrieval (vector + keyword + recency + importance)
- `mem_fetch` - deterministic listing of a kind (no embedding)
- `mem_update` - replace a stored atom's payload in place
- `mem_forget` - forget atoms by id and return a verifiable erasure receipt
- `mem_evict` - selective forgetting by policy
- `mem_link` / `mem_edges` - build and read the typed memory graph
- `mem_evolve` - recompute an atom's neighbor links and score
- `mem_profile` / `mem_summarize` - inspect what the memory knows about a query
- `mem_verify` - re-authenticate atoms off disk (per-atom integrity verdict)

## Verify

Store a fact with `mem_remember`, then retrieve it with `mem_recall` in a new session.
With `e5-large` enabled, a semantically related query (not an exact keyword) returns
the stored fact.

## Troubleshooting

- `uvx: command not found` - install `uv` (see Prerequisites) and reopen the terminal.
- Server exits immediately - `CITADEL_KEY` is unset or empty; set it under `env`.
- Recall behaves like plain keyword search - the `e5-large` model was not pulled, or
  the `--embedder e5-large` flag is missing. Run `uvx citadeldb-mcp pull e5-large`.
- `--reranker` errors - the `ms-marco-minilm` model was not pulled; run
  `uvx citadeldb-mcp pull ms-marco-minilm`.
