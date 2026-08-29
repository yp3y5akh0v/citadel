"""MCP server: expose a citadel memory region as MCP tools over JSON-RPC stdio.

This wheel is built without the Candle embedder, so callers must explicitly pass
``embedder="mock"`` for keyword-only recall. Use the standalone
``citadeldb-mcp`` package (``uvx citadeldb-mcp``) for semantic recall. The
memory API accepts your own embeddings in either build.
"""

import sys
from collections.abc import Sequence

from citadeldb._core import mcp_main

__all__ = ["main", "serve", "pull"]


def main(argv: Sequence[str] | None = None) -> int:
    """Run the MCP CLI; returns the process exit code."""
    args = list(sys.argv[1:]) if argv is None else list(argv)
    return mcp_main(args)


def serve(
    db: str,
    *,
    embedder: str,
    region: str = "default",
    encrypted: bool = True,
    model_dir: str | None = None,
    models_dir: str | None = None,
    reranker: str | None = None,
    reranker_dir: str | None = None,
) -> int:
    """Serve with the explicitly selected embedder until the client disconnects."""
    args = [
        "--db", db,
        "--region", region,
        "--region-mode", "encrypted" if encrypted else "plaintext",
        "--embedder", embedder,
    ]
    if model_dir is not None:
        args += ["--model-dir", model_dir]
    if models_dir is not None:
        args += ["--models-dir", models_dir]
    if reranker is not None:
        args += ["--reranker", reranker]
    if reranker_dir is not None:
        args += ["--reranker-dir", reranker_dir]
    return mcp_main(args)


def pull(name: str, models_dir: str | None = None) -> int:
    """Download a public model into the local cache (needs a candle-embed build)."""
    args = ["pull", name]
    if models_dir is not None:
        args += ["--models-dir", models_dir]
    return mcp_main(args)


if __name__ == "__main__":
    sys.exit(main())
