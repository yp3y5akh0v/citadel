"""MCP server: expose a citadel memory region as MCP tools over JSON-RPC stdio."""

import sys

from citadeldb._core import mcp_main

__all__ = ["main", "serve", "pull"]


def main(argv=None):
    """Run the MCP CLI; returns the process exit code."""
    args = list(sys.argv[1:]) if argv is None else list(argv)
    return mcp_main(args)


def serve(
    db,
    *,
    region="default",
    encrypted=True,
    embedder="mock",
    model_dir=None,
    models_dir=None,
    reranker=None,
    reranker_dir=None,
):
    """Serve a region over MCP (blocks until the client disconnects)."""
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


def pull(name, models_dir=None):
    """Download a public model into the local cache (needs a candle-embed build)."""
    args = ["pull", name]
    if models_dir is not None:
        args += ["--models-dir", models_dir]
    return mcp_main(args)


if __name__ == "__main__":
    sys.exit(main())
