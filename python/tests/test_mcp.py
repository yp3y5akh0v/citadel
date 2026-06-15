"""MCP server binding: drive ``python -m citadeldb.mcp`` over JSON-RPC stdio."""

import json
import os
import subprocess
import sys


def _send(proc, obj):
    proc.stdin.write(json.dumps(obj) + "\n")
    proc.stdin.flush()
    return json.loads(proc.stdout.readline())


def test_mcp_server_initialize_and_tools_list(tmp_path):
    db = str(tmp_path / "mcp.cdl")
    env = dict(os.environ, CITADEL_KEY="test-pass")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "citadeldb.mcp",
            "--db",
            db,
            "--region",
            "default",
            "--region-mode",
            "plaintext",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )
    try:
        init = _send(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "pytest", "version": "1"},
                },
            },
        )
        assert init["id"] == 1
        assert "result" in init, init
        assert "serverInfo" in init["result"]

        tools = _send(
            proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        assert tools["id"] == 2
        names = {t["name"] for t in tools["result"]["tools"]}
        assert names, "the server advertises tools"
        assert any("recall" in n for n in names), names
    finally:
        proc.stdin.close()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise


def test_mcp_missing_db_errors():
    proc = subprocess.run(
        [sys.executable, "-m", "citadeldb.mcp"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CITADEL_KEY="x"),
    )
    assert proc.returncode != 0
    assert "--db" in proc.stderr
