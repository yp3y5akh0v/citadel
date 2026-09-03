"""MCP server binding: drive ``python -m citadeldb.mcp`` over JSON-RPC stdio."""

import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TextIO

import pytest
from citadeldb import mcp


@dataclass
class _RpcClient:
    proc: subprocess.Popen
    responses: queue.Queue
    stderr: TextIO

    def notify(self, obj):
        self.proc.stdin.write(json.dumps(obj) + "\n")
        self.proc.stdin.flush()

    def request(self, obj, timeout=15):
        self.notify(obj)
        return self.receive(timeout)

    def receive(self, timeout=15):
        try:
            line = self.responses.get(timeout=timeout)
        except queue.Empty:
            reason = f"Timed out waiting for MCP response after {timeout:g}s"
        else:
            if line is not None:
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    reason = f"Invalid MCP JSON response: {line!r}"
            else:
                reason = "MCP server closed stdout before responding"

        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=5)
        self.stderr.seek(0)
        pytest.fail(
            f"{reason}\nexit code: {self.proc.returncode}\nstderr:\n{self.stderr.read()}",
            pytrace=False,
        )


@contextmanager
def _rpc_process(args, *, env=None, shutdown_timeout=5):
    with tempfile.TemporaryFile(
        mode="w+t", encoding="utf-8", errors="replace"
    ) as stderr:
        proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            env=env,
            text=True,
            encoding="utf-8",
        )
        responses = queue.Queue()

        def read_stdout():
            try:
                with proc.stdout:
                    for line in proc.stdout:
                        responses.put(line)
            finally:
                responses.put(None)

        reader = threading.Thread(target=read_stdout, daemon=True)
        reader.start()
        try:
            yield _RpcClient(proc, responses, stderr)
        finally:
            body_failed = sys.exc_info()[0] is not None
            shutdown_timed_out = False
            try:
                try:
                    proc.stdin.close()
                except BrokenPipeError:
                    pass
                try:
                    proc.wait(timeout=shutdown_timeout)
                except subprocess.TimeoutExpired:
                    shutdown_timed_out = True
                    proc.kill()
                    proc.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                if not body_failed:
                    raise
            finally:
                reader.join(timeout=1)
            if shutdown_timed_out and not body_failed:
                stderr.seek(0)
                pytest.fail(
                    f"MCP server did not exit after stdin closed\nstderr:\n{stderr.read()}",
                    pytrace=False,
                )


def test_mcp_server_initialize_and_tools_list(tmp_path):
    db = str(tmp_path / "mcp.cdl")
    env = dict(os.environ, CITADEL_KEY="test-pass")
    with _rpc_process(
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
            "--embedder",
            "mock",
        ],
        env=env,
    ) as client:
        init = client.request(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "pytest", "version": "1"},
                },
            },
        )
        assert init["id"] == 1
        assert "result" in init, init
        assert "serverInfo" in init["result"]
        assert init["result"]["protocolVersion"] == "2025-11-25", init

        premature = client.request(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        assert premature["id"] == 2, premature
        assert "error" in premature, premature
        assert premature["error"]["code"] == -32600, premature
        assert "notifications/initialized" in premature["error"]["message"], premature

        client.notify({"jsonrpc": "2.0", "method": "notifications/initialized"})
        tools = client.request(
            {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}}
        )
        assert tools["id"] == 3, tools
        assert "result" in tools, tools
        names = {t["name"] for t in tools["result"]["tools"]}
        assert names, "the server advertises tools"
        assert any("recall" in n for n in names), names


def test_mcp_response_timeout_reaps_process_and_reports_stderr():
    script = (
        "import sys, time; "
        "sys.stderr.write('x' * 262144 + '\\ntimeout diagnostic\\n'); "
        "sys.stderr.flush(); "
        "print('{\"ready\": true}', flush=True); "
        "sys.stdin.readline(); time.sleep(60)"
    )
    with _rpc_process([sys.executable, "-u", "-c", script]) as client:
        assert client.receive() == {"ready": True}
        started = time.monotonic()
        with pytest.raises(
            pytest.fail.Exception, match="Timed out.*MCP response"
        ) as exc:
            client.request({"jsonrpc": "2.0", "id": 1, "method": "ping"}, timeout=0.1)
        assert time.monotonic() - started < 5
        assert "timeout diagnostic" in str(exc.value)
        assert client.proc.returncode is not None


def test_mcp_response_eof_reports_stderr():
    script = "import sys; sys.stderr.buffer.write(b'startup diagnostic \\xff\\n'); sys.exit(7)"
    with _rpc_process([sys.executable, "-u", "-c", script]) as client:
        with pytest.raises(pytest.fail.Exception, match="closed stdout") as exc:
            client.receive()
        assert "startup diagnostic" in str(exc.value)
        assert client.proc.returncode is not None


@pytest.mark.parametrize("body_fails", [False, True])
def test_mcp_shutdown_timeout_preserves_original_failure(body_fails):
    script = (
        "import sys, time; print('{\"ready\": true}', flush=True); "
        "sys.stdin.read(); time.sleep(60)"
    )
    expected = "original failure" if body_fails else "did not exit after stdin closed"
    with (
        pytest.raises(pytest.fail.Exception, match=expected),
        _rpc_process(
            [sys.executable, "-u", "-c", script], shutdown_timeout=0.1
        ) as client,
    ):
        assert client.receive() == {"ready": True}
        if body_fails:
            pytest.fail("original failure")
    assert client.proc.returncode is not None


def test_mcp_missing_db_errors():
    proc = subprocess.run(
        [sys.executable, "-m", "citadeldb.mcp"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CITADEL_KEY="x"),
        timeout=15,
        check=False,
    )
    assert proc.returncode != 0
    assert "--db" in proc.stderr


def test_mcp_missing_embedder_errors_without_creating_a_database(tmp_path):
    db = tmp_path / "missing-embedder.cdl"
    proc = subprocess.run(
        [sys.executable, "-m", "citadeldb.mcp", "--db", str(db)],
        capture_output=True,
        text=True,
        env=dict(os.environ, CITADEL_KEY="x"),
        timeout=15,
        check=False,
    )
    assert proc.returncode != 0
    assert "--embedder <name> is required" in proc.stderr
    assert not db.exists()


def test_python_serve_requires_and_forwards_an_explicit_embedder(monkeypatch):
    seen = []
    monkeypatch.setattr(mcp, "mcp_main", lambda argv: seen.extend(argv) or 0)

    with pytest.raises(TypeError, match="embedder"):
        mcp.serve("memory.cdl")
    assert mcp.serve("memory.cdl", embedder="mock") == 0
    assert seen[-2:] == ["--embedder", "mock"]
