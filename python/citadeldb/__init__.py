"""Citadel: encrypted-first embedded database for Python."""

import os
import sys

# CUDA runtime DLLs aren't found via PATH on Python 3.8+; add the bin dir first.
if sys.platform == "win32":
    _cuda_path = os.environ.get("CUDA_PATH")
    if _cuda_path:
        _cuda_bin = os.path.join(_cuda_path, "bin")
        if os.path.isdir(_cuda_bin):
            os.add_dll_directory(_cuda_bin)

from citadeldb import agent, exceptions, mcp, memory, vector
from citadeldb._core import Database, DatabaseOptions, QueryResult, __version__, connect
from citadeldb.agent import Agent, BeliefGraph, Goal, LLMClient
from citadeldb.exceptions import (
    AgentError,
    CitadelError,
    DataError,
    EncryptionError,
    IntegrityError,
    LlmError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from citadeldb.memory import (
    CandleEmbedder,
    CrossEncoder,
    EvictionPolicy,
    MockEmbedder,
    MockReranker,
    RecallOptions,
)
from citadeldb.vector import Filter, VectorIndex

__all__ = [
    "connect",
    "Database",
    "DatabaseOptions",
    "QueryResult",
    "VectorIndex",
    "Filter",
    "MockEmbedder",
    "MockReranker",
    "CandleEmbedder",
    "CrossEncoder",
    "EvictionPolicy",
    "RecallOptions",
    "Agent",
    "LLMClient",
    "Goal",
    "BeliefGraph",
    "vector",
    "memory",
    "agent",
    "mcp",
    "exceptions",
    "CitadelError",
    "EncryptionError",
    "IntegrityError",
    "OperationalError",
    "ProgrammingError",
    "DataError",
    "NotSupportedError",
    "LlmError",
    "AgentError",
    "__version__",
]
