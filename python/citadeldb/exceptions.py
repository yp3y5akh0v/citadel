"""Typed exception hierarchy. Every error derives from ``CitadelError``."""

from citadeldb._core import (
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

__all__ = [
    "CitadelError",
    "EncryptionError",
    "IntegrityError",
    "OperationalError",
    "ProgrammingError",
    "DataError",
    "NotSupportedError",
    "LlmError",
    "AgentError",
]
