"""
Hashing support: all algorithms supported by hashlib, plus "blake3".

blake3 is not part of hashlib, it is provided by the optional "blake3" package
(pip install 'borgstore[blake3]'). It is usually much faster than the sha2
family, so it is an attractive choice for hashing stored object contents.
"""

import hashlib
from typing import Any, Callable, Optional

blake3: Optional[Callable[..., Any]]
try:
    from blake3 import blake3  # type: ignore[import-not-found,no-redef]
except ImportError:
    blake3 = None

BLAKE3 = "blake3"


def _blake3_factory() -> Callable[..., Any]:
    """return the blake3 hash object factory (or raise, if it is not available)"""
    if blake3 is None:
        raise ValueError(f"Unsupported hash algorithm: {BLAKE3} (pip install 'borgstore[blake3]')")
    return blake3


def new(algorithm: str, data: bytes = b"") -> Any:
    """like hashlib.new, but also supports "blake3"."""
    if algorithm == BLAKE3:
        return _blake3_factory()(data)
    try:
        return hashlib.new(algorithm, data)
    except (ValueError, TypeError):
        raise ValueError(f"Unsupported hash algorithm: {algorithm}") from None


def file_digest(fileobj, algorithm: str) -> Any:
    """like hashlib.file_digest, but also supports "blake3"."""
    if algorithm == BLAKE3:
        return hashlib.file_digest(fileobj, _blake3_factory())
    try:
        return hashlib.file_digest(fileobj, algorithm)
    except (ValueError, TypeError):
        raise ValueError(f"Unsupported hash algorithm: {algorithm}") from None
