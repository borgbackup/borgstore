"""
Tests for the hashing utils (hashlib algorithms plus optional blake3).
"""

import hashlib
import io

import pytest

from borgstore.utils import hashing

blake3_is_available = hashing.blake3 is not None


def test_new_hashlib():
    data = b"hash me"
    assert hashing.new("sha256", data).hexdigest() == hashlib.sha256(data).hexdigest()
    h = hashing.new("sha256")
    h.update(data)
    assert h.hexdigest() == hashlib.sha256(data).hexdigest()


def test_file_digest_hashlib():
    data = b"hash me"
    assert hashing.file_digest(io.BytesIO(data), "sha256").hexdigest() == hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("algorithm", ["invalid_algo", "", None, 42])
def test_unsupported_algorithm(algorithm):
    with pytest.raises(ValueError, match="Unsupported hash algorithm"):
        hashing.new(algorithm)
    with pytest.raises(ValueError, match="Unsupported hash algorithm"):
        hashing.file_digest(io.BytesIO(b"hash me"), algorithm)


@pytest.mark.skipif(not blake3_is_available, reason="blake3 package is not installed")
def test_blake3():
    data = b"hash me"
    expected = hashing.blake3(data).hexdigest()
    # known value, so we notice if the algorithm/output encoding ever changes
    assert expected == "e02b7e4520e277d4ef287f09c74fec4dd1df095b8b41a4d61dd4ed1589c4a281"
    assert hashing.new("blake3", data).hexdigest() == expected
    h = hashing.new("blake3")
    h.update(data)
    assert h.hexdigest() == expected
    assert hashing.file_digest(io.BytesIO(data), "blake3").hexdigest() == expected


@pytest.mark.skipif(blake3_is_available, reason="blake3 package is installed")
def test_blake3_not_available():
    with pytest.raises(ValueError, match="Unsupported hash algorithm: blake3"):
        hashing.new("blake3")
    with pytest.raises(ValueError, match="Unsupported hash algorithm: blake3"):
        hashing.file_digest(io.BytesIO(b"hash me"), "blake3")
