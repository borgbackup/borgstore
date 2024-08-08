"""
Testing for the nesting code.
"""
import pytest

from borgstore.constants import ROOTNS
from borgstore.utils.nesting import nest, unnest, split_key


@pytest.mark.parametrize("name,base,key", [("12345678", None, "12345678"), ("data/12345678", "data", "12345678")])
def test_split_key(name, base, key):
    assert split_key(name) == (base, key)


@pytest.mark.parametrize(
    "key,levels,deleted,nested_key",
    [
        ("12345678", 0, False, "12345678"),
        ("12345678", 1, False, "12/12345678"),
        ("12345678", 2, False, "12/34/12345678"),
        ("12345678", 3, False, "12/34/56/12345678"),
        ("12345678", 3, True, "12/34/56/12345678.del"),
        ("data/12345678", 0, False, "data/12345678"),
        ("data/12345678", 1, False, "data/12/12345678"),
        ("data/12345678", 2, False, "data/12/34/12345678"),
        ("data/12345678", 3, False, "data/12/34/56/12345678"),
        ("data/12345678", 3, True, "data/12/34/56/12345678.del"),
    ],
)
def test_nest(key, levels, deleted, nested_key):
    suffix = ".del" if deleted else None
    assert nest(key, levels, add_suffix=suffix) == nested_key


@pytest.mark.parametrize(
    "key,base,deleted,nested_key",
    [
        ("12345678", ROOTNS, False, "12345678"),
        ("12345678", ROOTNS, False, "12/12345678"),
        ("12345678", ROOTNS, False, "12/34/12345678"),
        ("12345678", ROOTNS, False, "12/34/56/12345678"),
        ("12345678", ROOTNS, True, "12/34/56/12345678.del"),
        ("data/12345678", "data", False, "data/12345678"),
        ("data/12345678", "data", False, "data/12/12345678"),
        ("data/12345678", "data", False, "data/12/34/12345678"),
        ("data/12345678", "data", False, "data/12/34/56/12345678"),
        ("data/12345678", "data", True, "data/12/34/56/12345678.del"),
    ],
)
def test_unnest(key, base, deleted, nested_key):
    suffix = ".del" if deleted else None
    assert unnest(nested_key, base, remove_suffix=suffix) == key


@pytest.mark.parametrize(
    "key,base,nested_key",
    [
        # does not start with base
        ("data/12345678", "data", "data_xxx/12/12345678"),
        ("data/12345678", "data", "dat/12/34/12345678"),
    ],
)
def test_unnest_invalid(key, base, nested_key):
    with pytest.raises(ValueError):
        unnest(nested_key, base)
