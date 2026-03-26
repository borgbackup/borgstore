from borgstore.backends._utils import make_range_header, parse_range_header
import pytest


def test_make_range_header():
    # From start
    assert make_range_header(0) is None
    assert make_range_header(100) == "bytes=100-"
    assert make_range_header(100, 50) == "bytes=100-149"
    assert make_range_header(0, 50) == "bytes=0-49"

    # From end
    assert make_range_header(-100) == "bytes=-100"
    assert make_range_header(-100, 50, 1000) == "bytes=900-949"

    with pytest.raises(ValueError):
        make_range_header(-100, 50)


def test_parse_range_header():
    assert parse_range_header(None) == (0, None)
    assert parse_range_header("") == (0, None)
    assert parse_range_header("invalid") == (0, None)
    assert parse_range_header("bytes=invalid") == (0, None)

    assert parse_range_header("bytes=100-") == (100, None)
    assert parse_range_header("bytes=100-149") == (100, 50)
    assert parse_range_header("bytes=0-49") == (0, 50)

    assert parse_range_header("bytes=-100") == (-100, None)
