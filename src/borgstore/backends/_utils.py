"""
Utilities for backend implementations.
"""

from typing import Tuple, Optional


def make_range_header(offset: int, size: Optional[int] = None, total_size: Optional[int] = None) -> Optional[str]:
    """
    Generate a standards compliant HTTP Range header.

    :param offset: offset in bytes. If negative, it is counted from the end of the file.
    :param size: number of bytes to load. If None, load until the end of the file.
    :param total_size: total size of the file. Required if offset < 0 and size is not None.
    :return: Range header value (e.g., "bytes=0-99") or None if no Range header is needed.
    """
    if offset < 0:
        if size is None:
            return f"bytes={offset}"
        else:
            if total_size is None:
                raise ValueError("total_size is required for negative offset with a specific size")
            start = total_size + offset
            return f"bytes={start}-{start + size - 1}"
    else:
        if size is None:
            return f"bytes={offset}-" if offset > 0 else None
        else:
            return f"bytes={offset}-{offset + size - 1}"


def parse_range_header(range_header: str) -> Tuple[int, Optional[int]]:
    """
    Parse a standards compliant HTTP Range header.
    Only supports "bytes" unit and single range specs.

    :param range_header: Range header value (e.g., "bytes=0-99", "bytes=100-", "bytes=-500").
    :return: A tuple (offset, size). offset is negative for suffix ranges.
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, None

    try:
        range_val = range_header.split("=")[1]
        if range_val.startswith("-"):
            # bytes=-SUFFIX
            return int(range_val), None
        elif "-" in range_val:
            # bytes=OFFSET- or bytes=OFFSET-END
            start_str, end_str = range_val.split("-")
            offset = int(start_str)
            size = None
            if end_str:
                size = int(end_str) - offset + 1
            return offset, size
    except (ValueError, IndexError):
        pass

    return 0, None
