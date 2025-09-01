"""Constants used by BorgStore."""

# Namespace to pass to list() for the storage root:
ROOTNS = ""

# Filename suffixes used for special purposes
TMP_SUFFIX = ".tmp"  # Temporary file while being uploaded/written
DEL_SUFFIX = ".del"  # "Soft-deleted" item; can be undeleted

# Maximum name length (not precise; suffixes might be added!)
MAX_NAME_LENGTH = 100  # Being rather conservative here to improve portability between backends and platforms
