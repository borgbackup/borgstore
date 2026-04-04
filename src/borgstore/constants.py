"""Constants used by BorgStore."""

# Namespace to pass to list() for the storage root:
ROOTNS = ""

# Filename suffixes used for special purposes
TMP_SUFFIX = ".tmp"  # Temporary file while being uploaded/written
DEL_SUFFIX = ".del"  # "Soft-deleted" item; can be undeleted
HID_SUFFIX = ".hid"  # Hidden internal file, not accessible by users

# Maximum name length (not precise; suffixes might be added!)
MAX_NAME_LENGTH = 100  # Being rather conservative here to improve portability between backends and platforms

# Quota tracking
QUOTA_STORE_NAME = "quota.hid"  # Hidden file storing current quota usage
QUOTA_PERSIST_DELTA = 10 * 1000 * 1000  # Persist quota if usage changed by at least 10MB
QUOTA_PERSIST_INTERVAL = 300  # Persist quota if at least 5 minutes have elapsed
