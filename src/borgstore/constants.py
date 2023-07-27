"""some constant definitions"""

# namespace that needs to be given to list from the root of the storage:
ROOTNS = ""

# filename prefixes / suffixes used for special purposes
TEMP_PREFIX = "temp-"  # temp file while being uploaded / written
DEL_SUFFIX = ".del"  # "soft deleted" item, undelete possible

# max name length (not precise, suffixes might be added!)
MAX_NAME_LENGTH = 100  # being rather conservative here to improve portability between backends and platforms
