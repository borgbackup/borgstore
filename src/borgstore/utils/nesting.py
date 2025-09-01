"""
Nest/un-nest names to address directory scalability issues and handle the suffix for deleted items.

Many filesystem directory implementations do not cope well with extremely large numbers of entries, so
we introduce intermediate directories to reduce the number of entries per directory.

The name is expected to have the key as the last element, for example:

    name = "namespace/0123456789abcdef"  # often, the key is hex(hash(content))

As we can have a huge number of keys, we could nest 2 levels deep:

    nested_name = nest(name, 2)
    nested_name == "namespace/01/23/0123456789abcdef"

Note that the final element is the full key — this is better to deal with in case
of errors (for example, a filesystem issue and items being pushed to lost+found) and also easier to handle (e.g., a
directory listing directly yields keys without needing to reassemble the full key from parent directories and
partial keys). Also, a sorted directory listing has the same order as a sorted key list.

    name = unnest(nested_name, namespace="namespace")  # a namespace with a final slash is also supported
    name == "namespace/0123456789abcdef"

Notes:
- It works the same way without a namespace, but we recommend always using a namespace.
- Always use nest/unnest, even if levels == 0 are desired, as they also perform some checks and
  handle adding/removing a suffix.
"""

from typing import Optional


def split_key(name: str) -> tuple[Optional[str], str]:
    namespace_key = name.rsplit("/", 1)
    if len(namespace_key) == 2:
        namespace, key = namespace_key
    else:  # == 1 (no slash in name)
        namespace, key = None, name
    return namespace, key


def nest(name: str, levels: int, *, add_suffix: Optional[str] = None) -> str:
    """namespace/12345678 --2 levels--> namespace/12/34/12345678"""
    if levels > 0:
        namespace, key = split_key(name)
        parts = [key[2 * level : 2 * level + 2] for level in range(levels)]
        parts.append(key)
        if namespace is not None:
            parts.insert(0, namespace)
        name = "/".join(parts)
    return (name + add_suffix) if add_suffix else name


def unnest(name: str, namespace: str, *, remove_suffix: Optional[str] = None) -> str:
    """namespace/12/34/12345678 --namespace=namespace--> namespace/12345678"""
    if namespace:
        if not namespace.endswith("/"):
            namespace += "/"
        if not name.startswith(namespace):
            raise ValueError(f"name {name} does not start with namespace {namespace}")
        name = name.removeprefix(namespace)
    key = name.rsplit("/", 1)[-1]
    if remove_suffix:
        key = key.removesuffix(remove_suffix)
    return namespace + key
