"""
Nest / un-nest names to address directory scalability issues and deal with suffix of deleted items.

Many directory implementations can't cope well with gazillions of entries, so
we introduce intermediate directories to lower the amount of entries per directory.

The name is expected to have the key as the last element, like:

    name = "namespace/0123456789abcdef"  # often, the key is hex(hash(content))

As we can have a huge amount of keys, we could nest 2 levels deep:

    nested_name = nest(name, 2)
    nested_name == "namespace/01/23/0123456789abcdef"

Note that the final element is the **full** key - we assume that this is better to deal with in case
of errors (like a fs issue and stuff being pushed to lost+found) and also easier to deal with (e.g. the
directory listing directly gives keys without needing to reassemble the full key from parent dirs and
partial key). Also, a sorted directory list would be same order as a sorted key list.

    name = unnest(nested_name, namespace="namespace")  # namespace with a final slash is also supported
    name == "namespace/0123456789abcdef"

Notes:
- it works the same way without a namespace, but guess one always wants to use a namespace.
- always use nest / unnest, even if levels == 0 are desired as it also does some checks and
  cares for adding / removing a suffix.
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
