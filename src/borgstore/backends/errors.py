"""
Generic exception classes used by all backends.
"""


class BackendError(Exception):
    """Base class for exceptions in this module."""


class BackendURLInvalid(BackendError):
    """Raised when trying to create a store using an invalid backend URL."""


class NoBackendGiven(BackendError):
    """Raised when trying to create a store and not giving a backend nor a URL."""


class BackendAlreadyExists(BackendError):
    """Raised when a backend already exists."""


class BackendDoesNotExist(BackendError):
    """Raised when a backend does not exist."""


class BackendMustNotBeOpen(BackendError):
    """Backend must not be open."""


class BackendMustBeOpen(BackendError):
    """Backend must be open."""


class ObjectNotFound(BackendError):
    """Object not found."""


class PermissionDenied(BackendError):
    """Permission denied for the requested operation."""
