"""
Generic exception classes used by all backends.
"""


class BackendError(Exception):
    """Base class for exceptions in this module."""


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
