class NodeError(Exception):
    pass


class ResultError(Exception):
    """Base class of all result related errors."""


class ResultIOError(ResultError):
    """Raised when there is an I/O error related to result processing."""
    pass


class ResultDataError(ResultError):
    """Raised when there is an issue with the result data."""
    pass


class ResultNotFoundError(ResultIOError):
    """Raised when a result for a specific node label is not found."""
    pass
