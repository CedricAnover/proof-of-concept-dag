class NodeError(Exception):
    pass


class DependencyError(NodeError):
    """Raised when a node's dependency fails."""
    pass


class NodeExecutionError(NodeError):
    """Raised when a node encounters an error during execution."""
    pass


class TimeoutError(NodeExecutionError):
    """Raised when a node execution exceeds its timeout."""
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


class ConduitError(Exception):
    pass
