"""Exception Class."""


class MaxLimitException(Exception):
    """Exception raised for maximum skip limit exceeded.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        """Initialize."""
        self.message = message


class IncompleteTransactionError(Exception):
    """Exception raised when invalid response received due to network issue.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        """Initialize."""
        self.message = message


class ForbiddenError(Exception):
    """Exception raised when 403 received.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        """Initialize."""
        self.message = message
