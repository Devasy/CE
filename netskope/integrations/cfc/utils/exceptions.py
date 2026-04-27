"""Custom exceptions for the CFC modules."""


class CustomException(Exception):
    """Custom exception class for the CFC module plugins."""

    def __init__(self, message: str, value=None):
        """Initialize the CustomException class.

        Args:
            message (str): The error message associated with the exception.
            value (Optional[_type_], optional): Additional information related to the exception. Defaults to None.
        """
        self.message = message
        self.value = value
        super().__init__(self.message)
