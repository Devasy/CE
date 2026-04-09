"""Exception Class for EDM module."""


class CustomException(Exception):
    """CustomException Class."""

    def __init__(
        self, value=None, message="Error occurred when processing your request"
    ):
        """Init method.

        Args:
            value (Optional[_type_], optional): Additional information related to the exception.
            message (str, optional): The error message associated with the exception.
        """
        self.value = value
        self.message = message
        super().__init__(self.message)
