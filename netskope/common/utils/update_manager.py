"""Update manager class."""

from .singleton import Singleton
from .logger import Logger


class UpdateException(Exception):
    """Custom update exception."""

    pass


class UpdateManager(metaclass=Singleton):
    """Update manager class."""

    def __init__(self):
        """Initialize update manager."""
        self.logger = Logger()
