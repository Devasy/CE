"""Classifier related schemas."""
from enum import Enum


class TrainingType(str, Enum):
    """Type of the training to perform on the classifier."""

    POSITIVE = "positive"
    NEGATIVE = "negative"


class ClassifierType(str, Enum):
    """Type of the classifier selected in the mapping."""

    CUSTOM = "custom"
    # PREDEFINED = "predefined"
