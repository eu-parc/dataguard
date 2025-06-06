"""Enums used in the validation library.
These enums define various constants used throughout the library for
error levels, validation types, and check cases.

config_input must use the following naming conventions.

"""

from enum import Enum


class ErrorLevel(Enum):
    """Enum representing different levels of error severity."""

    WARNING = 'warning'
    ERROR = 'error'
    CRITICAL = 'critical'


class ValidationType(Enum):
    """Enum representing different validation types for DataFrame columns."""

    DATE = 'date'
    DATETIME = 'datetime'
    BOOL = 'boolean'
    FLOAT = 'decimal'
    INT = 'integer'
    STR = 'varchar'
    CAT = 'categorical'


class CheckCases(Enum):
    """Enum representing different types of check cases."""

    CONDITION = 'condition'
    CONJUNCTION = 'conjunction'
    DISJUNCTION = 'disjunction'
