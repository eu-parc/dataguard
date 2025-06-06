from enum import Enum


class ErrorLevel(Enum):
    WARNING = 'warning'
    ERROR = 'error'
    CRITICAL = 'critical'


class ValidationType(Enum):
    DATE = 'date'
    DATETIME = 'datetime'
    BOOL = 'boolean'
    FLOAT = 'decimal'
    INT = 'integer'
    STR = 'varchar'
    CAT = 'categorical'


class CheckCases(Enum):
    CONDITION = 'condition'
    CONJUNCTION = 'conjunction'
    DISJUNCTION = 'disjunction'
