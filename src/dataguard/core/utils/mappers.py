import polars as pl

from dataguard.core.utils.enums import (
    CheckCases,
    ValidationType,
)

validation_type_mapper = {
    ValidationType.DATE: pl.Date,
    ValidationType.DATETIME: pl.Datetime,
    ValidationType.BOOL: pl.Boolean,
    ValidationType.FLOAT: pl.Float64,
    ValidationType.INT: pl.Int64,
    ValidationType.STR: pl.Utf8,
    # Cannot cast numerical to categorical, so reling on String
    ValidationType.CAT: pl.Utf8,
}

check_cases_mapper = {
    CheckCases.CONDITION: 'when_then',
    CheckCases.CONJUNCTION: 'and_',
    CheckCases.DISJUNCTION: 'or_',
}

expression_mapper = {
    'is_equal_to': 'eq',
    'is_equal_to_or_both_missing': 'eq_missing',
    'is_greater_than_or_equal_to': 'ge',
    'is_greater_than': 'gt',
    'is_less_than_or_equal_to': 'le',
    'is_less_than': 'lt',
    'is_not_equal_to': 'ne',
    'is_not_equal_to_and_not_both_missing': 'ne_missing',
    'is_unique': 'is_unique',
    'is_duplicated': 'is_duplicated',
    'is_in': 'is_in',
    'is_null': 'is_null',
    'is_not_null': 'is_not_null',
}
