import importlib

import pytest
import pandera.polars as pa

from peh_validation_library.core.check.schemas import (
    SimpleCheckExpression,
    CaseCheckExpression,
)
from peh_validation_library.core.utils.enums import (
    ErrorLevel,
    ValidationType,
)
from peh_validation_library.core.utils.mappers import (
    expression_mapper,
    validation_type_mapper,
)
from peh_validation_library.core.models.schemas import CheckSchema

@pytest.mark.parametrize(
    'check_command, subject, arg_values, arg_columns', [
        ('is_null', None, None, None),
        ('is_not_null', ['col1'], None, None),
        ('is_equal_to', None, [1], None),
        ('is_equal_to', None, None, ['col_2']),
        ('is_equal_to_or_both_missing', ['col1'], [1], None),
        ('is_greater_than_or_equal_to', ['col1'], None, ['col_2']),
    ]
)
def test_get_schema_simple_check(check_command, subject, arg_values, arg_columns):
    check_command = SimpleCheckExpression(
        command=check_command,
        subject=subject,
        arg_values=arg_values,
        arg_columns=arg_columns
    )

    expected_name = check_command.get_check_title()
    expected_args = check_command.get_args()
    expected_error_msg = check_command.get_check_message()

    check_schema = CheckSchema.get_schema(check_command)
    

    assert check_schema.name == expected_name
    assert check_schema.args_ == expected_args
    assert check_schema.error_level == ErrorLevel.ERROR
    assert check_schema.error_msg == expected_error_msg
    assert hasattr(check_schema.fn, '__call__')
    assert isinstance(check_schema.build(), pa.Check)

def test_get_schema_simple_check_module():

    def fake_check_fn(*args, **kwargs):
        pass

    check_command = SimpleCheckExpression(
        command=fake_check_fn,
    )

    expected_name = check_command.get_check_title()
    expected_args = check_command.get_args()
    expected_error_msg = check_command.get_check_message()

    check_schema = CheckSchema.get_schema(check_command)
    

    assert check_schema.name == expected_name
    assert check_schema.args_ == expected_args
    assert check_schema.error_level == ErrorLevel.ERROR
    assert check_schema.error_msg == expected_error_msg
    assert hasattr(check_schema.fn, '__call__')
    assert isinstance(check_schema.build(), pa.Check)


@pytest.mark.parametrize(
    'check_case, expressions', [
        ('condition', [
            SimpleCheckExpression(
                command='is_null',
                subject=None,
                arg_values=None,
                arg_columns=None
            ),
            SimpleCheckExpression(
                command='is_not_null',
                subject=['col1'],
                arg_values=None,
                arg_columns=None
            )
        ]),
        ('conjunction', [
            SimpleCheckExpression(
                command='is_equal_to',
                subject=None,
                arg_values=[1],
                arg_columns=None
            ),
            SimpleCheckExpression(
                command='is_equal_to',
                subject=None,
                arg_values=None,
                arg_columns=['col_2']
            )
        ]),
    ]
)
def test_get_schema_case_check(check_case, expressions):
    check_case = CaseCheckExpression(
        check_case=check_case,
        expressions=expressions
    )

    expected_name = check_case.get_check_title()
    expected_args = check_case.get_args()
    expected_error_msg = check_case.get_check_message()

    check_schema = CheckSchema.get_schema(check_case)

    assert check_schema.name == expected_name
    assert check_schema.args_ == expected_args
    assert check_schema.error_level == ErrorLevel.ERROR
    assert check_schema.error_msg == expected_error_msg
    assert hasattr(check_schema.fn, '__call__')
    assert isinstance(check_schema.build(), pa.Check)