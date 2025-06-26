import pytest
from pydantic import ValidationError
from dataguard.core.check.schemas import SimpleCheckExpression, CaseCheckExpression
from dataguard.core.utils.enums import CheckCases
from dataguard.core.utils.mappers import expression_mapper

@pytest.fixture
def fake_callable():
    def callable_function(*args, **kwargs):
        return "fake_result"
    return callable_function


def test_case_check_expression_valid():
    expression_mapper['test_command'] = 'mapped_command'
    simple_expr = SimpleCheckExpression(command='test_command')
    case_expr = CaseCheckExpression(
        check_case=CheckCases.CONJUNCTION,
        expressions=[simple_expr, simple_expr]
    )
    assert case_expr.check_case == CheckCases.CONJUNCTION
    assert len(case_expr.expressions) == 2


def test_case_check_expression_invalid_length():
    expression_mapper['test_command'] = 'mapped_command'
    simple_expr = SimpleCheckExpression(command='test_command')
    with pytest.raises(ValidationError) as exc_info:
        CaseCheckExpression(
            check_case=CheckCases.CONJUNCTION,
            expressions=[simple_expr]
        )
    
def test_simple_check_expression_get_check_name():
    instance = SimpleCheckExpression(command='test_command')
    assert instance.get_check_title() == 'Test command'


def test_simple_check_expression_get_message_with_subject():
    instance = SimpleCheckExpression(command='test_command', subject=['column1', 'column2'])
    assert instance.get_check_message() == "Column(s) ['column1', 'column2'] test command"


def test_simple_check_expression_get_message_with_arg_values():
    instance = SimpleCheckExpression(command='test_command', arg_values=[1, 2, 3])
    assert instance.get_check_message() == 'The column under validation test command [1, 2, 3]'


def test_simple_check_expression_get_message_with_arg_columns():
    instance = SimpleCheckExpression(command='test_command', arg_columns=['col1', 'col2'])
    assert instance.get_check_message() == "The column under validation test command ['col1', 'col2']"


def test_simple_check_expression_map_command():
    expression_mapper['test_command'] = 'mapped_command'
    instance = SimpleCheckExpression(command='test_command')
    instance.map_command()
    assert instance.command == 'mapped_command'


def test_simple_check_expression_get_args():
    instance = SimpleCheckExpression(
        command='test_command',
        subject=['column1'],
        arg_values=[1, 2],
        arg_columns=['col1']
    )
    args = instance.get_args()
    assert args == {
        'subject': ['column1'],
        'arg_values': [1, 2],
        'arg_columns': ['col1']
    }


def test_case_check_expression_get_check_name():
    expression_mapper['test_command'] = 'mapped_command'
    simple_expr = SimpleCheckExpression(command='test_command')
    case_expr = CaseCheckExpression(
        check_case=CheckCases.CONJUNCTION,
        expressions=[simple_expr, simple_expr]
    )
    assert case_expr.get_check_title() == 'Test command and Test command'


def test_case_check_expression_get_message():
    expression_mapper['test_command'] = 'mapped_command'
    simple_expr = SimpleCheckExpression(command='test_command', subject=['column1'])
    case_expr = CaseCheckExpression(
        check_case=CheckCases.CONJUNCTION,
        expressions=[simple_expr, simple_expr]
    )
    assert case_expr.get_check_message() == (
        'Column(s) "column1" test command and Column(s) "column1" test command'
    )


def test_case_check_expression_get_args():
    expression_mapper['test_command'] = 'mapped_command'
    simple_expr = SimpleCheckExpression(
        command='test_command',
        subject=['column1'],
        arg_values=[1, 2]
    )
    case_expr = CaseCheckExpression(
        check_case=CheckCases.CONJUNCTION,
        expressions=[simple_expr, simple_expr]
    )
    args = case_expr.get_args()
    assert args == [
        {'subject': ['column1'], 'arg_values': [1, 2]},
        {'subject': ['column1'], 'arg_values': [1, 2]}
    ]


