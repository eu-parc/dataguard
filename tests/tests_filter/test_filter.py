import pytest
import polars as pl
from polars.testing import assert_frame_equal
from pydantic import ValidationError

from dataguard.filter.filter import Filter
from dataguard.core.utils.mappers import expression_mapper

_CLEAN_EXPRESSION_MAPPER = dict(expression_mapper)


@pytest.fixture(autouse=True)
def reset_expression_mapper():
    expression_mapper.clear()
    expression_mapper.update(_CLEAN_EXPRESSION_MAPPER)
    yield


@pytest.mark.parametrize('config, input_data, expected_data', [
    (   ### 0: simple predicate — equality
        {'name': 'status ok', 'filter': {'command': 'is_equal_to', 'subject': ['status'], 'arg_values': ['OK']}},
        pl.DataFrame({'status': ['OK', 'FAIL', 'OK']}),
        pl.DataFrame({'status': ['OK', 'OK']}),
    ),
    (   ### 1: conjunction — age >= 18 AND country in [BE, BR]
        {
            'name': 'adult be/br',
            'filter': {
                'check_case': 'conjunction',
                'expressions': [
                    {'command': 'is_greater_than_or_equal_to', 'subject': ['age'], 'arg_values': [18]},
                    {'command': 'is_in', 'subject': ['country'], 'arg_values': ['BE', 'BR']},
                ],
            },
        },
        pl.DataFrame({'age': [10, 20, 25, 30], 'country': ['BE', 'BE', 'US', 'BR']}),
        pl.DataFrame({'age': [20, 30], 'country': ['BE', 'BR']}),
    ),
    (   ### 2: disjunction — age < 18 OR age > 60
        {
            'name': 'minor or senior',
            'filter': {
                'check_case': 'disjunction',
                'expressions': [
                    {'command': 'is_less_than', 'subject': ['age'], 'arg_values': [18]},
                    {'command': 'is_greater_than', 'subject': ['age'], 'arg_values': [60]},
                ],
            },
        },
        pl.DataFrame({'age': [10, 20, 65, 30]}),
        pl.DataFrame({'age': [10, 65]}),
    ),
    (   ### 3 (n-ary): conjunction with 3 expressions
        {
            'name': 'adult active be/br',
            'filter': {
                'check_case': 'conjunction',
                'expressions': [
                    {'command': 'is_greater_than_or_equal_to', 'subject': ['age'], 'arg_values': [18]},
                    {'command': 'is_in', 'subject': ['country'], 'arg_values': ['BE', 'BR']},
                    {'command': 'is_equal_to', 'subject': ['status'], 'arg_values': ['active']},
                ],
            },
        },
        pl.DataFrame({
            'age':     [20,      30,      25,        17,      22],
            'country': ['BE',    'BR',    'BE',      'BE',    'US'],
            'status':  ['active','active','inactive','active','active'],
        }),
        pl.DataFrame({
            'age':     [20,      30],
            'country': ['BE',    'BR'],
            'status':  ['active','active'],
        }),
    ),
    (   ### 4 (n-ary): disjunction with 3 expressions
        {
            'name': 'minor senior or vip',
            'filter': {
                'check_case': 'disjunction',
                'expressions': [
                    {'command': 'is_less_than', 'subject': ['age'], 'arg_values': [18]},
                    {'command': 'is_greater_than', 'subject': ['age'], 'arg_values': [60]},
                    {'command': 'is_equal_to', 'subject': ['status'], 'arg_values': ['VIP']},
                ],
            },
        },
        pl.DataFrame({'age': [10, 30, 65, 25, 40], 'status': ['OK', 'VIP', 'OK', 'OK', 'OK']}),
        pl.DataFrame({'age': [10, 30, 65], 'status': ['OK', 'VIP', 'OK']}),
    ),
    (   ### 5: condition — when status == OK, then age >= 18
        # Rows where status != OK yield null from when/then → dropped by filter
        {
            'name': 'ok must be adult',
            'filter': {
                'check_case': 'condition',
                'expressions': [
                    {'command': 'is_equal_to', 'subject': ['status'], 'arg_values': ['OK']},
                    {'command': 'is_greater_than_or_equal_to', 'subject': ['age'], 'arg_values': [18]},
                ],
            },
        },
        pl.DataFrame({'age': [10, 20, 15, 25], 'status': ['OK', 'OK', 'FAIL', 'FAIL']}),
        pl.DataFrame({'age': [20], 'status': ['OK']}),
    ),
    (   ### 6: nested conjunction inside disjunction
        {
            'name': 'adult be/br or vip',
            'filter': {
                'check_case': 'disjunction',
                'expressions': [
                    {
                        'check_case': 'conjunction',
                        'expressions': [
                            {'command': 'is_greater_than_or_equal_to', 'subject': ['age'], 'arg_values': [18]},
                            {'command': 'is_in', 'subject': ['country'], 'arg_values': ['BE', 'BR']},
                        ],
                    },
                    {'command': 'is_equal_to', 'subject': ['status'], 'arg_values': ['VIP']},
                ],
            },
        },
        pl.DataFrame({
            'age':     [10,   20,   25,   30,   15],
            'country': ['BE', 'BE', 'US', 'BR', 'US'],
            'status':  ['OK', 'OK', 'VIP', 'OK', 'VIP'],
        }),
        pl.DataFrame({
            'age':     [20,   25,   30,   15],
            'country': ['BE', 'US', 'BR', 'US'],
            'status':  ['OK', 'VIP', 'OK', 'VIP'],
        }),
    ),
    (   ### 7: column selection via 'select'
        {
            'name': 'status ok age only',
            'filter': {'command': 'is_equal_to', 'subject': ['status'], 'arg_values': ['OK']},
            'select': ['age'],
        },
        pl.DataFrame({'age': [10, 20, 30], 'status': ['OK', 'FAIL', 'OK']}),
        pl.DataFrame({'age': [10, 30]}),
    ),
])
def test_filter_apply_dataframe(config, input_data, expected_data):
    filt = Filter.config_from_mapping(config)
    assert filt.name == config['name']
    result = filt.apply(input_data)
    assert isinstance(result, pl.DataFrame)
    assert_frame_equal(result, expected_data)


def test_filter_apply_lazyframe():
    config = {
        'name': 'adults',
        'filter': {'command': 'is_greater_than', 'subject': ['age'], 'arg_values': [18]},
    }
    df = pl.LazyFrame({'age': [10, 20, 30]})
    filt = Filter.config_from_mapping(config)
    result = filt.apply(df)
    assert isinstance(result, pl.LazyFrame)
    assert_frame_equal(result.collect(), pl.DataFrame({'age': [20, 30]}))


def test_filter_missing_filter_key():
    with pytest.raises(KeyError):
        Filter.config_from_mapping({'name': 'x'})


def test_filter_invalid_command():
    config = {
        'name': 'bad',
        'filter': {'command': 'not_a_real_command', 'subject': ['col'], 'arg_values': [1]},
    }
    with pytest.raises((ValidationError, AttributeError)):
        Filter.config_from_mapping(config)
