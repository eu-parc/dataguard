import logging

import pytest
import polars as pl

from dataguard.validator.validator import Validator
from dataguard.error_report.error_collector import ErrorCollector
from dataguard.error_report.error_schemas import ExceptionSchema

@pytest.fixture
def error_collector():
    error_collector = ErrorCollector()
    yield error_collector
    error_collector.clear_errors()


@pytest.fixture
def fake_check_fn():
    def check_fn(data, arg_values=None, arg_columns=None, subject=None):
        return data.lazyframe.select(
            pl.col(data.key).is_in(arg_values)
        )

    return check_fn


def test_build_validator_success(error_collector, fake_check_fn):
    conf_input = {
        'name': 'test_config',
        'columns': (
            {
            'id': 'test_column',
            'data_type': 'integer',
            'nullable': False,
            'unique': True,
            'required': True,
            'checks': [
                {
                    'check_case': 'condition',
                    'expressions': [
                        {
                        'command': 'is_in', 
                        'arg_values': [0, 1, 2]
                        },
                        {
                        'command': 'is_equal_to', 
                        'subject': ['second_column'], 
                        'arg_values': [1]
                        }
                    ]
                },
                {
                    'command': fake_check_fn,
                    'arg_values': [0, 1],
                    'arg_columns': None
                },
                {
                    'check_case': 'condition',
                    'expressions': [
                        {
                            'check_case': 'conjunction',
                            'expressions': [
                                {
                                    'command': 'is_not_null',
                                },
                                {
                                    'command': 'is_in',
                                    'arg_values': [1, 2, 3]
                                },
                            ]
                        },
                        {
                            'command': 'is_equal_to',
                            'arg_values': [1],
                            'subject': ['second_column']
                        }
                    ]
                }
                ]
                },
                {
                    'id': 'second_column',
            'data_type': 'integer',
            'nullable': False,
            'unique': False,
            'required': False,
            'checks': [
                {
                    'command': 'is_equal_to',
                    'arg_columns': ['test_column'],
                }
            ]
        }
        ),
        'ids': ['test_column'],
        'metadata': {'meta_key': 'meta_value'},
        'checks': [
            {
                'command': 'is_in',
                'subject': ['test_column', 'second_column'],
                'arg_values': [1, 2]
            }
        ]
    }

    dataframe = {
        "test_column": [1, 2, 3, None],
        "second_column": [3, 2, 1, None]
    }

    validator = Validator.config_from_mapping(config=conf_input)
    
    validator.validate(dataframe)

    assert validator is not None
    assert validator.df_schema is not None
    assert len(error_collector.get_errors().error_reports[0].errors) == 7
    

def test_build_validator_error_handling(error_collector):
    config = {
        "invalid_key": "invalid_value"
    }
    
    dataframe = {
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"]
    }
    
    validator = Validator.config_from_mapping(config=config)
    
    assert len(error_collector.get_errors().exceptions) == 1
    error = error_collector.get_errors().exceptions[0]
    assert isinstance(error, ExceptionSchema)
    assert error.error_level.name == 'CRITICAL'


def test_validator_validate_exception(error_collector, fake_check_fn):
    conf_input = {
        'name': 'test_config',
        'columns': (
            {
            'id': 'test_column',
            'data_type': 'integer',
            'nullable': False,
            'unique': True,
            'required': True,
            'checks': [
                {
                    'check_case': 'condition',
                    'expressions': [
                        {
                        'command': 'is_in', 
                        'arg_values': [0, 1, 2]
                        },
                        {
                        'command': 'is_equal_to', 
                        'subject': ['second_column'], 
                        'arg_values': [1]
                        }
                    ]
                },
                {
                    'command': fake_check_fn,
                    'arg_values': [0, 1],
                    'arg_columns': None
                }
            ]
            },
            {
            'id': 'second_column',
            'data_type': 'integer',
            'nullable': True,
            'unique': False,
            'required': False,
            'checks': [
                {
                    'command': 'is_equal_to',
                    'arg_columns': ['test_column'],
                }
            ]
        }
        ),
        'ids': ['test_column'],
        'metadata': {'meta_key': 'meta_value'},
        'checks': [
            {
                'command': 'is_in',
                'subject': ['test_column', 'second_column'],
                'arg_values': [1, 2]
            }
        ]
    }

    dataframe = {
        "test_column": ['a', 'b', 'c'],  # Invalid data type for integer check
        "second_column": [3, 2, 1]
    }
    
    validator = Validator.config_from_mapping(config=conf_input)

    validator.validate(dataframe)
    
    assert len(error_collector.get_errors().exceptions) == 1
    assert len(error_collector.get_errors().error_reports) == 0
    assert error_collector.get_errors().exceptions[0].error_type == 'InvalidOperationError'
    assert 'polars/lazyframe/frame.py:2224:collect' in error_collector.get_errors().exceptions[0].error_source


def test_validator_validate_exception_eager(error_collector, fake_check_fn):
    conf_input = {
        'name': 'test_config',
        'columns': (
            {
            'id': 'test_column',
            'data_type': 'integer',
            'nullable': False,
            'unique': True,
            'required': True,
            'checks': [
                {
                    'check_case': 'condition',
                    'expressions': [
                        {
                        'command': 'is_in', 
                        'arg_values': [0, 1, 2]
                        },
                        {
                        'command': 'is_equal_to', 
                        'subject': ['second_column'], 
                        'arg_values': [1]
                        }
                    ]
                },
                {
                    'command': fake_check_fn,
                    'arg_values': [0, 1],
                    'arg_columns': None
                }
            ]
            },
            {
            'id': 'second_column',
            'data_type': 'integer',
            'nullable': True,
            'unique': False,
            'required': False,
            'checks': [
                {
                    'command': 'is_equal_to',
                    'arg_columns': ['test_column'],
                }
            ]
        }
        ),
        'ids': ['test_column'],
        'metadata': {'meta_key': 'meta_value'},
        'checks': [
            {
                'command': 'is_in',
                'subject': ['test_column', 'second_column'],
                'arg_values': [1, 2]
            }
        ]
    }

    dataframe = {
        "test_column": ['a', 'b', 'c'],  # Invalid data type for integer check
        "second_column": [3, 2, 1]
    }
    
    validator = Validator.config_from_mapping(config=conf_input)

    with pytest.raises(pl.exceptions.InvalidOperationError):
        validator.validate(dataframe, collect_exceptions=False)
    
    