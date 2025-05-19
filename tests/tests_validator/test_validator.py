import logging

import pytest
import polars as pl

from peh_validation_library.validator.validator import Validator
from peh_validation_library.error_report.error_collector import ErrorCollector
from peh_validation_library.error_report.error_schemas import ExceptionSchema

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
        "test_column": [1, 2, 3],
        "second_column": [3, 2, 1]
    }

    logger = logging.getLogger("test_logger")

    validator = Validator.build_validator(
        config=conf_input, dataframe=dataframe, logger=logger
        )
    
    validator.validate()
    
    assert validator is not None
    assert validator.dataframe is not None
    assert validator.config is not None
    assert len(error_collector.get_errors()[0].schema_errors) == 4
    

def test_build_validator_error_handling(error_collector):
    config = {
        "invalid_key": "invalid_value"
    }
    
    dataframe = {
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"]
    }
    
    logger = logging.getLogger("test_logger")

    validator = Validator.build_validator(
        config=config, dataframe=dataframe, logger=logger
        )
    
    assert len(error_collector.get_errors()) == 1
    error = error_collector.get_errors()[0]
    assert isinstance(error, ExceptionSchema)
    assert error.error_level.name == 'CRITICAL'