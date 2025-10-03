import logging

import pytest
import polars as pl
import pandera.polars as pa

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

@pytest.mark.parametrize("input_config, input_data, expected_output", [
    (   ### 0 INIT: Empty config -> KeyError
        ## Config
        {

        }, 
        ## Data
        pl.DataFrame(),
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL'],
        'error_types': ['KeyError'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### 0 END: Empty input
    (   ### 1 INIT: Basic invalid config -> AttributeError
        ## Config
        {
            
            'name': 'empty config',
            'columns': [],
            'ids': [],
            'metadata': {},
            'checks': 'string_instead_of_list'
        }, 
        ## Data
        pl.DataFrame(),
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL'],
        'error_types': ['AttributeError'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### 1 END: Basic invalid config
    (   ### 2 INIT: Basic invalid config -> TypeError
        ## Config
        {
            
            'name': 'empty config',
            'columns': [],
            'ids': 10,  # Should be a list
            'metadata': {},
            'checks': []
        }, 
        ## Data
        pl.DataFrame(),
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL'],
        'error_types': ['TypeError'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### 2 END: Basic invalid config
    (   ### 3 INIT: Basic invalid config -> ValidationError
        ## Config
        {
            
            'name': 10,
            'columns': [],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        pl.DataFrame(),
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL'],
        'error_types': ['ValidationError'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### 3 END: Basic invalid config
    (   ### 4 INIT: Invalid DataFrame with minimal config
        ## Config
        {
            'name': 'invalid dataframe',
            'columns': [],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': [1, 2, 3], 'col2': ['a', 'b',]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL'],
        'error_types': ['ShapeError'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ), ### 4 END: Invalid DataFrame with minimal config
    (  ### 5 INIT: Cast error
        ## Config
        {
            'name': 'invalid dataframe',
            'columns': [{
                'id': 'col1',
                'data_type': 'integer',
                'nullable': False,
                'unique': False,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'b', 'c']},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL'],
        'error_types': ['InvalidOperationError'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ), ### 5 END: Cast error
])
def test_validator_before_pandera_validation(
    error_collector, input_config, input_data, expected_output
    ):
    
    validator = Validator.config_from_mapping(config=input_config)
    
    validator.validate(input_data)
    
    assert validator is not None
    assert error_collector is not None
    assert len(error_collector.get_errors().error_reports) == expected_output['len_error_reports']
    assert [report.total_errors for report in error_collector.get_errors().error_reports] == expected_output['total_errors']
    assert [error.level.name for report in error_collector.get_errors().error_reports for error in report.errors] == expected_output['error_levels']
    assert [error.type for report in error_collector.get_errors().error_reports for error in report.errors] == expected_output['error_types']
    assert len(error_collector.get_errors().exceptions) == expected_output['len_exceptions']
    assert [exception.level.name for exception in error_collector.get_errors().exceptions] == expected_output['exception_levels']
    
@pytest.mark.parametrize("input_config, input_data, expected_output", [
    (   ### INIT ###
        ## Config
        {
            'name': 'empty dataframe + minimal config',
            'columns': [],
            'ids': [],
            'metadata': {},
            'checks': [],
        }, 
        ## Data
        pl.DataFrame(),
        ## Expected output 
        {
        'len_error_reports': 0,
        'total_errors': [],
        'error_levels': [],
        'error_types': [],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'empty dataframe + col not required',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': True,
                'unique': False,
                'required': False,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': [],
        }, 
        ## Data
        pl.DataFrame(),
        ## Expected output 
        {
        'len_error_reports': 0,
        'total_errors': [],
        'error_levels': [],
        'error_types': [],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col not nullable + missing data in df',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': False,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'b', 'c', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],
        'error_types': ['SchemaErrorReason.SERIES_CONTAINS_NULLS'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'All good',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': True,
                'unique': False,
                'required': False,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'b', 'c', None]},
        ## Expected output 
        {
        'len_error_reports': 0,
        'total_errors': [],
        'error_levels': [],
        'error_types': [],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col required but missing in df',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': False,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col2': ['a', 'b', 'c', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],
        'error_types': ['SchemaErrorReason.COLUMN_NOT_IN_DATAFRAME'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col unique but duplicates in df',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'b', 'c', 'a']},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],
        'error_types': ['SchemaErrorReason.SERIES_CONTAINS_DUPLICATES'],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col unique AND not nullable but duplicates AND nullable in df',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'a', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [2],
        'error_levels': ['ERROR', 'ERROR'],
        'error_types': [
            'SchemaErrorReason.SERIES_CONTAINS_NULLS',
            'SchemaErrorReason.SERIES_CONTAINS_DUPLICATES',
            ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col unique AND not nullable but duplicates AND nullable in df + fail check',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': [
                    {
                        'command': 'is_in',
                        'arg_values': ['x', 'y', 'z'],
                        'error_level': 'warning', # Override default error level
                    }
                ]
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'a', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [3],
        'error_levels': ['ERROR', 'ERROR', 'WARNING'],  
        'error_types': [
            'SchemaErrorReason.SERIES_CONTAINS_NULLS',
            'SchemaErrorReason.SERIES_CONTAINS_DUPLICATES',
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col unique AND not nullable but duplicates AND nullable in df + fail check',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': [
                    {
                        'command': 'is_in',
                        'arg_values': ['x'],
                        'error_level': 'warning', # Override default error level
                    }
                ]
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'a', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [3],
        'error_levels': ['ERROR', 'ERROR', 'WARNING'],  
        'error_types': [
            'SchemaErrorReason.SERIES_CONTAINS_NULLS',
            'SchemaErrorReason.SERIES_CONTAINS_DUPLICATES',
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col unique at df level - falls under eager validation in pandera',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': []
            }],
            'ids': ['col1'],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'a', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['CRITICAL',],
        'error_types': [
            'SchemaErrorReason.DUPLICATES',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Col > x OR <y',
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': [
                    {
                    'check_case': 'disjunction',
                    'expressions': [
                        {
                        'command': 'is_greater_than', 
                        'arg_values': [10]
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [1]
                        }
                    ]
                },
                ]
            }],
            'ids': ['col1'],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': [9.0, 0.2, None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [2],
        'error_levels': ['ERROR', 'ERROR'],
        'error_types': [
            'SchemaErrorReason.SERIES_CONTAINS_NULLS',
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Check against a column that does not exist in df',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': [
                    {
                        'command': 'is_not_equal_to_and_not_both_missing',
                        'arg_columns': ['col2'],  # col2 does not exist in df
                    }
                ]
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': ['a', 'a', None]},
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],  
        'error_types': [
            'SchemaErrorReason.CHECK_ERROR',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Check against a column',
            'columns': [{
                'id': 'col1',
                'data_type': 'string',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': [
                    {
                        'command': 'is_not_equal_to_and_not_both_missing',
                        'arg_columns': ['col2'],
                    }
                ]
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {
            'col1': ['a', 'a', None],
            'col2': ['a', 'a', None],
        },
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],  
        'error_types': [
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'Column in a range',
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': [
                    {
                    'check_case': 'conjunction',
                    'expressions': [
                        {
                        'command': 'is_greater_than', 
                        'arg_values': [2]
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [4]
                        }
                    ]
                },
                ]
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {
            'col1': [1, 2, 3, 7],
        },
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],  
        'error_types': [
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'DF check - Column in a range',
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': []
            },{
                'id': 'col2',
                'data_type': 'float',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': [          
                    {
                    'check_case': 'conjunction',
                    'expressions': [
                        {
                        'command': 'is_greater_than', 
                        'arg_values': [2],
                        'subject': ['col1', 'col2']
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [4],
                        'subject': ['col1', 'col2']
                        }
                    ]
                },
            ]
        }, 
        ## Data
        {
            'col1': [1, 2, 3, 7],
            'col2': [1, 3, 3, 7],
        },
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],  
        'error_types': [
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'DF check - Check Error - invalid check command', # Makes the PolarsData.key type into a string, where the default key is "*" (select all columns) by default. #1972 Pandera change
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': []
            },{
                'id': 'col2',
                'data_type': 'float',
                'nullable': True,
                'unique': False,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': [          
                    {
                    'check_case': 'conjunction',
                    'expressions': [
                        {
                        'command': 'is_greater_than', 
                        'arg_values': [2],
                        #'subject': ['col1', 'col2'] # Must especify subject at this level
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [4],
                        #'subject': ['col1', 'col2'] # Must especify subject at this level
                        }
                    ]
                },
            ]
        }, 
        ## Data
        {
            'col1': [1, 2, 3, 7],
            'col2': [1, 3, 3, 7],
        },
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [1],
        'error_levels': ['ERROR'],  
        'error_types': [
            # #1972 Pandera bugfix
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
    (   ### INIT ###
        ## Config
        {
            'name': 'DF check - Check Error - invalid check command first will break',
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': False,
                'unique': False,
                'required': True,
                'checks': []
            },{
                'id': 'col2',
                'data_type': 'float',
                'nullable': True,
                'unique': True,
                'required': True,
                'checks': []
            }],
            'ids': [],
            'metadata': {},
            'checks': [          
                        {
                        'command': 'is_greater_than', 
                        'arg_values': [2],
                        # #1972 Pandera bugfix - "*" is default key that selects all columns
                        #'subject': ['col1', 'col2'] # Must especify subject at this level,
                        'error_level': 'critical', # Stop processing further errors if this one fails
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [4],
                        'subject': ['col1', 'col2']
                        }
            ]
        }, 
        ## Data
        {
            'col1': [1, 2, 3, 7, None],
            'col2': [1, 3, 3, 7, 3],
        },
        ## Expected output 
        {
        'len_error_reports': 1,
        'total_errors': [3],
        'error_levels': ['ERROR', 'ERROR', 'CRITICAL'],  
        'error_types': [
            'SchemaErrorReason.SERIES_CONTAINS_NULLS',
            'SchemaErrorReason.SERIES_CONTAINS_DUPLICATES',
            'SchemaErrorReason.DATAFRAME_CHECK',
        ],
        'len_exceptions': 0,
        'exception_levels': [],
        }
    ),  ### END ###
])
def test_validator_collect_errors(
    error_collector, input_config, input_data, expected_output
    ):
   
    validator = Validator.config_from_mapping(config=input_config)
    
    validator.validate(input_data)

    assert validator is not None
    assert error_collector is not None
    assert len(error_collector.get_errors().error_reports) == expected_output['len_error_reports']
    assert [report.total_errors for report in error_collector.get_errors().error_reports] == expected_output['total_errors']
    assert [error.level.name for report in error_collector.get_errors().error_reports for error in report.errors] == expected_output['error_levels']
    assert [error.type for report in error_collector.get_errors().error_reports for error in report.errors] == expected_output['error_types']
    assert len(error_collector.get_errors().exceptions) == expected_output['len_exceptions']
    assert [exception.level.name for exception in error_collector.get_errors().exceptions] == expected_output['exception_levels']


def test_validator_custom_function(
    error_collector, fake_check_fn
    ):
   
    validator = Validator.config_from_mapping(
        config={
            'name': 'fn check',
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': False,
                'unique': False,
                'required': True,
                'checks': [
                    {
                        'command': fake_check_fn,
                        'arg_values': [1, 2, 3],
                    }
                ]
            },],
            'ids': [],
            'metadata': {},
            'checks': []
        }
    )
    
    
    validator.validate({'col1': [1, 2, 3, 7, None],})

    assert validator is not None
    assert error_collector is not None
    assert len(error_collector.get_errors().error_reports) == 1
    assert [report.total_errors for report in error_collector.get_errors().error_reports] == [2]
    assert [error.level.name for report in error_collector.get_errors().error_reports for error in report.errors] == ['ERROR', 'ERROR']
    assert [error.type for report in error_collector.get_errors().error_reports for error in report.errors] == ['SchemaErrorReason.SERIES_CONTAINS_NULLS', 'SchemaErrorReason.DATAFRAME_CHECK']
    assert len(error_collector.get_errors().exceptions) == 0
    assert [exception.level.name for exception in error_collector.get_errors().exceptions] == []

@pytest.mark.parametrize("input_config, input_data", [
    (   ### INIT ###
        ## Config
        {
            'name': 'Col > x OR <y',
            'columns': [{
                'id': 'col1',
                'data_type': 'float',
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': [
                    {
                    'check_case': 'disjunction',
                    'expressions': [
                        {
                        'command': 'is_greater_than', 
                        'arg_values': [10]
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [1]
                        }
                    ]
                },
                ]
            }],
            'ids': [],
            'metadata': {},
            'checks': []
        }, 
        ## Data
        {'col1': [9.0, 0.2, None]},
    ),  ### END ###
])
def test_validator_eager_validation(input_config, input_data,):
   
    validator = Validator.config_from_mapping(config=input_config)
    
    with pytest.raises(pa.errors.SchemaError):
        validator.validate(
            input_data, lazy_validation=False, collect_exceptions=False
            )

