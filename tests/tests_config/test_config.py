import pytest
from dataguard.config.config_reader import get_df_schema
from dataguard.core.models.schemas import DFSchema

import pandera.polars as pa
from pydantic import ValidationError


def test_get_config():
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
                        'arg_values': ['0', '1', '2']
                        },
                        {
                        'command': 'is_equal_to', 
                        'subject': ['second_column'], 
                        'arg_values': ['1']
                        }
                    ]
                },
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
                'arg_values': ['1', '2']
            }
        ]
    }

    conf = get_df_schema(conf_input)
    schema = conf.build()

    assert isinstance(conf, DFSchema)
    assert isinstance(schema, pa.DataFrameSchema)
    assert schema.name == 'test_config'
    assert schema.metadata == {'meta_key': 'meta_value'}
    assert len(schema.columns) == 2
    assert len(schema.checks) == 1
    assert schema.columns['test_column'].name == 'test_column'
    assert schema.columns['test_column'].nullable is False
    assert schema.columns['test_column'].unique is True
    assert schema.columns['test_column'].required is True
    assert schema.columns['test_column'].checks[0].name == 'error'

    assert schema.columns['second_column'].name == 'second_column'
    assert schema.columns['second_column'].nullable is True
    assert schema.columns['second_column'].unique is False
    assert schema.columns['second_column'].required is False
    assert schema.columns['second_column'].checks[0].name == 'error'
    assert schema.checks[0].name == 'error'


def test_invalid_config_mapping():
    # Invalid configuration input that does not follow the specifications
    invalid_conf_input = {
        'name': 'invalid_config',
        'columns': (
            {
                'id': 'invalid_column',
                # Missing 'data_type' key
                'nullable': False,
                'unique': True,
                'required': True,
                'checks': (
                    {
                        'name': 'invalid_check',
                        'check_command': ('ge', 1),
                    },
                )
            },
        ),
        'metadata': {'meta_key': 'meta_value'},
        'checks': (
            {
                'name': 'is_in',
                'check_command': (
                    'is_in', ('invalid_column',), (1, 2)
                ),
                'error_level': 'error',
                'error_msg': '{} is in {}'
            },
        )
    }

    with pytest.raises(KeyError):
        get_df_schema(invalid_conf_input)


def test_get_config_break_check():
    conf_input = {
        'name': 'test_config',
        'columns': (
            {
            'id': 'test_column',
            'data_type': 'integer',
            'nullable': False,
            'unique': True,
            'required': True,
            'checks': (
                {
                #'name': 'test_check', # Missing name
                'check_command': ('ge', 1),
                },
            )
            },
            {
            'id': 'second_column',
            'data_type': 'integer',
            'nullable': True,
            'unique': False,
            'required': False,
            'checks': (
                {
                'name': 'second_check',
                'check_command': ('eq', 'test_column'),
                'error_level': 'warning',
                'error_msg': 'Second test error message'
                },
            )
            }
        ),
        'ids': ['test_column'],
        'metadata': {'meta_key': 'meta_value'},
        'checks': (
            {
            'name': 'is_in',
            'check_command': (
                'is_in', ('test_column', 'second_column'), (1, 2)
                ),
            'error_level': 'error',
            'error_msg': '{} is in {}'
            },
        )
    }

    with pytest.raises(ValidationError):
        get_df_schema(conf_input)


def test_get_config_validation_error():
    conf_input = {
        'name': 'test_config',
        'columns': (
            {
            'id': 'test_column',
            'data_type': 'integer',
            'nullable': False,
            'unique': 'ttt',   # Invalid type (should be bool)
            'required': True,
            'checks': (
                {
                'name': 'test_check',
                'check_command': ('ge', 1),
                },
            )
            },
            {
            'id': 'second_column',
            'data_type': 'integer',
            'nullable': True,
            'unique': False,
            'required': False,
            'checks': (
                {
                'name': 'second_check',
                'check_command': ('eq', 'test_column'),
                'error_level': 'warning',
                'error_msg': 'Second test error message'
                },
            )
            }
        ),
        'ids': ['test_column'],
        'metadata': {'meta_key': 'meta_value'},
        'checks': (
            {
            'name': 'is_in',
            'check_command': (
                'is_in', ('test_column', 'second_column'), (1, 2)
                ),
            'error_level': 'error',
            'error_msg': '{} is in {}'
            },
        )
    }

    with pytest.raises(ValidationError):
        get_df_schema(conf_input)

