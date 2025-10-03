import polars as pl
from dataguard import Validator, ErrorCollector


def is_between(data, arg_values=None, arg_columns=None, subject=None):
        return data.lazyframe.select(
            pl.col(data.key).is_between(arg_values[0], arg_values[1], closed='left')
        )

config_age = {
    'name': 'Age must be not null, grater than or equal to 0 and less than 150',
    'columns': [
        {
            'id': 'age',
            'data_type': 'integer',
            'nullable': False,
            'unique': False,
            'required': True,
            'checks': [
                {
                    'name': 'Tailor-made function check: is_between',
                    'error_level': 'warning',
                    'error_msg': 'Age must be between 0 (inclusive) and 150 (exclusive)',
                    'command': is_between,
                    'arg_values': [0, 150],
                },
            ],
        },
    ],
    'ids': [],
    'metadata': {},
    'checks': [],
}

df_age = pl.DataFrame({
    'age': [2, 30, None, -5, 150, 45, 50],
})

validator = Validator.config_from_mapping(config_age)

validator.validate(df_age)

ErrorCollector().get_errors()
import polars as pl
from dataguard import Validator, ErrorCollector


def is_between(data, arg_values=None, arg_columns=None, subject=None):
        return data.lazyframe.select(
            pl.col(data.key).is_between(arg_values[0], arg_values[1], closed='left')
        )

config_age = {
    'name': 'Age must be not null, grater than or equal to 0 and less than 150',
    'columns': [
        {
            'id': 'age',
            'data_type': 'integer',
            'nullable': False,
            'unique': False,
            'required': True,
            'checks': [
                {
                    'name': 'Tailor-made function check: is_between',
                    'error_level': 'warning',
                    'error_msg': 'Age must be between 0 (inclusive) and 150 (exclusive)',
                    'command': is_between,
                    'arg_values': [0, 150],
                },
            ],
        },
    ],
    'ids': [],
    'metadata': {},
    'checks': [],
}

df_age = pl.DataFrame({
    'age': [2, 30, None, -5, 150, 45, 50],
})

validator = Validator.config_from_mapping(config_age)

validator.validate(df_age)

ErrorCollector().get_errors()


ErrorCollector().clear_errors()


config_age = {
    'name': 'Age must be not null, grater than or equal to 0 and less than 150',
    'columns': [
        {
            'id': 'age',
            'data_type': 'integer',
            'nullable': False,
            'unique': False,
            'required': True,
            'checks': [
                    {
                    'check_case': 'conjunction',
                    'expressions': [
                        {
                        'command': 'is_greater_than_or_equal_to', 
                        'arg_values': [0]
                        },
                        {
                        'command': 'is_less_than', 
                        'arg_values': [150]
                        }
                    ]
                },
                ]
        },
    ],
    'ids': [],
    'metadata': {},
    'checks': [],
}

df_age = pl.DataFrame({
    'age': [2, 30, None, -5, 150, 45, 50],
})

validator = Validator.config_from_mapping(config_age)

validator.validate(df_age)

ErrorCollector().get_errors()