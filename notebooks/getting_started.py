from dataguard import Validator

config = {
            'name': 'Minimal Config',
            'columns': [],
            'ids': [],
            'metadata': {},
            'checks': [],
        }

df = {}  # You can also pass a polars DataFrame here

validator = Validator.config_from_mapping(config)

validator.validate(df)

# Example with a more complex schema and checks
import polars as pl

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
                    'command': 'is_greater_than_or_equal_to',
                    'arg_values': [0],
                },
                {
                    'command': 'is_less_than',
                    'arg_values': [150],
                }
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

from dataguard import ErrorCollector

ErrorCollector().get_errors()

ErrorCollector().get_errors().error_reports[0].errors[1]