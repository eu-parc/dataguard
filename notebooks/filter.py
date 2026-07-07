import polars as pl
from dataguard import Filter

# Simple filter — keep rows where status == 'active'
config = {
    'name': 'active records',
    'filter': {
        'command': 'is_equal_to',
        'subject': ['status'],
        'arg_values': ['active'],
    },
}

df = pl.DataFrame({
    'id':     [1,        2,          3,        4],
    'age':    [25,       17,         34,       42],
    'status': ['active', 'inactive', 'active', 'inactive'],
})

filt = Filter.config_from_mapping(config)
result = filt.apply(df)
print(result)

# Conjunction — age >= 18 AND country in [BE, BR]
config_conj = {
    'name': 'adult be/br',
    'filter': {
        'check_case': 'conjunction',
        'expressions': [
            {
                'command': 'is_greater_than_or_equal_to',
                'subject': ['age'],
                'arg_values': [18],
            },
            {
                'command': 'is_in',
                'subject': ['country'],
                'arg_values': ['BE', 'BR'],
            },
        ],
    },
}

df_conj = pl.DataFrame({
    'age':     [10, 20, 25, 30],
    'country': ['BE', 'BE', 'US', 'BR'],
})

filt_conj = Filter.config_from_mapping(config_conj)
result_conj = filt_conj.apply(df_conj)
print(result_conj)

# Disjunction — age < 18 OR status == 'VIP'
config_disj = {
    'name': 'minor or vip',
    'filter': {
        'check_case': 'disjunction',
        'expressions': [
            {
                'command': 'is_less_than',
                'subject': ['age'],
                'arg_values': [18],
            },
            {
                'command': 'is_equal_to',
                'subject': ['status'],
                'arg_values': ['VIP'],
            },
        ],
    },
}

df_disj = pl.DataFrame({
    'age':    [10,  25,    35,   15],
    'status': ['OK', 'VIP', 'OK', 'OK'],
})

filt_disj = Filter.config_from_mapping(config_disj)
result_disj = filt_disj.apply(df_disj)
print(result_disj)

# Nested — (age >= 18 AND country in [BE, BR]) OR status == 'VIP'
config_nested = {
    'name': 'adult be/br or vip',
    'filter': {
        'check_case': 'disjunction',
        'expressions': [
            {
                'check_case': 'conjunction',
                'expressions': [
                    {
                        'command': 'is_greater_than_or_equal_to',
                        'subject': ['age'],
                        'arg_values': [18],
                    },
                    {
                        'command': 'is_in',
                        'subject': ['country'],
                        'arg_values': ['BE', 'BR'],
                    },
                ],
            },
            {
                'command': 'is_equal_to',
                'subject': ['status'],
                'arg_values': ['VIP'],
            },
        ],
    },
}

df_nested = pl.DataFrame({
    'age':     [10,   20,   25,   30,   15],
    'country': ['BE', 'BE', 'US', 'BR', 'US'],
    'status':  ['OK', 'OK', 'VIP', 'OK', 'VIP'],
})

filt_nested = Filter.config_from_mapping(config_nested)
result_nested = filt_nested.apply(df_nested)
print(result_nested)

# Column selection — return only selected columns after filtering
config_select = {
    'name': 'active age only',
    'filter': {
        'command': 'is_equal_to',
        'subject': ['status'],
        'arg_values': ['active'],
    },
    'select': ['id', 'age'],
}

filt_select = Filter.config_from_mapping(config_select)
result_select = filt_select.apply(df)
print(result_select)
