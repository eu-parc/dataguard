# Getting Started

## Fundamental Dataguard workflow

```py title="getting_started.py" linenums="1" hl_lines="3-9 13 15"
--8<-- "notebooks/getting_started.py:1:15"
```
1 - Create a minimal validation configuration with the required fields: name, columns, ids, metadata and checks.

2 - Instantiate a `Validator` using `config_from_mapping()` method.

3 - Validate the dataframe `df` calling `validate()`

While this example uses empty lists and an empty DataFrame for simplicity, it illustrates the core three-step process: **define your validation rules** in a configuration, **create a validator** from that configuration, and **apply it to your data**.

## Validating real-world constraints

Consider an `age` column configuration that demonstrates DataGuard's data quality enforcement capabilities:

- **Type Safety**: Enforcing `integer` data type prevents string or float contamination
- **Null Prevention**: `nullable: False` ensures no missing age values slip through
- **Range Validation**: Age bounds `[0-150)` catch unrealistic values like negative ages or extreme outliers
- **Business Logic**: Reflects real-world constraints for human age data

```py title="getting_started.py" linenums="20" hl_lines="6 7 12-13 16-17"
--8<-- "notebooks/getting_started.py:20:52"
```

### Analyzing error report

Each validation instance that catches validation errors creates an `ErrorReport` that is
collected to the `ErrorCollector` under the hood. 

*Another way to access the `ErrorCollector` is calling the `Validator`'s `error_collector` property.*

```py title="getting_started.py" linenums="54"
--8<-- "notebooks/getting_started.py:54:56"

#{
#  "error_reports": [
#    {
#      "name": "Age must be not null, grater than or equal to 0 and less than 150",
#      "errors": [
#        {
#          "type": "SchemaErrorReason.SERIES_CONTAINS_NULLS",
#          "message": "non-nullable column 'age' contains null values",
#          "level": "error",
#          "title": "Not_Nullable",
#          "traceback": null
#        },
#        {
#          "type": "SchemaErrorReason.DATAFRAME_CHECK",
#          "message": "Column 'age' failed validator number 0: <Check error: The column under validation is greater than or equal to \"0\"> failure case examples: [{'age': -5}]",
#          "level": "error",
#          "title": "Is greater than or equal to",
#          "traceback": null
#        },
#        {
#          "type": "SchemaErrorReason.DATAFRAME_CHECK",
#          "message": "Column 'age' failed validator number 1: <Check error: The column under validation is less than \"150\"> failure case examples: [{'age': 150}]",
#          "level": "error",
#          "title": "Is less than",
#          "traceback": null
#        }
#      ],
#      "total_errors": 3,
#      "id": "555860cb-9598-4e68-b6e5-deb7d04aced2"
#    }
#  ],
#  "exceptions": []
#}
```

Let's deep dive into the second error `Is greater than or equal to`.

```py title="getting_started.py" linenums="57"
--8<-- "notebooks/getting_started.py:57:58"

#DFErrorSchema(
# type='SchemaErrorReason.DATAFRAME_CHECK', 
# message='Column \'age\' failed validator number 0: <Check error: The column under validation is greater than or equal to "0"> failure case examples: [{\'age\': -5}]', 
# level=<ErrorLevel.ERROR: 'error'>, 
# title='Is greater than or equal to', 
# traceback=None, 
# column_names=['age'], 
# row_ids=[3], 
# idx_columns=[]
#)
```

The `DFErrorSchema` return provides detailed information about a specific validation error:

#### DFErrorSchema Fields
- **type**: The error category (SchemaErrorReason.DATAFRAME_CHECK) indicating this is a column-level validation failure

- **message**: Descriptive error text explaining what failed, including the specific check and example failure cases

- **level**: Error severity (ErrorLevel.ERROR) - can be error, warning, or info

- **title**: Human-readable error name (Is greater than or equal to) matching the validation rule

- **traceback**: Stack trace information (None if not available) for debugging

- **column_names**: List of affected columns (['age']) - useful for multi-column validations

- **row_ids**: Specific row indices that failed validation ([3]) - enables precise error location

- **idx_columns**: Index column information ([]) - empty when not using custom indices

In this example, the error shows that row `3` in the `'age'` column contains the value `-5`, which violates the "greater than or equal to 0" constraint. This granular information allows you to pinpoint exactly which data points need fixing and why they failed validation.