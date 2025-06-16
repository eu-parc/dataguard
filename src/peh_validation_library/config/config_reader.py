from collections.abc import Mapping, Sequence

from pydantic import ValidationError

from peh_validation_library.core.check.schemas import (
    CaseCheckExpression,
    SimpleCheckExpression,
)
from peh_validation_library.core.models.schemas import (
    CheckSchema,
    ColSchema,
    DFSchema,
)
from peh_validation_library.core.utils.enums import ErrorLevel


def get_df_schema(
    config_input: Mapping[str, str | Sequence | Mapping],
) -> DFSchema:
    try:
        return DFSchema(
            name=config_input['name'],
            columns=parse_columns(config_input['columns']),
            ids=(list(config_input['ids']) if 'ids' in config_input else None),
            metadata=config_input.get('metadata', None),
            checks=(
                parse_checks(config_input['checks'])
                if 'checks' in config_input
                else None
            ),
        )
    except (KeyError, TypeError, ValidationError, IndexError) as err:
        raise RuntimeError(f'Error reading configuration: {err}') from err


def parse_checks(
    checks: Sequence[Mapping[str, str | Sequence]],
) -> list[CheckSchema]:
    parsed_checks = []
    for check in checks:
        name = check.pop('name', None)
        error_level = check.pop('error_level', ErrorLevel.ERROR)
        error_message = check.pop('error_msg', None)
        try:
            check_command = CaseCheckExpression.model_validate(check)
        except ValidationError:
            check_command = SimpleCheckExpression.model_validate(check)

        parsed_checks.append(
            CheckSchema.get_schema(
                check_command=check_command,
                name=name,
                error_level=error_level,
                error_msg=error_message,
            )
        )
    return parsed_checks


def parse_columns(
    columns: Sequence[Mapping[str, str | Sequence]],
) -> list[ColSchema]:
    parsed_columns = []
    for column in columns:
        parsed_columns.append(
            ColSchema(
                id=column['id'],
                data_type=column['data_type'],
                nullable=column['nullable'],
                unique=column['unique'],
                required=column['required'],
                checks=(
                    parse_checks(column['checks'])
                    if 'checks' in column
                    else None
                ),
            )
        )
    return parsed_columns
