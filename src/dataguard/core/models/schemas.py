from __future__ import annotations

from functools import partial
from typing import Any, Callable

import pandera.polars as pa
import polars as pl
from pydantic import BaseModel, ConfigDict

from dataguard.core.check.check_cmd import (
    get_check_fn,
    get_expression,
)
from dataguard.core.check.schemas import (
    CaseCheckExpression,
    SimpleCheckExpression,
)
from dataguard.core.utils.enums import (
    ErrorLevel,
    ValidationType,
)
from dataguard.core.utils.mappers import (
    expression_mapper,
    validation_type_mapper,
)


class CheckSchema(BaseModel):
    """Schema for a validation check.

    Attributes:
        name (str): Name of the check.
        fn (Callable): Function to execute the check.
        args_ (Any | None): Arguments for the check function.
        error_level (ErrorLevel): Level of error for the check.
        error_msg (str): Error message for the check.

    """

    name: str
    fn: Callable[[pa.PolarsData, Any], pl.LazyFrame]
    args_: Any | None
    error_level: ErrorLevel
    error_msg: str

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def get_schema(
        cls,
        check_command: SimpleCheckExpression | CaseCheckExpression,
        name: str | None = None,
        error_level: ErrorLevel = ErrorLevel.ERROR,
        error_msg: str | None = None,
    ) -> CheckSchema:
        """Creates a CheckSchema instance from a check command.

        Args:
            check_command (SimpleCheckExpression | CaseCheckExpression): The check command to create the schema from.
            name (str | None): Optional name for the check. If not provided, it will be derived from the command.
            error_level (ErrorLevel): The level of error for the check.
            error_msg (str | None): Optional error message for the check. If not provided, it will be derived from the command.

        Returns:
            CheckSchema: An instance of CheckSchema with the provided or derived values.

        """  # noqa: E501
        if not name:
            name = check_command.get_check_title()
        if not error_msg:
            error_msg = check_command.get_check_message()
        args_ = check_command.get_args()

        if hasattr(check_command, 'check_case'):
            exp = get_case_check(check_command)
            return cls(
                name=name,
                fn=partial(get_check_fn, exp=exp),
                args_=args_,
                error_level=error_level,
                error_msg=error_msg,
            )

        if check_command.command in expression_mapper:
            check_command.map_command()
            exp = get_expression(check_command)
            return cls(
                name=name,
                fn=partial(get_check_fn, exp=exp),
                args_=args_,
                error_level=error_level,
                error_msg=error_msg,
            )

        fn = check_command.command
        partial_fn = partial(
            fn,
            arg_values=check_command.arg_values,
            arg_columns=check_command.arg_columns,
            subject=check_command.subject,
        )

        return cls(
            name=name,
            fn=partial_fn,
            args_=args_,
            error_level=error_level,
            error_msg=error_msg,
        )

    def build(self):
        return pa.Check(
            self.fn,
            name=self.error_level.value,
            title=self.name,
            error=self.error_msg,
            statistics={'args_': self.args_},
        )


def get_case_check(check_command: CaseCheckExpression) -> str:
    for expression in check_command.expressions:
        if hasattr(expression, 'map_command'):
            expression.map_command()
        else:
            get_case_check(expression)
    return get_expression(check_command)


class ColSchema(BaseModel):
    """Schema for a DataFrame column.

    Attributes:
        id (str): Identifier for the column.
        data_type (ValidationType): Data type of the column.
        nullable (bool): Whether the column can contain null values.
        unique (bool): Whether the column values must be unique.
        required (bool): Whether the column is required.
        checks (list[CheckSchema] | None): Optional list of checks to apply to the column.

    """  # noqa: E501

    id: str
    data_type: ValidationType
    nullable: bool
    unique: bool
    required: bool
    checks: list[CheckSchema] | None

    def build(self):
        return pa.Column(
            validation_type_mapper[self.data_type],
            nullable=self.nullable,
            unique=self.unique,
            coerce=True,
            required=self.required,
            checks=(
                [check.build() for check in self.checks]
                if self.checks
                else None
            ),
            name=self.id,
        )


class DFSchema(BaseModel):
    """Schema for a DataFrame.

    Attributes:
        name (str): Name of the DataFrame schema.
        columns (list[ColSchema]): List of column schemas defining the DataFrame structure.
        ids (list[str] | None): Optional list of unique identifiers for the DataFrame.
        metadata (dict[str, Any] | None): Optional metadata for the DataFrame schema.
        checks (list[CheckSchema] | None): Optional list of checks to apply to the DataFrame.

    """  # noqa: E501

    name: str
    columns: list[ColSchema]
    ids: list[str] | None
    metadata: dict[str, Any] | None
    checks: list[CheckSchema] | None

    def build(self):
        return pa.DataFrameSchema(
            columns={col.id: col.build() for col in self.columns},
            unique=self.ids,
            name=self.name,
            unique_column_names=True,
            metadata=self.metadata,
            checks=(
                [check.build() for check in self.checks]
                if self.checks
                else None
            ),
        )
