from __future__ import annotations

from typing import Any, Callable

import pandera.polars as pa
import polars as pl
from pydantic import BaseModel, Field

from dataguard.core.utils.enums import CheckCases
from dataguard.core.utils.mappers import (
    expression_mapper,
)


class SimpleCheckExpression(BaseModel):
    """Schema for a simple validation check expression.

    Attributes:
        command (str | Callable): The command or function to execute the check.
        subject (list[str] | None): List of column names to apply the check on.
        arg_values (list[Any] | None): List of values to pass as arguments to the command.
        arg_columns (list[str] | None): List of column names to pass as arguments to the command.

    """  # noqa: E501

    command: str | Callable[[pa.PolarsData, Any], pl.LazyFrame]
    subject: list[str] | None = None
    arg_values: list[Any] | None = None
    arg_columns: list[str] | None = None

    def get_check_title(self) -> str:
        try:
            return self.command.replace('_', ' ').capitalize()
        except AttributeError:
            return self.command.__name__.replace('_', ' ').capitalize()

    def get_check_message(self) -> str:
        msg = f'The column under validation {self.get_check_title().lower()}'

        if self.subject:
            msg = (
                f'Column(s) {get_args_string(self.subject)} '
                f'{self.get_check_title().lower()}'
            )
        if self.arg_values:
            msg += f' {get_args_string(self.arg_values)}'
        elif self.arg_columns:
            msg += f' {get_args_string(self.arg_columns)}'
        return msg

    def map_command(self) -> str:
        self.command = expression_mapper[self.command]

    def get_args(self) -> dict[str, Any]:
        args = {}
        if self.subject:
            args['subject'] = self.subject
        if self.arg_values:
            args['arg_values'] = self.arg_values
        if self.arg_columns:
            args['arg_columns'] = self.arg_columns
        return args


def get_args_string(args: list[Any]) -> str:
    if len(args) == 1:
        return f'"{args[0]}"'
    return args


class CaseCheckExpression(BaseModel):
    """Schema for a case-based validation check expression.

    Attributes:
        check_case (CheckCases): The type of case for the check (e.g., CONDITION, CONJUNCTION, DISJUNCTION).
        expressions (list[SimpleCheckExpression | CaseCheckExpression]): List of expressions to evaluate in the case.

    """  # noqa: E501

    check_case: CheckCases
    expressions: list[SimpleCheckExpression | CaseCheckExpression] = Field(
        min_length=2, max_length=2
    )

    def get_check_title(self) -> str:
        match self.check_case:
            case CheckCases.CONDITION:
                return (
                    f'When {self.expressions[0].get_check_title()}, Then '
                    f'{self.expressions[1].get_check_title()}'
                )
            case CheckCases.CONJUNCTION:
                return f'{" and ".join([e.get_check_title() for e in self.expressions])}'  # noqa: E501
            case CheckCases.DISJUNCTION:
                return f'{" or ".join([e.get_check_title() for e in self.expressions])}'  # noqa: E501
            case _:  # pragma: no cover
                # This is unreachable due to pydantic validation
                raise ValueError(f'Unknown check case: {self.check_case}')

    def get_check_message(self) -> str:
        match self.check_case:
            case CheckCases.CONDITION:
                return (
                    f'When {self.expressions[0].get_check_message()} Then '
                    f'{self.expressions[1].get_check_message()}'
                )
            case CheckCases.CONJUNCTION:
                return f'{" and ".join([e.get_check_message() for e in self.expressions])}'  # noqa: E501
            case CheckCases.DISJUNCTION:
                return f'{" or ".join([e.get_check_message() for e in self.expressions])}'  # noqa: E501
            case _:  # pragma: no cover
                # This is unreachable due to pydantic validation
                raise ValueError(f'Unknown check case: {self.check_case}')

    def get_args(self) -> dict[str, Any]:
        args = []
        for exp in self.expressions:
            args.append(exp.get_args())
        return args
