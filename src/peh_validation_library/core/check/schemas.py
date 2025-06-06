from __future__ import annotations

from typing import Any, Callable

import pandera.polars as pa
import polars as pl
from pydantic import BaseModel, Field

from peh_validation_library.core.utils.enums import CheckCases
from peh_validation_library.core.utils.mappers import (
    expression_mapper,
)


class SimpleCheckExpression(BaseModel):
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
                f'Column(s) {self.get_args_string(self.subject)} '
                f'{self.get_check_title().lower()}'
            )
        if self.arg_values:
            msg += f' {self.get_args_string(self.arg_values)}'
        elif self.arg_columns:
            msg += f' {self.get_args_string(self.arg_columns)}'
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

    @staticmethod
    def get_args_string(args: list[Any]) -> str:
        if not args:
            return ''
        if len(args) == 1:
            return f'"{args[0]}"'
        return args


class CaseCheckExpression(BaseModel):
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
            case _:
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
            case _:
                raise ValueError(f'Unknown check case: {self.check_case}')

    def get_args(self) -> dict[str, Any]:
        args = []
        for exp in self.expressions:
            args.append(exp.get_args())
        return args
