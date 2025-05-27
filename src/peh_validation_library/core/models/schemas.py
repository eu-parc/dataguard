from __future__ import annotations

from functools import partial
from typing import Any, Callable

import pandera.polars as pa
import polars as pl
from pydantic import BaseModel, ConfigDict

from peh_validation_library.core.check.check_cmd import (
    get_check_fn,
    get_expression,
)
from peh_validation_library.core.check.schemas import (
    CaseCheckExpression,
    SimpleCheckExpression,
)
from peh_validation_library.core.utils.enums import (
    ErrorLevel,
    ValidationType,
)
from peh_validation_library.core.utils.mappers import (
    expression_mapper,
    validation_type_mapper,
)


class CheckSchema(BaseModel):
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
        if not name:
            name = check_command.get_check_title()
        if not error_msg:
            error_msg = check_command.get_check_message()
        args_ = check_command.get_args()

        if hasattr(check_command, 'check_case'):
            for expression in check_command.expressions:
                expression.map_command()
            exp = get_expression(check_command)
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


class ColSchema(BaseModel):
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
            coerce=False,
            required=self.required,
            checks=(
                [check.build() for check in self.checks]
                if self.checks
                else None
            ),
            name=self.id,
        )


class DFSchema(BaseModel):
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
