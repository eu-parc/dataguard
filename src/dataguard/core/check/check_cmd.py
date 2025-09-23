from collections.abc import Callable
from typing import Any

import pandera.polars as pa
import polars as pl

from dataguard.core.check.schemas import (
    CaseCheckExpression,
    SimpleCheckExpression,
)
from dataguard.core.utils.enums import CheckCases

CheckFn = Callable[[pa.PolarsData, Any], pl.Expr]


def get_column_subject_expression(
    data: pa.PolarsData, simple_check_expr: SimpleCheckExpression
) -> pl.Expr:
    if not data.key:
        return pl.col(simple_check_expr.subject)

    pl_col = pl.col(data.key)
    if subject := simple_check_expr.subject:
        pl_col = pl.col(subject[0])

    return pl_col


def create_single_expression(
    data, simple_check_expr: SimpleCheckExpression
) -> pl.Expr:
    pl_col = get_column_subject_expression(data, simple_check_expr)

    if arg_values := simple_check_expr.arg_values:
        if len(arg_values) == 1:
            exp_arg = arg_values[0]
        # Due to Polars API, eq needs a Series for multiple values
        # https://github.com/pola-rs/polars/pull/22178
        # https://github.com/pola-rs/polars/issues/22149
        elif simple_check_expr.command == 'eq':
            exp_arg = pl.Series(values=arg_values)
        else:
            exp_arg = arg_values

    if arg_column := simple_check_expr.arg_columns:
        exp_arg = pl.col(arg_column[0])

    exp = getattr(pl_col, simple_check_expr.command)

    if (not arg_values) and (not arg_column):
        return exp()

    return exp(exp_arg)


def get_single_expression(simple_check_expr: SimpleCheckExpression) -> CheckFn:
    def single_expression(
        data,
        arg_values=None,
        arg_columns=None,
        subject=None,
    ):
        return create_single_expression(data, simple_check_expr)

    return single_expression


def create_complex_expression(
    data, case_check_expr: CaseCheckExpression
) -> pl.Expr:
    if hasattr(case_check_expr.expressions[0], 'check_case'):
        exp_1 = create_complex_expression(data, case_check_expr.expressions[0])
    else:
        exp_1 = create_single_expression(data, case_check_expr.expressions[0])

    if hasattr(case_check_expr.expressions[1], 'check_case'):
        exp_2 = create_complex_expression(data, case_check_expr.expressions[1])
    else:
        exp_2 = create_single_expression(data, case_check_expr.expressions[1])

    match case_check_expr.check_case:
        case CheckCases.CONDITION:
            return pl.when(exp_1).then(exp_2)
        case CheckCases.CONJUNCTION:
            return exp_1.and_(exp_2)
        case CheckCases.DISJUNCTION:
            return exp_1.or_(exp_2)
        case _:  # pragma: no cover
            # This is unreachable due to pydantic validation
            raise ValueError(
                f'Invalid check_case: {case_check_expr.check_case}'
            )


def get_complex_expression(case_check_expr: CaseCheckExpression) -> CheckFn:
    def complex_expression(
        data,
        *params,
        **kwargs,
    ) -> CheckFn:
        return create_complex_expression(data, case_check_expr)

    return complex_expression


def get_expression(
    check_expr: SimpleCheckExpression | CaseCheckExpression,
) -> CheckFn:
    if hasattr(check_expr, 'check_case'):
        return get_complex_expression(check_expr)

    return get_single_expression(check_expr)


def get_check_fn(data: pa.PolarsData, exp: pl.Expr, **kwargs) -> pl.LazyFrame:
    return data.lazyframe.select(exp(data))
