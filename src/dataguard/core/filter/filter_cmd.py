from __future__ import annotations

from types import SimpleNamespace

import polars as pl

from dataguard.core.check.check_cmd import (
    create_complex_expression,
    create_single_expression,
)
from dataguard.core.check.schemas import (
    CaseCheckExpression,
    SimpleCheckExpression,
)
from dataguard.core.utils.mappers import expression_mapper


def map_commands(
    expr: SimpleCheckExpression | CaseCheckExpression,
) -> None:
    if hasattr(expr, 'check_case'):
        for sub_expr in expr.expressions:
            map_commands(sub_expr)
    elif expr.command in expression_mapper:
        expr.map_command()


def build_filter_expression(
    expr: SimpleCheckExpression | CaseCheckExpression,
) -> pl.Expr:
    null_data = SimpleNamespace(key=None)
    if isinstance(expr, CaseCheckExpression):
        return create_complex_expression(null_data, expr)
    return create_single_expression(null_data, expr)


def get_filter_fn(
    dataframe: pl.DataFrame | pl.LazyFrame,
    expr: pl.Expr,
) -> pl.DataFrame | pl.LazyFrame:
    return dataframe.filter(expr)
