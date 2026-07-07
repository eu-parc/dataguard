from __future__ import annotations

from collections.abc import Mapping, Sequence

import polars as pl

from dataguard.config.config_reader import parse_filter
from dataguard.core.filter.filter_cmd import (
    build_filter_expression,
    get_filter_fn,
    map_commands,
)


class Filter:
    """Filter class for filtering DataFrames against a config-based expression."""  # noqa: E501

    def __init__(
        self,
        name: str,
        filter_expr: pl.Expr,
        columns: list[str] | None = None,
    ) -> None:
        self.name = name
        self._filter_expr = filter_expr
        self._columns = columns

    @classmethod
    def config_from_mapping(
        cls,
        config: Mapping[str, str | Sequence | Mapping],
    ) -> Filter:
        """Creates a Filter instance from a configuration mapping.

        Args:
            config (Mapping): Configuration mapping containing a ``name`` key,
                a ``filter`` key whose value is a check expression
                (``check_case`` / ``expressions`` or ``command`` / ``subject``
                / ``arg_values``), and an optional ``select`` key with a list
                of column names to keep.

        Returns:
            Filter: A Filter instance ready to apply to a DataFrame.

        Examples:
            >>> config = {
            ...     'name': 'adult belgians or brazilians',
            ...     'filter': {
            ...         'check_case': 'conjunction',
            ...         'expressions': [
            ...             {'command': 'is_greater_than_or_equal_to', 'subject': ['age'], 'arg_values': [18]},
            ...             {'command': 'is_in', 'subject': ['country'], 'arg_values': ['BE', 'BR']},
            ...         ],
            ...     },
            ...     'select': ['age', 'country'],
            ... }
            >>> filt = Filter.config_from_mapping(config)
            >>> filt.apply(df)

        """  # noqa: E501
        name, expr, columns = parse_filter(config)
        map_commands(expr)
        pl_expr = build_filter_expression(expr)
        return cls(name, pl_expr, columns)

    def apply(
        self,
        dataframe: pl.DataFrame | pl.LazyFrame,
    ) -> pl.DataFrame | pl.LazyFrame:
        """Applies the filter expression to the given DataFrame.

        Args:
            dataframe (pl.DataFrame | pl.LazyFrame): The DataFrame to filter.

        Returns:
            The filtered DataFrame, same type as the input.

        """
        result = get_filter_fn(dataframe, self._filter_expr)
        if self._columns:
            result = result.select(self._columns)
        return result
