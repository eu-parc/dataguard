from collections.abc import Sequence

import polars as pl


def read_dataframe(
    data: dict[str, Sequence], schema: dict[str, str] | None = None
) -> pl.LazyFrame:
    try:
        return pl.from_dict(data, schema=schema)
    except (pl.exceptions.ShapeError, TypeError, AttributeError) as err:
        raise RuntimeError(f'Error reading dataframe: {err}') from err
