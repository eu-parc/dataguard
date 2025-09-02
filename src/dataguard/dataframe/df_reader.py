from collections.abc import Sequence

import polars as pl


def read_dataframe(
    data: dict[str, Sequence], schema: dict[str, str] | None = None
) -> pl.LazyFrame:
    return pl.from_dict(data, schema=schema)
