import polars as pl


def create_row_idx(df: pl.DataFrame | None) -> list[int]:
    if df is None or 'check_output' not in df.columns:
        return []
    return (
        df.lazy()
        .with_columns(index=pl.arange(0, pl.count('check_output')))
        .filter(pl.col('check_output').eq(False))
        .collect()
        .get_column('index')
        .to_list()
    )
