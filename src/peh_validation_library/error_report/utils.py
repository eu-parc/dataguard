import polars as pl


def from_schema_error(schema_error):
    row_ids = create_row_idx(schema_error.check_output)
    if getattr(schema_error.schema, 'columns', None):
        column_names = list(schema_error.schema.columns.keys())
        return column_names, row_ids
    return schema_error.schema.name, row_ids


def create_row_idx(df) -> list[int]:
    return (
        df.lazy()
        .with_columns(index=pl.arange(0, pl.count('check_output')))
        .filter(pl.col('check_output').eq(False))
        .collect()
        .get_column('index')
        .to_list()
    )
