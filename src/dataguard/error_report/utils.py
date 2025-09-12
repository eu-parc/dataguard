import logging
import traceback

import polars as pl

from dataguard.error_report.error_schemas import ExceptionSchema


def from_schema_error(schema_error):
    try:
        row_ids = create_row_idx(schema_error.check_output)
    except AttributeError:
        row_ids = []
    if getattr(schema_error.schema, 'columns', None):
        column_names = list(schema_error.schema.columns.keys())
        return column_names, row_ids
    return getattr(schema_error.schema, 'name', None), row_ids


def create_row_idx(df) -> list[int]:
    return (
        df.lazy()
        .with_columns(index=pl.arange(0, pl.count('check_output')))
        .filter(pl.col('check_output').eq(False))
        .collect()
        .get_column('index')
        .to_list()
    )


def exception_handler(
    err: Exception,
    return_exception: bool,
    err_msg: str,
    err_level: str,
    logger: logging.Logger,
) -> None:
    logger.error(f'Error occurred: {err_msg}', exc_info=err)

    if not return_exception:
        raise err

    error_traceback = traceback.format_exc()
    logger.error(f'Error traceback: {error_traceback}')

    tb = get_str_frame_list(traceback.extract_tb(err.__traceback__))

    return ExceptionSchema(
        error_type=type(err).__name__,
        error_message=str(err),
        error_level=err_level,
        error_traceback=error_traceback,
        error_context=tb if tb else None,
        error_source=tb[-1] if tb else None,
    )


def get_str_frame_list(tb: list[traceback.FrameSummary]) -> list[str]:
    return [f'{frame.filename}:{frame.lineno}:{frame.name}' for frame in tb]
