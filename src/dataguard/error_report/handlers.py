import logging
import traceback
from uuid import uuid4

import pandera.polars as pa
import polars as pl
from pydantic import ValidationError

from dataguard.core.utils.enums import ErrorLevel
from dataguard.error_report.error_collector import ErrorCollector
from dataguard.error_report.error_schemas import (
    DFErrorSchema,
    ErrorReportSchema,
    ErrorSchema,
    ExceptionSchema,
)
from dataguard.error_report.utils import create_row_idx


def exception_handler(
    err: Exception,
    lazy: bool,
    err_level: str,
    logger: logging.Logger,
) -> None:
    logger.error(f'An unknown exception occurred: {str(err)}', exc_info=err)

    if not lazy:
        raise err

    error_traceback = traceback.format_exc()
    logger.error(f'Unknown exception traceback: {error_traceback}')

    exc_schema = ExceptionSchema(
        type=type(err).__name__,
        message=str(err),
        level=err_level,
        traceback=error_traceback,
    )

    ErrorCollector().add_unknown_exception(exc_schema)


def error_handler(
    err: Exception | ValidationError | pl.exceptions.PolarsError,
    err_level: str,
    message: str | None = None,
    lazy: bool = True,
    logger: logging.Logger = logging.getLogger(__name__),
) -> None:
    logger.error(f'Error occurred: {message}', exc_info=err)

    if not lazy:
        raise err

    error_traceback = traceback.format_exc()
    logger.error(f'Error traceback: {error_traceback}')

    err_schema = ErrorSchema(
        level=err_level,
        message=message if message else str(err),
        title=f'{type(err).__name__}: {str(err)}',
        type=type(err).__name__,
        traceback=error_traceback,
    )

    err_report = ErrorReportSchema(
        name='Critical Error Report',
        errors=[err_schema],
        total_errors=1,
        id=str(uuid4()),
    )

    ErrorCollector().add_error_report(err_report)


def pandera_schema_errors_handler(
    err: pa.errors.SchemaErrors | pa.errors.SchemaError,
    lazy: bool = False,
    logger: logging.Logger = logging.getLogger(__name__),
) -> None:
    logger.info(f'Processing pandera schema errors: {str(err)}')

    if not lazy:
        raise err

    idx_columns = (
        getattr(err.schema, 'unique', [])
        if not getattr(err.schema, 'unique', None)
        else []
    )

    errors = []
    if getattr(err, 'schema_errors', None):
        for schema_error in err.schema_errors:
            df_error = parse_schema_error(schema_error, idx_columns)
            errors.append(df_error)
            if df_error.level == ErrorLevel.CRITICAL:
                logger.warning(
                    'Critical error found, stopping further error processing'
                )
                break

    else:
        df_error = parse_schema_error(
            err, idx_columns, error_level=ErrorLevel.CRITICAL.value
        )
        errors.append(df_error)

    err_report = ErrorReportSchema(
        name=err.schema.name,
        errors=errors,
        total_errors=len(errors),
        id=str(uuid4()),
    )

    logger.info(f'Adding error report with {len(errors)} errors to collector')
    ErrorCollector().add_error_report(err_report)


def parse_schema_error(
    schema_error: pa.errors.SchemaError,
    idx_columns: list[str],
    error_level: str = ErrorLevel.ERROR.value,
) -> DFErrorSchema:
    return DFErrorSchema(
        type=str(schema_error.reason_code),
        message=str(schema_error),
        level=(
            schema_error.check.name
            if getattr(schema_error.check, 'name', None)
            else error_level
        ),
        title=(
            schema_error.check.title
            if isinstance(schema_error.check.title, str)
            else schema_error.check.title()
        ),
        column_names=(
            list(schema_error.schema.columns.keys())
            if getattr(schema_error.schema, 'columns', None)
            else [schema_error.schema.name]
        ),
        row_ids=create_row_idx(schema_error.check_output),
        idx_columns=idx_columns,
    )
