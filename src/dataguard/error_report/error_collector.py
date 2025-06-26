from functools import cache

import pandera.polars as pa

from dataguard.error_report.error_schemas import (
    ErrorCollectorSchema,
    ErrorReportSchema,
    ErrorSchema,
    ExceptionSchema,
)
from dataguard.error_report.utils import from_schema_error


@cache
class ErrorCollector:
    """ErrorCollector class for collecting errors during validation."""

    COUNTER = 0

    def __init__(self):
        self.__errors = []
        self.__exceptions = []

    def add_errors(
        self,
        error: ExceptionSchema
        | pa.errors.SchemaError
        | pa.errors.SchemaErrors,  # noqa: E501
    ) -> None:
        """Adds errors to the collector.

        Args:
            error (ExceptionSchema | pa.errors.SchemaError | pa.errors.SchemaErrors): The error to add.

        Returns:
            None

        """  # noqa: E501
        if getattr(error, 'error_traceback', None):
            self.__exceptions.append(error)
            return

        errors = []

        if getattr(error, 'schema_errors', None):
            idx_columns = error.schema.unique
            for err in error.schema_errors:
                column_names, row_ids = from_schema_error(err)

                if not isinstance(err.check, str):
                    errors.append(
                        ErrorSchema(
                            column_names=column_names,
                            row_ids=row_ids,
                            idx_columns=idx_columns,
                            level=err.check.name,
                            message=err.check.error,
                            title=err.check.title,
                        )
                    )
                else:
                    errors.append(
                        ErrorSchema(
                            column_names=column_names,
                            row_ids=row_ids,
                            idx_columns=idx_columns,
                            level='error',
                            message=err.check,
                            title=err.check,
                        )
                    )

        else:
            column_names, row_ids = from_schema_error(err)
            errors.append(
                ErrorSchema(
                    column_names=column_names,
                    row_ids=row_ids,
                    idx_columns=error.schema.unique,
                    level=error.schema.level,
                    message=error.schema.message,
                    title=error.schema.title,
                )
            )

        if len(errors) > 0:
            self.__errors.append(
                ErrorReportSchema(
                    name=error.schema.name,
                    errors=errors,
                    total_errors=len(errors),
                    id=self.COUNTER,
                )
            )
            self.COUNTER += len(errors)

    def get_errors(self) -> ErrorCollectorSchema:
        """Returns the collected errors and exceptions.

        Returns:
            ErrorCollectorSchema: A schema containing the collected errors and exceptions.

        """  # noqa: E501
        return ErrorCollectorSchema(
            error_reports=self.__errors, exceptions=self.__exceptions
        )

    def clear_errors(self) -> None:
        """Clears the collected errors and exceptions."""
        self.__errors.clear()
        self.__exceptions.clear()
        self.COUNTER = 0
