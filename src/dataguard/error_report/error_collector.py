from functools import cache

from dataguard.error_report.error_schemas import (
    ErrorCollectorSchema,
    ErrorReportSchema,
    ExceptionSchema,
)


@cache
class ErrorCollector:
    """ErrorCollector class for collecting errors during validation."""

    COUNTER = 0

    def __init__(self):
        self.__errors = []
        self.__exceptions = []

    def add_unknown_exception(
        self,
        exception: ExceptionSchema,
    ) -> None:
        """Adds an unknown exception to the collector.

        Args:
            exception (ExceptionSchema): The exception to add.

        Returns:
            None

        """
        self.__exceptions.append(exception)

    def add_error_report(
        self,
        error_report: ErrorReportSchema,
    ) -> None:
        """Adds an error report to the collector.

        Args:
            error_report (ErrorReportSchema): The error report to add.

        Returns:
            None

        """
        self.__errors.append(error_report)
        self.COUNTER += error_report.total_errors

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
