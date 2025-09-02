from pydantic import BaseModel

from dataguard.core.utils.enums import ErrorLevel


class ExceptionSchema(BaseModel):
    """Schema for exceptions that occur during validation.

    Attributes:
        error_type (str): Type of the error.
        error_message (str): Message describing the error.
        error_level (ErrorLevel): Level of the error.
        error_traceback (str): Traceback of the error.
        error_context (str | None): Context of the error, if available.
        error_source (str | None): Source of the error, if available.

    """

    error_type: str
    error_message: str
    error_level: ErrorLevel
    error_traceback: str
    error_context: list[str] | None = None
    error_source: str | None = None


class ErrorSchema(BaseModel):
    """Schema for errors that occur during DataFrame validation.

    Attributes:
        column_names (list[str] | str): Names of the columns where the error occurred.
        row_ids (list[int]): IDs of the rows where the error occurred.
        idx_columns (list[str]): Index columns used for identifying errors.
        level (str): Level of the error, e.g., 'error', 'warning'.
        message (str): Message describing the error.
        title (str): Title of the error.

    """  # noqa: E501

    column_names: list[str] | str
    row_ids: list[int]
    idx_columns: list[str]
    level: str
    message: str
    title: str


class ErrorReportSchema(BaseModel):
    """Schema for error reports generated during validation.

    Attributes:
        name (str): Name of the error report.
        errors (list[ErrorSchema]): List of errors found in the DataFrame.
        total_errors (int): Total number of errors in the report.
        id (int): Unique identifier for the error report.

    """

    name: str
    errors: list[ErrorSchema]
    total_errors: int
    id: int


class ErrorCollectorSchema(BaseModel):
    """Schema for collecting errors and exceptions during validation.

    Attributes:
        error_reports (list[ErrorReportSchema]): List of error reports.
        exceptions (list[ExceptionSchema]): List of exceptions that occurred during validation.

    """  # noqa: E501

    error_reports: list[ErrorReportSchema] = []
    exceptions: list[ExceptionSchema] = []
