from pydantic import BaseModel, ConfigDict

from dataguard.core.utils.enums import ErrorLevel


class BasicExceptionSchema(BaseModel):
    """Basic schema for exceptions.

    Attributes:
        type (str): Type of the error.
        message (str): Message describing the error.

    """

    type: str
    message: str
    level: ErrorLevel


class ExceptionSchema(BasicExceptionSchema):
    """Schema for unknown exceptions that occur during validation.

    Attributes:
        type (str): Type of the error.
        message (str): Message describing the error.
        level (ErrorLevel): Level of the error.
        traceback (str): Traceback of the error.

    """

    traceback: str


class ErrorSchema(BasicExceptionSchema):
    """Schema for errors that occur during validation.

    Attributes:
        type (str): Type of the error.
        message (str): Message describing the error.
        title (str): Title of the error.
        traceback (str): Traceback of the error.
    """

    title: str
    traceback: str


class DFErrorSchema(ErrorSchema):
    """Schema for errors that occur during DataFrame validation.

    Attributes:
        column_names (list[str] | str): Names of the columns where the error occurred.
        row_ids (list[int]): IDs of the rows where the error occurred.
        idx_columns (list[str]): Index columns used for identifying errors.
        level (str): Level of the error, e.g., 'error', 'warning'.
        message (str): Message describing the error.
        title (str): Title of the error.

    """  # noqa: E501

    column_names: list[str]
    row_ids: list[int]
    idx_columns: list[str]
    title: str
    traceback: str | None = None


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
    id: str

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ErrorCollectorSchema(BaseModel):
    """Schema for collecting errors and exceptions during validation.

    Attributes:
        error_reports (list[ErrorReportSchema]): List of error reports.
        exceptions (list[ExceptionSchema]): List of exceptions that occurred during validation.

    """  # noqa: E501

    error_reports: list[ErrorReportSchema] = []
    exceptions: list[ExceptionSchema] = []
