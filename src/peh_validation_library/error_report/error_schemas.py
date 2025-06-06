from pydantic import BaseModel

from peh_validation_library.core.utils.enums import ErrorLevel


class ExceptionSchema(BaseModel):
    error_type: str
    error_message: str
    error_level: ErrorLevel
    error_traceback: str
    error_context: str | None = None
    error_source: str | None = None


class ErrorSchema(BaseModel):
    column_names: list[str] | str
    row_ids: list[int]
    idx_columns: list[str]
    level: str
    message: str
    title: str


class ErrorReportSchema(BaseModel):
    name: str
    errors: list[ErrorSchema]
    total_errors: int
    id: int


class ErrorCollectorSchema(BaseModel):
    error_reports: list[ErrorReportSchema] = []
    exceptions: list[ExceptionSchema] = []
