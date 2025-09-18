import logging
import pytest
import polars as pl
from pydantic import ValidationError
from dataguard.error_report.error_collector import ErrorCollector

import pandera.polars as pa

from dataguard.error_report.handlers import (
    exception_handler,
    error_handler,
    pandera_schema_errors_handler,
    parse_schema_error,
)
from dataguard.error_report.error_schemas import (
    ErrorSchema,
    ErrorReportSchema,
    ExceptionSchema,
    DFErrorSchema,
)

class DummyLogger(logging.Logger):
    def __init__(self):
        super().__init__("dummy")
        self.messages = []

    def error(self, msg, *args, **kwargs):
        self.messages.append(("error", msg))

    def info(self, msg, *args, **kwargs):
        self.messages.append(("info", msg))

@pytest.fixture(autouse=True)
def clear_error_collector():
    ErrorCollector().clear_errors()
    yield
    ErrorCollector().clear_errors()

def test_exception_handler_lazy_true_adds_exception():
    logger = DummyLogger()
    exc = ValueError("test error")
    exception_handler(exc, lazy=True, err_level="critical", logger=logger)
    collected = ErrorCollector().get_errors().exceptions
    assert len(collected) == 1
    assert collected[0].type == "ValueError"
    assert "test error" in collected[0].message

def test_exception_handler_lazy_false_raises():
    logger = DummyLogger()
    exc = RuntimeError("fail")
    with pytest.raises(RuntimeError):
        exception_handler(exc, lazy=False, err_level="error", logger=logger)

def test_error_handler_lazy_true_adds_error():
    logger = DummyLogger()
    err = Exception("err")
    error_handler(err, err_level="warning", message='note1', lazy=True, logger=logger)
    reports = ErrorCollector().get_errors().error_reports
    assert len(reports) == 1
    report = reports[0]
    assert report.total_errors == 1
    assert report.errors[0].message == "note1"
    assert report.errors[0].type == "Exception"

def test_error_handler_lazy_false_raises():
    logger = DummyLogger()
    err = pl.PolarsError("polars fail")
    with pytest.raises(pl.PolarsError):
        error_handler(err, err_level="critical", lazy=False, logger=logger)

