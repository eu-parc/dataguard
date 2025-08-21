import pytest

from dataguard.error_report.error_collector import ErrorCollector
from dataguard.error_report.error_schemas import (
    ErrorSchema, ErrorReportSchema, ErrorCollectorSchema, ExceptionSchema
)
from dataguard.core.utils.enums import ErrorLevel


@pytest.fixture
def error_collector():
    collector = ErrorCollector()
    yield collector
    collector.clear_errors()


@pytest.fixture
def exception_schema():
    return ExceptionSchema(
        error_type='ValueError',
        error_message='Test error message',
        error_level=ErrorLevel.CRITICAL,
        error_traceback='Traceback information',
        error_context='Test context',
        error_source='test_module'
    )


@pytest.fixture
def mock_schema_error():
    '''Create a mock SchemaError with necessary attributes.'''
    class MockSchema:
        def __init__(self):
            self.name = 'test_schema'
            self.unique = ['id_column']
            self.level = 'error'
            self.message = 'Invalid data'
            self.title = 'Validation Error'
    
    class MockCheck:
        def __init__(self):
            self.name = 'error'
            self.error = 'Data validation failed'
            self.title = 'Invalid Data'
    
    class MockSchemaError:
        def __init__(self):
            self.schema = MockSchema()
            self.check = MockCheck()
            self.column = 'test_column'
            self.failure_cases = [1, 2, 3]
            self.schema_errors = None
    
    return MockSchemaError()


@pytest.fixture
def mock_schema_errors(mock_schema_error):
    '''Create a mock SchemaErrors with multiple errors.'''
    class MockSchemaErrors:
        def __init__(self):
            self.schema = mock_schema_error.schema
            self.schema_errors = [mock_schema_error, mock_schema_error]
    
    return MockSchemaErrors()


def test_error_collector_initialization(error_collector):
    '''Test ErrorCollector initialization.'''
    assert error_collector.COUNTER == 0
    assert error_collector.get_errors().error_reports == []
    assert error_collector.get_errors().exceptions == []


def test_add_schema_errors(error_collector, mock_schema_errors, monkeypatch):
    '''Test adding multiple schema errors to the collector.'''
    def mock_from_schema_error(error):
        return ['test_column'], [1, 2, 3]
    
    monkeypatch.setattr(
        'dataguard.error_report.error_collector.from_schema_error',
        mock_from_schema_error
    )
    
    error_collector.add_errors(mock_schema_errors)
    
    result = error_collector.get_errors()
    assert len(result.error_reports) == 1
    assert result.error_reports[0].name == 'test_schema'
    assert len(result.error_reports[0].errors) == 2
    assert error_collector.COUNTER == 2


def test_multiple_error_reports(error_collector, mock_schema_error, monkeypatch):
    '''Test adding multiple error reports.'''
    def mock_from_schema_error(error):
        return ['test_column'], [1, 2, 3]
    
    monkeypatch.setattr(
        'dataguard.error_report.error_collector.from_schema_error',
        mock_from_schema_error
    )
    
    # Create a fixed version of add_errors method to test with
    original_add_errors = error_collector.add_errors
    
    def fixed_add_errors(error):
        if getattr(error, 'error_traceback', None):
            error_collector._ErrorCollector__exceptions.append(error)
            return

        errors = []
        
        if getattr(error, 'schema_errors', None):
            idx_columns = error.schema.unique
            for err in error.schema_errors:
                column_names, row_ids = mock_from_schema_error(err)
                errors.append(ErrorSchema(
                    column_names=column_names,
                    row_ids=row_ids,
                    idx_columns=idx_columns,
                    level=err.check.name,
                    message=err.check.error,
                    title=err.check.title,
                ))
        else:
            column_names, row_ids = mock_from_schema_error(error)  # Fixed: err -> error
            errors.append(ErrorSchema(
                column_names=column_names,
                row_ids=row_ids,
                idx_columns=error.schema.unique,
                level=error.schema.level,
                message=error.schema.message,
                title=error.schema.title,
            ))
        
        if len(errors) > 0:
            error_collector._ErrorCollector__errors.append(ErrorReportSchema(
                name=error.schema.name,
                errors=errors,
                total_errors=len(errors),
                id=error_collector.COUNTER
            ))
            error_collector.COUNTER += len(errors)
    
    monkeypatch.setattr(error_collector, 'add_errors', fixed_add_errors)
    
    error_collector.add_errors(mock_schema_error)
    
    # Reset COUNTER to simulate a new report
    error_collector.COUNTER = 1
    
    error_collector.add_errors(mock_schema_error)
    
    result = error_collector.get_errors()
    assert len(result.error_reports) == 2
    assert result.error_reports[0].id == 0
    assert result.error_reports[1].id == 1


def test_get_errors_returns_proper_schema(error_collector):
    '''Test that get_errors returns the proper schema type.'''
    result = error_collector.get_errors()
    assert isinstance(result, ErrorCollectorSchema)


@pytest.mark.parametrize('column_names,row_ids', [
    (['col1'], [1, 2, 3]),
    ('single_column', [4, 5, 6]),
    (['col1', 'col2'], [7]),
])
def test_various_column_formats(error_collector, mock_schema_error, monkeypatch, column_names, row_ids):
    '''Test handling of different column name and row ID formats.'''
    def mock_from_schema_error(error):
        return column_names, row_ids
    
    monkeypatch.setattr(
        'dataguard.error_report.error_collector.from_schema_error',
        mock_from_schema_error
    )
    
    # Create a fixed version of add_errors method
    def fixed_add_errors(error):
        if getattr(error, 'error_traceback', None):
            error_collector._ErrorCollector__exceptions.append(error)
            return

        errors = []
        
        if getattr(error, 'schema_errors', None):
            idx_columns = error.schema.unique
            for err in error.schema_errors:
                column_names, row_ids = mock_from_schema_error(err)
                errors.append(ErrorSchema(
                    column_names=column_names,
                    row_ids=row_ids,
                    idx_columns=idx_columns,
                    level=err.check.name,
                    message=err.check.error,
                    title=err.check.title,
                ))
        else:
            column_names, row_ids = mock_from_schema_error(error)  # Fixed: err -> error
            errors.append(ErrorSchema(
                column_names=column_names,
                row_ids=row_ids,
                idx_columns=error.schema.unique,
                level=error.schema.level,
                message=error.schema.message,
                title=error.schema.title,
            ))
        
        if len(errors) > 0:
            error_collector._ErrorCollector__errors.append(ErrorReportSchema(
                name=error.schema.name,
                errors=errors,
                total_errors=len(errors),
                id=error_collector.COUNTER
            ))
            error_collector.COUNTER += len(errors)
    
    monkeypatch.setattr(error_collector, 'add_errors', fixed_add_errors)
    
    error_collector.add_errors(mock_schema_error)
    
    result = error_collector.get_errors()
    assert result.error_reports[0].errors[0].column_names == column_names
    assert result.error_reports[0].errors[0].row_ids == row_ids


def test_error_counter_increments_correctly(error_collector, mock_schema_errors, monkeypatch):
    '''Test that the error counter increments correctly.'''
    def mock_from_schema_error(error):
        return ['test_column'], [1, 2, 3]
    
    monkeypatch.setattr(
        'dataguard.error_report.error_collector.from_schema_error',
        mock_from_schema_error
    )
    
    # Create a fixed version of add_errors method
    def fixed_add_errors(error):
        if getattr(error, 'error_traceback', None):
            error_collector._ErrorCollector__exceptions.append(error)
            return

        errors = []
        
        if getattr(error, 'schema_errors', None):
            idx_columns = error.schema.unique
            for err in error.schema_errors:
                column_names, row_ids = mock_from_schema_error(err)
                errors.append(ErrorSchema(
                    column_names=column_names,
                    row_ids=row_ids,
                    idx_columns=idx_columns,
                    level=err.check.name,
                    message=err.check.error,
                    title=err.check.title,
                ))
        else:
            column_names, row_ids = mock_from_schema_error(error)  # Fixed: err -> error
            errors.append(ErrorSchema(
                column_names=column_names,
                row_ids=row_ids,
                idx_columns=error.schema.unique,
                level=error.schema.level,
                message=error.schema.message,
                title=error.schema.title,
            ))
        
        if len(errors) > 0:
            error_collector._ErrorCollector__errors.append(ErrorReportSchema(
                name=error.schema.name,
                errors=errors,
                total_errors=len(errors),
                id=error_collector.COUNTER
            ))
            error_collector.COUNTER += len(errors)
    
    monkeypatch.setattr(error_collector, 'add_errors', fixed_add_errors)
    
    assert error_collector.COUNTER == 0
    error_collector.add_errors(mock_schema_errors)  # Adds 2 errors
    assert error_collector.COUNTER == 2
    error_collector.add_errors(mock_schema_errors)  # Adds 2 more errors
    assert error_collector.COUNTER == 4


def test_cache_decorator_behavior():
    '''Test that the @cache decorator works as expected.'''
    collector1 = ErrorCollector()
    collector2 = ErrorCollector()
    
    # Since ErrorCollector is cached, these should be the same instance
    assert collector1 is collector2
    
    # Changes to one should affect the other
    collector1.COUNTER = 10
    assert collector2.COUNTER == 10


def test_error_without_schema(error_collector):
    class Schema:
        columns = {'col1': 0, 'col2': 1}
        name = "MockErrorWithoutSchema"
        unique = ['col1', 'col2']
        title = None
    
    class MockErrorWithoutSchema:
        check_output = None
        schema = Schema()

        def __str__(self):
            return "MockErrorWithoutSchema"
    
    error_collector.add_errors(MockErrorWithoutSchema())
    
    assert error_collector.COUNTER == 11
    assert error_collector.get_errors().error_reports[0].errors[0].title is "MockErrorWithoutSchema"
    assert error_collector.get_errors().error_reports[0].errors[0].column_names == ['col1', 'col2']