import pandera.polars as pa

from dataguard.core.models.schemas import CheckSchema, ColSchema, DFSchema
from dataguard.core.utils.enums import ValidationType, ErrorLevel


def test_col_schema_build():
    check_schema = CheckSchema(
        name="test_check",
        fn=lambda df, args: df,
        args_={"key": "value"},
        error_level=ErrorLevel.ERROR,
        error_msg="Test error message"
    )

    col_schema = ColSchema(
        id="test_column",
        data_type=ValidationType.STR,
        nullable=False,
        unique=True,
        required=True,
        checks=[check_schema]
    )

    built_column = col_schema.build()
    assert isinstance(built_column, pa.Column)
    assert built_column.name == "test_column"
    assert built_column.nullable is False
    assert built_column.unique is True
    assert built_column.required is True
    assert len(built_column.checks) == 1

def test_df_schema_build():
    check_schema = CheckSchema(
        name="test_check",
        fn=lambda df, args: df,
        args_={"key": "value"},
        error_level=ErrorLevel.ERROR,
        error_msg="Test error message"
    )

    col_schema = ColSchema(
        id="test_column",
        data_type=ValidationType.STR,
        nullable=False,
        unique=True,
        required=True,
        checks=[check_schema]
    )

    df_schema = DFSchema(
        name="test_dataframe",
        columns=[col_schema],
        ids=["test_column"],
        metadata={"meta_key": "meta_value"},
        checks=[check_schema]
    )

    built_df = df_schema.build()
    assert isinstance(built_df, pa.DataFrameSchema)
    assert built_df.name == "test_dataframe"
    assert "test_column" in built_df.columns
    assert built_df.metadata == {"meta_key": "meta_value"}
    assert len(built_df.checks) == 1