import polars as pl

import pytest
from dataguard.dataframe.df_reader import read_dataframe


def test_df_reader():
    df = {
        'col_a': [1, 2, 3],
        'col_b': ['x', 'y', 'z'],
        'col_c': [True, False, True],
    }

    out = read_dataframe(df, schema=None)

    assert isinstance(out, pl.DataFrame)
    assert out.shape == (3, 3)
    assert out.columns == ['col_a', 'col_b', 'col_c']
    assert out['col_a'].dtype == pl.Int64
    assert out['col_b'].dtype == pl.Utf8
    assert out['col_c'].dtype == pl.Boolean

def test_df_reader_with_schema():    
    df = {
        'col_a': [1, 2, 3],
        'col_b': ['x', 'y', 'z'],
        'col_c': [True, False, True],
    }

    schema = {
        'col_a': pl.Int32,
        'col_b': pl.Utf8,
        'col_c': pl.Boolean,
    }

    out = read_dataframe(df, schema=schema)

    assert isinstance(out, pl.DataFrame)
    assert out.shape == (3, 3)
    assert out.columns == ['col_a', 'col_b', 'col_c']
    assert out['col_a'].dtype == pl.Int32
    assert out['col_b'].dtype == pl.Utf8
    assert out['col_c'].dtype == pl.Boolean


def test_df_reader_empty_frame():    
    df = {
        'col_a': [],
        'col_b': [],
        'col_c': [],
    }

    out = read_dataframe(df)

    assert isinstance(out, pl.DataFrame)
    assert out.shape == (0, 3)
    assert out.columns == ['col_a', 'col_b', 'col_c']


def test_df_reader_invalid_data_shape():
    df = {
        'col_a': [1, 2],
        'col_b': ['x', 'y', 'z'],
        'col_c': [True, False, True],
    }

    with pytest.raises(RuntimeError) as err:
        read_dataframe(df)
    
    assert "Error reading dataframe" in str(err.value)


def test_df_reader_invalid_data_schema():
    df = {
        'col_a': [1, 2, 3],
        'col_b': ['x', 'y', 'z'],
        'col_c': [True, False, True],
    }
    
    schema = {
        'col_a': pl.Int32,
        'col_b': pl.Int32,  # Invalid type
        'col_c': pl.Boolean,
    }

    with pytest.raises(RuntimeError) as err:
        read_dataframe(df, schema=schema)
    
    assert "Error reading dataframe:" in str(err.value)


def test_df_reader_invalid_data_object():
    df = list(range(10))

    with pytest.raises(RuntimeError) as err:
        read_dataframe(df)
    
    assert "Error reading dataframe:" in str(err.value)


def test_df_reader_none_data_object():

    with pytest.raises(RuntimeError) as err:
        read_dataframe(None)
    
    assert "Error reading dataframe:" in str(err.value)
