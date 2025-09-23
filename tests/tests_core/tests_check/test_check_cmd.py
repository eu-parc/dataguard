import pytest
import polars as pl
import pandera.polars as pa
from polars.testing.parametric import dataframes, column
from polars.testing import assert_frame_equal
from hypothesis import given, settings, strategies as st 

from dataguard.core.check.check_cmd import (
    create_single_expression,
    get_single_expression,
    get_expression,
    get_check_fn,
    create_complex_expression,
    get_complex_expression,
)
from dataguard.core.check.schemas import (
    SimpleCheckExpression,
    CaseCheckExpression
)
from dataguard.core.utils.enums import CheckCases


class TestCreateSingleColumnExpression:
    @pytest.mark.parametrize(
            'attr', [ 'eq', 'gt', 'lt'],
            )
    @given(df=dataframes(
            [
                column(
                    'col_a',  
                    strategy=st.floats(min_value=1, max_value=6), 
                    allow_null=True
                       ),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_single_arg(self, df, attr):
        data = pa.PolarsData(df, 'col_a')

        simple_check_expr = SimpleCheckExpression(
            command=attr,
            arg_values=[5,]
        )
        # Test (<exp>, single_arg)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(getattr(pl.col('col_a'), attr)(5)).collect()

        assert_frame_equal(result, expected_result)

    @pytest.mark.parametrize(
            'attr, params', [ ('eq', 'ac'), ('is_in', ('ac', 'ad'))],
            )
    @given(df=dataframes(
            [
                column(
                    'col_a',  
                     strategy=st.text(
                         alphabet=['a', 'b', 'c']
                         ),	
                    allow_null=True
                       ),
            ],
            min_size=5,
            max_size=20,
            lazy=True,
        ))
    def test_single_arg_str(self, df, attr, params):
        data = pa.PolarsData(df, 'col_a')

        simple_check_expr = SimpleCheckExpression(
            command=attr,
            arg_values=[params]
        )

        # Test (<exp>, single_arg)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        
        expected_result = df.select(getattr(pl.col('col_a'), attr)(params)).collect()
        
        assert_frame_equal(result, expected_result)

    @pytest.mark.parametrize(
            'attr', [ 'eq', 'gt'],
            )
    @given(df=dataframes(
            [
                column(
                    'col_a',  
                    strategy=st.integers(min_value=1, max_value=6), 
                    allow_null=True
                       ),
                column(
                    'col_b',  
                    strategy=st.integers(min_value=1, max_value=6), 
                    allow_null=True
                       ),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_single_other_arg(self, df, attr):
        data = pa.PolarsData(df, 'col_a')
        simple_check_expr = SimpleCheckExpression(
            command=attr,
            subject=['col_b'],
            arg_values=[5,]
        )
        # Test (<exp>, single_arg)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(
            getattr(pl.col('col_b'), attr)(5)
            ).collect()
        
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
            [
                column(
                    'col_a',  
                    strategy=st.integers(min_value=1, max_value=3), 
                    allow_null=True
                       ),
                column(
                    'col_b',  
                    strategy=st.integers(min_value=1, max_value=3), 
                    allow_null=True
                       ),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_column_name(self, df):
        data = pa.PolarsData(df, 'col_a')

        simple_check_expr = SimpleCheckExpression(
            command='eq',
            arg_columns=['col_b',]
        )
        
        # Test (<exp>, <col_name>)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(pl.col('col_a').eq(pl.col('col_b'))).collect()
        
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
            [
                column('col_a', strategy=st.datetimes(), allow_null=True),
                column('col_b', strategy=st.datetimes(), allow_null=True),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_no_args(self, df):
        data = pa.PolarsData(df, 'col_a')
        
        simple_check_expr = SimpleCheckExpression(
            command='is_null',
        )
        # Test (<exp>, )
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(pl.col('col_a').is_null()).collect()
        
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
            [
                column('col_a', allow_null=True),
                column('col_b', allow_null=True),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_other_no_args(self, df):
        data = pa.PolarsData(df, 'col_a')
        simple_check_expr = SimpleCheckExpression(
            command='is_null',
            subject=['col_b']
        )
        
        # Test (<exp>, )
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(pl.col('col_b').is_null()).collect()
        
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
            [
                column(
                    'col_a', 
                    strategy=st.integers(min_value=1, max_value=3), 
                    allow_null=True
                    ),
            ],
            min_size=5,
            max_size=5,
            lazy=True,
            ),
            test_col=st.lists(
                st.integers(min_value=1, max_value=3), min_size=5, max_size=5
                )
        )
    def test_seq(self, df, test_col):
        data = pa.PolarsData(df, 'col_a')

        simple_check_expr = SimpleCheckExpression(
            command='eq',	
            arg_values=test_col
        )
        
        # Test (<exp>, single_arg)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(
            pl.col('col_a').eq(pl.Series(values=test_col))
            ).collect()

        assert_frame_equal(result, expected_result)


class TestCreateSingleDfExpression:
    @pytest.mark.parametrize(
            'attr', [ 'eq', 'gt', 'lt'],
            )
    @given(df=dataframes(
            [
                column(
                    'col_a',  
                    strategy=st.integers(min_value=1, max_value=6), 
                    allow_null=True
                       ),
                column(
                    'col_b',  
                    strategy=st.integers(min_value=1, max_value=6), 
                    allow_null=True
                       ),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_single_arg(self, df, attr): 
        data = pa.PolarsData(df, None)

        simple_check_expr = SimpleCheckExpression(
            command=attr,	
            arg_values=[5,],
            subject=['col_a', 'col_b']
        )      
        # Test (<exp>, (<col_x>, <col_y>), single_arg)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(
            getattr(pl.col(('col_a', 'col_b')), attr)(5)
            ).collect()
        
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
            [
                column('col_a', strategy=st.dates(), allow_null=True),
                column('col_b', strategy=st.dates(), allow_null=True),
            ],
            max_size=20,
            lazy=True,
        ))
    def test_no_args(self, df):
        data = pa.PolarsData(df, None)

        simple_check_expr = SimpleCheckExpression(
            command='is_null',	
            subject=['col_a', 'col_b']
        ) 
        # Test (<exp>, (<col_x>, <col_y>), )
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(
            pl.col(('col_a', 'col_b')).is_null()
            ).collect()
        
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
            [
                column(
                    'col_a', 
                    strategy=st.integers(min_value=1, max_value=3), 
                    allow_null=True
                    ),
                column(
                    'col_b', 
                    strategy=st.integers(min_value=1, max_value=3), 
                    allow_null=True
                    ),
            ],
            min_size=5,
            max_size=5,
            lazy=True,
            ),
            test_col=st.lists(
                st.integers(min_value=1, max_value=3), min_size=5, max_size=5
                )
        )
    def test_seq(self, df, test_col): 
        data = pa.PolarsData(df, None)

        simple_check_expr = SimpleCheckExpression(
            command='eq',	
            arg_values=test_col,
            subject=['col_a', 'col_b']
        )      
        # Test (<exp>, single_arg)
        result = df.select(
            create_single_expression(data, simple_check_expr)
            ).collect()
        expected_result = df.select(
            pl.col(('col_a', 'col_b')).eq(pl.Series(values=test_col))
            ).collect()

        assert_frame_equal(result, expected_result)


class TestGetSingleExpression:
    @pytest.mark.parametrize(
        'attr', ['eq', 'gt', 'lt']
    )
    @given(df=dataframes(
        [
            column(
                'col_a',
                strategy=st.integers(min_value=1, max_value=6),
                allow_null=True
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_single_column_expression(self, df, attr):
        data = pa.PolarsData(df, 'col_a')

        simple_check_expr = SimpleCheckExpression(
            command=attr,	
            arg_values=[5,],
        ) 
        single_expression = get_single_expression(simple_check_expr)

        # Test (<exp>, single_arg)
        result = df.select(
            single_expression(data)
        ).collect()
        expected_result = df.select(getattr(pl.col('col_a'), attr)(5)).collect()

        assert_frame_equal(result, expected_result)

    @pytest.mark.parametrize(
        'attr', ['is_null', 'is_not_null']
    )
    @given(df=dataframes(
        [
            column(
                'col_a',
                strategy=st.integers(min_value=1, max_value=6),
                allow_null=True
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_single_column_expression_no_args(self, df, attr):
        data = pa.PolarsData(df, 'col_a')
        simple_check_expr = SimpleCheckExpression(
            command=attr,
        ) 
        single_expression = get_single_expression(simple_check_expr)

        # Test (<exp>, )
        result = df.select(
            single_expression(data,)
        ).collect()
        expected_result = df.select(getattr(pl.col('col_a'), attr)()).collect()

        assert_frame_equal(result, expected_result)

    @pytest.mark.parametrize(
        'attr', ['eq', 'gt', 'lt']
    )
    @given(df=dataframes(
        [
            column(
                'col_a',
                strategy=st.integers(min_value=1, max_value=6),
                allow_null=True
            ),
            column(
                'col_b',
                strategy=st.integers(min_value=1, max_value=6),
                allow_null=True
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_single_df_expression(self, df, attr):
        data = pa.PolarsData(df, None)
        simple_check_expr = SimpleCheckExpression(
            command=attr,	
            arg_values=[5,],
            subject=['col_a', 'col_b']
        ) 
        single_expression = get_single_expression(simple_check_expr)

        # Test (<exp>, (<col_x>, <col_y>), single_arg)
        result = df.select(
            single_expression(data)
        ).collect()
        expected_result = df.select(
            getattr(pl.col(('col_a', 'col_b')), attr)(5)
        ).collect()

        assert_frame_equal(result, expected_result)

    @pytest.mark.parametrize(
        'attr', ['is_null', 'is_not_null']
    )
    @given(df=dataframes(
        [
            column(
                'col_a',
                strategy=st.integers(min_value=1, max_value=6),
                allow_null=True
            ),
            column(
                'col_b',
                strategy=st.integers(min_value=1, max_value=6),
                allow_null=True
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_single_df_expression_no_args(self, df, attr):
        data = pa.PolarsData(df, None)
        simple_check_expr = SimpleCheckExpression(
            command=attr,	
            subject=['col_a', 'col_b']
        ) 
        single_expression = get_single_expression(simple_check_expr)

        # Test (<exp>, (<col_x>, <col_y>), )
        result = df.select(
            single_expression(data,)
        ).collect()
        expected_result = df.select(
            getattr(pl.col(('col_a', 'col_b')), attr)()
        ).collect()

        assert_frame_equal(result, expected_result)        


class TestCreateComplexExpression:
    @pytest.mark.parametrize(
        "check_case, expected_result_func",
        [
            # Test case for a conjunction (AND) expression
            (
                CheckCases.CONJUNCTION,
                lambda df: df.select(
                    pl.col("col_a").gt(3).and_(pl.col("col_a").lt(6))
                ).collect(),
            ),
            # Test case for a disjunction (OR) expression
            (
                CheckCases.DISJUNCTION,
                lambda df: df.select(
                    pl.col("col_a").gt(3).or_(pl.col("col_a").lt(6))
                ).collect(),
            ),
            # Test case for a condition (WHEN-THEN) expression
            (
                CheckCases.CONDITION,
                lambda df: df.select(
                    pl.when(pl.col("col_a").gt(3)).then(pl.col("col_a").lt(6))
                ).collect(),
            ),
        ],
    )
    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_create_complex_expression(self, df, check_case, expected_result_func):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=check_case,
            expressions=[
                SimpleCheckExpression(command="gt", arg_values=[3]),
                SimpleCheckExpression(command="lt", arg_values=[6]),
            ],
        )
        result = df.select(create_complex_expression(data, case_check_expr)).collect()
        # Compare with the expected result
        expected_result = expected_result_func(df)
        assert_frame_equal(result, expected_result)
    
    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
            column(
                "col_b",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_nested_complex_expression(self, df):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=CheckCases.CONDITION,
            expressions=[
                SimpleCheckExpression(command="gt", subject=["col_b"], arg_values=[5]),
                CaseCheckExpression(
                    check_case=CheckCases.CONJUNCTION,
                    expressions=[
                        SimpleCheckExpression(command="lt", arg_values=[8]),
                        SimpleCheckExpression(command="is_not_null", subject=["col_a"]),
                    ],
                ),
            ],
        )
        result = df.select(create_complex_expression(data, case_check_expr)).collect()
        # Expected result
        expected_result = df.select(
            pl.when(pl.col("col_b").gt(5)).then(
                pl.col("col_a").lt(8).and_(pl.col("col_a").is_not_null())
            )
        ).collect()
        assert_frame_equal(result, expected_result)


    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
            column(
                "col_b",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_nested_complex_expression_other_side(self, df):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=CheckCases.CONDITION,
            expressions=[
                CaseCheckExpression(
                    check_case=CheckCases.CONJUNCTION,
                    expressions=[
                        SimpleCheckExpression(command="lt", arg_values=[8]),
                        SimpleCheckExpression(command="is_not_null", subject=["col_a"]),
                    ],
                ),
                SimpleCheckExpression(command="gt", subject=["col_b"], arg_values=[5]),
            ],
        )
        result = df.select(create_complex_expression(data, case_check_expr)).collect()
        # Expected result
        expected_result = df.select(
            pl.when(pl.col("col_a").lt(8).and_(pl.col("col_a").is_not_null())).then(
                pl.col("col_b").gt(5))
        ).collect()
        assert_frame_equal(result, expected_result)
        

class TestGetComplexExpression:
    @pytest.mark.parametrize(
        "check_case, expected_result_func",
        [
            # Test case for a conjunction (AND) expression
            (
                CheckCases.CONJUNCTION,
                lambda df: df.select(
                    pl.col("col_a").gt(3).and_(pl.col("col_a").lt(6))
                ).collect(),
            ),
            # Test case for a disjunction (OR) expression
            (
                CheckCases.DISJUNCTION,
                lambda df: df.select(
                    pl.col("col_a").gt(3).or_(pl.col("col_a").lt(6))
                ).collect(),
            ),
            # Test case for a condition (WHEN-THEN) expression
            (
                CheckCases.CONDITION,
                lambda df: df.select(
                    pl.when(pl.col("col_a").gt(3)).then(pl.col("col_a").lt(6))
                ).collect(),
            ),
        ],
    )
    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_get_complex_expression(self, df, check_case, expected_result_func):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=check_case,
            expressions=[
                SimpleCheckExpression(command="gt", arg_values=[3]),
                SimpleCheckExpression(command="lt", arg_values=[6]),
            ],
        )
        complex_expression = get_complex_expression(case_check_expr)
        result = df.select(complex_expression(data)).collect()
        # Compare with the expected result
        expected_result = expected_result_func(df)
        assert_frame_equal(result, expected_result)
    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
            column(
                "col_b",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_nested_get_complex_expression(self, df):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=CheckCases.CONDITION,
            expressions=[
                SimpleCheckExpression(command="gt", subject=["col_b"], arg_values=[5]),
                CaseCheckExpression(
                    check_case=CheckCases.CONJUNCTION,
                    expressions=[
                        SimpleCheckExpression(command="lt", arg_values=[8]),
                        SimpleCheckExpression(command="is_not_null", subject=["col_a"]),
                    ],
                ),
            ],
        )
        complex_expression = get_complex_expression(case_check_expr)
        result = df.select(complex_expression(data)).collect()
        # Expected result
        expected_result = df.select(
            pl.when(pl.col("col_b").gt(5)).then(
                pl.col("col_a").lt(8).and_(pl.col("col_a").is_not_null())
            )
        ).collect()
        assert_frame_equal(result, expected_result)


class TestGetExpression:
    @pytest.mark.parametrize(
        "check_expr, expected_result_func",
        [
            # Test case for a single expression
            (
                SimpleCheckExpression(
                    command="eq",
                    arg_values=[5],
                ),
                lambda df: df.select(pl.col("col_a").eq(5)).collect(),
            ),
            # Test case for a complex expression (AND)
            (
                CaseCheckExpression(
                    check_case=CheckCases.CONJUNCTION,
                    expressions=[
                        SimpleCheckExpression(command="gt", arg_values=[3]),
                        SimpleCheckExpression(command="lt", arg_values=[6]),
                    ],
                ),
                lambda df: df.select(
                    pl.col("col_a").gt(3).and_(pl.col("col_a").lt(6))
                ).collect(),
            ),
            # Test case for a complex expression (WHEN-THEN)
            (
                CaseCheckExpression(
                    check_case=CheckCases.CONDITION,
                    expressions=[
                        SimpleCheckExpression(command="gt", arg_values=[3]),
                        SimpleCheckExpression(command="lt", arg_values=[6]),
                    ],
                ),
                lambda df: df.select(
                    pl.when(pl.col("col_a").gt(3)).then(pl.col("col_a").lt(6))
                ).collect(),
            ),
        ],
    )
    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_get_expression(self, df, check_expr, expected_result_func):
        data = pa.PolarsData(df, "col_a")
        expression_fn = get_expression(check_expr)
        result = df.select(expression_fn(data)).collect()
        # Compare with the expected result
        expected_result = expected_result_func(df)
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
            column(
                "col_b",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_nested_complex_expression(self, df):
        data = pa.PolarsData(df, "col_a")
        check_expr = CaseCheckExpression(
            check_case=CheckCases.CONDITION,
            expressions=[
                SimpleCheckExpression(command="gt", subject=["col_b"], arg_values=[5]),
                CaseCheckExpression(
                    check_case=CheckCases.CONJUNCTION,
                    expressions=[
                        SimpleCheckExpression(command="lt", arg_values=[8]),
                        SimpleCheckExpression(command="is_not_null", subject=["col_a"]),
                    ],
                ),
            ],
        )
        expression_fn = get_expression(check_expr)
        result = df.select(expression_fn(data)).collect()
        # Expected result
        expected_result = df.select(
            pl.when(pl.col("col_b").gt(5)).then(
                pl.col("col_a").lt(8).and_(pl.col("col_a").is_not_null())
            )
        ).collect()
        assert_frame_equal(result, expected_result)


class TestGetCheckFn:
    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_get_check_fn_single_expression(self, df):
        data = pa.PolarsData(df, "col_a")
        simple_check_expr = SimpleCheckExpression(
            command="gt",
            arg_values=[5],
        )
        expression_fn = get_single_expression(simple_check_expr)
        result = get_check_fn(data, expression_fn).collect()
        # Expected result
        expected_result = df.select(pl.col("col_a").gt(5)).collect()
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
            column(
                "col_b",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_get_check_fn_complex_expression(self, df):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=CheckCases.CONJUNCTION,
            expressions=[
                SimpleCheckExpression(command="gt", arg_values=[3]),
                SimpleCheckExpression(command="lt", arg_values=[8]),
            ],
        )
        expression_fn = get_complex_expression(case_check_expr)
        result = get_check_fn(data, expression_fn).collect()
        # Expected result
        expected_result = df.select(
            pl.col("col_a").gt(3).and_(pl.col("col_a").lt(8))
        ).collect()
        assert_frame_equal(result, expected_result)

    @given(df=dataframes(
        [
            column(
                "col_a",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
            column(
                "col_b",
                strategy=st.integers(min_value=1, max_value=10),
                allow_null=True,
            ),
        ],
        max_size=20,
        lazy=True,
    ))
    def test_get_check_fn_condition_expression(self, df):
        data = pa.PolarsData(df, "col_a")
        case_check_expr = CaseCheckExpression(
            check_case=CheckCases.CONDITION,
            expressions=[
                SimpleCheckExpression(command="gt", subject=["col_b"], arg_values=[5]),
                SimpleCheckExpression(command="lt", arg_values=[8]),
            ],
        )
        expression_fn = get_complex_expression(case_check_expr)
        result = get_check_fn(data, expression_fn).collect()
        # Expected result
        expected_result = df.select(
            pl.when(pl.col("col_b").gt(5)).then(pl.col("col_a").lt(8))
        ).collect()
        assert_frame_equal(result, expected_result)
