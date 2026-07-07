# How to filter

The `Filter` class applies a config-based row filter to a Polars DataFrame or LazyFrame and returns a result of the same type.
It reuses the same check expression notation as `Validator` — `command`, `check_case`, `expressions`, `subject`, `arg_values` — so the same operators and composition patterns apply.

A filter config requires a `name` and a `filter` key. An optional `select` key limits the returned columns.

## Simple filter

The simplest form is a single check expression referencing one column.

```py title="filter.py" linenums="1" hl_lines="4-11"
--8<-- "notebooks/filter.py:1:22"
```

```
shape: (2, 3)
┌─────┬─────┬────────┐
│ id  ┆ age ┆ status │
│ --- ┆ --- ┆ ---    │
│ i64 ┆ i64 ┆ str    │
╞═════╪═════╪════════╡
│ 1   ┆ 25  ┆ active │
│ 3   ┆ 34  ┆ active │
└─────┴─────┴────────┘
```

## Conjunction

Use `check_case: conjunction` to keep rows that satisfy **all** expressions.

```py title="filter.py" linenums="24" hl_lines="4-20"
--8<-- "notebooks/filter.py:24:51"
```

```
shape: (2, 2)
┌─────┬─────────┐
│ age ┆ country │
│ --- ┆ ---     │
│ i64 ┆ str     │
╞═════╪═════════╡
│ 20  ┆ BE      │
│ 30  ┆ BR      │
└─────┴─────────┘
```

## Disjunction

Use `check_case: disjunction` to keep rows that satisfy **at least one** expression.

```py title="filter.py" linenums="53" hl_lines="4-18"
--8<-- "notebooks/filter.py:53:80"
```

```
shape: (3, 2)
┌─────┬────────┐
│ age ┆ status │
│ --- ┆ ---    │
│ i64 ┆ str    │
╞═════╪════════╡
│ 10  ┆ OK     │
│ 25  ┆ VIP    │
│ 15  ┆ OK     │
└─────┴────────┘
```

## Nested expressions

Conjunctions and disjunctions can be nested freely to express compound conditions.

```py title="filter.py" linenums="82" hl_lines="4-28"
--8<-- "notebooks/filter.py:82:114"
```

```
shape: (4, 3)
┌─────┬─────────┬────────┐
│ age ┆ country ┆ status │
│ --- ┆ ---     ┆ ---    │
│ i64 ┆ str     ┆ str    │
╞═════╪═════════╪════════╡
│ 20  ┆ BE      ┆ OK     │
│ 25  ┆ US      ┆ VIP    │
│ 30  ┆ BR      ┆ OK     │
│ 15  ┆ US      ┆ VIP    │
└─────┴─────────┴────────┘
```

## Column selection

Add a `select` key to return only the specified columns after filtering.

```py title="filter.py" linenums="116" hl_lines="9"
--8<-- "notebooks/filter.py:116:130"
```

```
shape: (2, 2)
┌─────┬─────┐
│ id  ┆ age │
│ --- ┆ --- │
│ i64 ┆ i64 │
╞═════╪═════╡
│ 1   ┆ 25  │
│ 3   ┆ 34  │
└─────┴─────┘
```

## Available commands

The `command` field accepts the same values as check expressions:

```
'is_equal_to'
'is_equal_to_or_both_missing'
'is_greater_than_or_equal_to'
'is_greater_than'
'is_less_than_or_equal_to'
'is_less_than'
'is_not_equal_to'
'is_not_equal_to_and_not_both_missing'
'is_unique'
'is_duplicated'
'is_in'
'is_null'
'is_not_null'
```

## Configuration reference

```
name:   <string>
filter: <simple or complex check expression>
select: <empty or list of column names to return>
```

A **simple** filter expression:

```
command: <string — one of the commands above>
subject: <list with the column name to filter on>
arg_values: <empty or list of values>
arg_columns: <empty or list of column names>
```

A **complex** filter expression:

```
check_case: <conjunction | disjunction | condition>
expressions: [<2 or more simple or complex expressions>]
```
