# DataGuard

<p align="center">
   <a href="https://github.com/eu-parc/dataguard/actions?query=workflow%3ACI" target="_blank" rel="noopener">
    <img src="https://github.com/eu-parc/dataguard/actions/workflows/ci.yml/badge.svg" alt="CI">
   </a>
   <a href="https://github.com/eu-parc/dataguard/actions?query=workflow%3ADOCS" target="_blank" rel="noopener">
    <img src="https://github.com/eu-parc/dataguard/actions/workflows/docs.yml/badge.svg" alt="Documentation build">
   </a>
</p>

- Source: [GitHub repository](https://github.com/eu-parc/dataguard)
- Documentation: [DataGuard documentation](https://eu-parc.github.io/dataguard/)
- Changelog: [CHANGELOG.md](https://github.com/eu-parc/dataguard/blob/master/CHANGELOG.md)

DataGuard supports Python 3.10 and later.

## Features

- **Flexible validation checks**: Define simple checks using built-in commands (equality, comparisons, nullability, etc.)
- **Complex check expressions**: Combine multiple checks with conjunctions (`and`), disjunctions (`or`), and conditionals (`when-then`)
- **N-ary expressions**: Support for 2 or more expressions in conjunction/disjunction operations (v0.5.0+)
- **Nested expressions**: Compose complex conditions by nesting check expressions
- **Custom validation functions**: Define your own validation logic following the framework's signature pattern
- **DataFrame-level checks**: Apply validations across entire dataframes or specific column groups
- **Config-based filtering**: Filter rows and columns using the same expression notation as checks — simple predicates or compound conjunctions/disjunctions/conditionals
- **Detailed error reporting**: Collect and format validation errors with custom messages and severity levels
- **Polars support**: Built on Polars for efficient data processing at scale

## Installation

The package is published on PyPI as `peh-dataguard` and imported as
`dataguard`.

### Install with uv

For a `uv`-managed project, add DataGuard as a dependency:

```bash
uv add peh-dataguard
```

To install it into the current environment without changing a project file:

```bash
uv pip install peh-dataguard
```

### Install with pip

Create and activate a virtual environment, then install the package:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install peh-dataguard
```

On Windows PowerShell, activate the environment with
`.venv\Scripts\Activate.ps1` instead of `source`.

Verify that the public API can be imported:

```bash
python -c "from dataguard import Filter, Validator; print('DataGuard is ready')"
```

For a `uv`-managed project, use `uv run python ...` instead of `python ...`.

## Development

For a source checkout, install the development dependencies with Python 3.14:

```bash
git clone https://github.com/eu-parc/dataguard.git
cd dataguard
uv sync --python 3.14
```

Run the project checks with:

```bash
make test
make lint
make format
```

To preview the Zensical documentation locally:

```bash
make serve
```
