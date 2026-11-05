# Changelog

All notable changes to DataGuard are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-11-05

This release adds Python 3.15 support, drops Python 3.10, and moves to
Pydantic 2.14 and Polars 2.

### Changed

- Bumped the package version to `0.7.0`.
- Python support is now `>=3.11, <3.16`. Python 3.10 is no longer supported and
  Python 3.15 is added to the CI, documentation, and publishing workflows.
- Raised the Ruff target version to Python 3.11.
- Updated direct dependency ranges: `pandera[polars]>=0.35.0,<0.36`,
  `polars>=1.42.1,<2.2` (`>=2.0,<2.2` on Python 3.15), and
  `pydantic>=2.14.3,<2.15`. Polars 1.x does not work on Python 3.15, and
  Pydantic versions before 2.14 have no Python 3.15 wheels for `pydantic-core`.
- Updated development, build, and documentation dependencies and regenerated
  `uv.lock`.

### Notes

- **Stricter type checking in Polars 2.** Polars 2 no longer silently coerces
  operands in `is_in`. A check such as `is_in` with `arg_values: [1, 2, 3]` on
  a `float` column, which Polars 1.x compared after converting both sides to
  `Float64`, now fails with a Polars `InvalidOperationError`. DataGuard reports
  it as a `CHECK_ERROR` instead of a regular validation failure. DataGuard does
  not change this behavior: use `arg_values` whose type matches the column
  (for example `[1.0, 2.0, 3.0]` for a `float` column). Custom check functions
  that call `is_in` directly must cast their values explicitly. Other Polars 2
  behavior changes may apply; see the Polars 2.0 migration notes.

## [0.6.1] - 2026-09-25

This maintenance release updates packaging, supported Python versions,
dependencies, CI, and documentation without changing the public runtime API.

### Added

- Added end-user installation instructions for `uv` and `pip` to the
  documentation and README.
- Added this changelog and exposed it in the documentation navigation.

### Changed

- Bumped the package version to `0.6.1`.
- Added Python 3.14 to the CI, documentation, and publishing workflows and
  updated the contributor setup.
- Migrated the documentation from MkDocs Material to Zensical, including the
  GitHub Pages deployment workflow.
- Declared direct Polars and Pydantic runtime dependencies and added
  next-minor upper bounds for direct runtime, development, build, and
  documentation dependencies.
- Regenerated `uv.lock` with the validated dependency graph.
- Updated the README with installation, development, documentation, and
  release-history links.

[Unreleased]: https://github.com/eu-parc/dataguard/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/eu-parc/dataguard/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/eu-parc/dataguard/compare/v0.6.0...v0.6.1
