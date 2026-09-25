# Changelog

All notable changes to DataGuard are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/eu-parc/dataguard/compare/v0.6.1...HEAD
[0.6.1]: https://github.com/eu-parc/dataguard/compare/v0.6.0...v0.6.1
