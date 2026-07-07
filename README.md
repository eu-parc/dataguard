# DataGuard

<p align="center">
   <a href="https://github.com/eu-parc/dataguard/actions?query=workflow%3ACI" targe>
    <img src="https://github.com/eu-parc/dataguard/actions/workflows/ci.yml/badge.svg" alt="CI">
   </a>
   <a href="https://github.com/eu-parc/dataguard/actions?query=workflow%3ADOCS" targe>
    <img src="https://github.com/eu-parc/dataguard/actions/workflows/docs.yml/badge.svg" alt="CI">
   </a>
<p/>

Source: https://github.com/eu-parc/dataguard  
Documentation: https://eu-parc.github.io/dataguard/

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
To install the library for development.

### Synchronize the project dependencies
    
   ```bash
   $ uv sync
   ```

#### Tests

   ```bash
   $ make test
   ```

#### Format

   ```bash
   $ make format
   ```

#### lint

   ```bash
   $ make lint
   ```
