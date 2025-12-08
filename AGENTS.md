# AGENTS.md

This file provides context and guidelines for AI agents working on the `pyspark-data-sources` repository.

## Project Overview

`pyspark-data-sources` provides custom data sources for reading and writing data in Apache Spark using the Python Data Source API. It includes implementations for various sources like Arrow, Github, Google Sheets, etc.

## Development Guidelines

### Setup & Dependencies

- **Package Manager**: The project uses **`uv`** for dependency management.
- **Installation**: Run `uv sync` to install dependencies defined in `pyproject.toml`.

### Code Style & Linting

- **Linter**: Use **`ruff`** for linting and formatting.
- **Configuration**: The project uses `pyproject.toml` for configuration.
  - Line length: 100 characters.
  - Selected checks: `E` (pycodestyle errors), `F` (Pyflakes), `I` (isort), `UP` (pyupgrade).
- **Command**: Run `ruff check .` to check for errors and `ruff format .` to format code.

### Testing

- **Test Runner**: Use **`pytest`** for running tests.
- **Location**: Tests are located in the `tests/` directory.
- **Command**: Run `pytest` from the root directory to execute the test suite.
- **Coverage**: The project uses `pytest-cov` for coverage reporting.

### Pull Requests

- **ALWAYS** `gh` CLI to create and update Pull Requests.

## Project Structure

- `pyspark_datasources/`: Source code for the data source implementations.
- `tests/`: Unit and integration tests.
- `docs/`: Documentation using MkDocs.
- `examples/`: Example usage scripts.
- `pyproject.toml`: Project configuration and dependencies.

## Dependency Management

- The project uses `uv` for dependency management (implied by `[tool.uv]` in `pyproject.toml`).
- Build backend is `hatchling`.

## Documentation

- Documentation is built with `mkdocs` and `mkdocs-material`.
- API references are generated using `mkdocstrings`.

