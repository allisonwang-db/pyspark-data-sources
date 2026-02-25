---
name: create-data-source
description: Create a new PySpark data source implementation. Use when adding a new connector, data source, or integration to the project.
---

# Create Data Source

## Overview

This skill guides you through the process of adding a new PySpark data source to the repository. A data source typically consists of:
1.  **DataSource**: The main entry point, defining capabilities and schema.
2.  **Reader**: Logic for reading data (batch/stream).
3.  **Writer**: Logic for writing data (batch/stream).
4.  **Tests**: Unit and integration tests.
5.  **Documentation**: Usage guide and API reference.

## Workflow

1.  **Define Requirements**: Determine if the source supports reading, writing, or both. Is it batch or streaming?
2.  **Implementation**: Create the implementation file in `pyspark_datasources/`.
3.  **Registration**: Register the new source in `pyspark_datasources/__init__.py`.
4.  **Dependencies**: Add any required libraries to `pyproject.toml`.
5.  **Testing**: Create a test file in `tests/`.
6.  **Documentation**: Add documentation in `docs/datasources/` and update `mkdocs.yml` and `README.md`.

## Implementation Details

### 1. Create Implementation File

Create a new file `pyspark_datasources/<name>.py`. Use the templates in `templates.md`.

-   Implement `DataSource` class.
-   Implement `DataSourceReader` (if reading).
-   Implement `DataSourceWriter` (if writing).
-   Define the schema in the `DataSource` class.

### 2. Register Data Source

Add the new class to `pyspark_datasources/__init__.py`:

```python
from .<name> import <Name>DataSource
```

### 3. Add Dependencies

If the data source requires external libraries:
1.  Add them to `[project.optional-dependencies]` in `pyproject.toml`.
2.  Update the `all` group to include the new dependencies.

### 4. Add Tests

Create `tests/test_<name>.py`.
-   Use `unittest.mock` to mock external services/libraries.
-   Test registration, reading, and writing logic.
-   See `templates.md` for test structure.

### 5. Add Documentation

1.  Create `docs/datasources/<name>.md`.
2.  Add the new page to `nav` in `mkdocs.yml`.
3.  Add installation and usage examples to `README.md` and `docs/data-sources-guide.md`.

## Checklist

Use the checklist in `checklist.md` to track your progress.

## Resources

-   [Python Data Source API Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_data_source.html)
-   Existing implementations in `pyspark_datasources/` (e.g., `github.py`, `salesforce.py`).
