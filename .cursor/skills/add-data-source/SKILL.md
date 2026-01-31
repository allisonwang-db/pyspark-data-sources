---
name: add-data-source
description: Guides the creation of new PySpark data sources, including implementation, testing, and documentation. Use when adding a new connector, data source, or sink to the project.
---

# Add Data Source

## Overview

This skill guides you through the process of adding a new data source to `pyspark-data-sources`.

## Workflow

### 1. Design Phase
- [ ] Create a design document in `docs/design/<source_name>.md`.
- [ ] Define options, schema, and authentication methods.
- [ ] Plan dependencies (add to `pyproject.toml`).

### 2. Implementation Phase
- [ ] Add dependencies to `pyproject.toml` under `[project.optional-dependencies]` and `all`.
- [ ] Create implementation file: `pyspark_datasources/<source_name>.py`.
    - Implement `DataSource`, `DataSourceReader`, and optional `DataSourceWriter`.
    - See [TEMPLATE.md](TEMPLATE.md) for code structure.
- [ ] Register source in `pyspark_datasources/__init__.py`.

### 3. Testing Phase
- [ ] Create unit tests: `tests/test_<source_name>.py`.
- [ ] (Optional) Create end-to-end test script in `examples/`.
- [ ] Run tests: `pytest tests/test_<source_name>.py`.

### 4. Documentation Phase
- [ ] Create documentation page: `docs/datasources/<source_name>.md`.
- [ ] Update `mkdocs.yml`: Add new page to navigation.
- [ ] Update `README.md`: Add to "Available Data Sources" tables.
- [ ] Update `docs/index.md`: Add to "Available Data Sources" tables.
- [ ] Update `docs/data-sources-guide.md`: Add section with examples.

## Best Practices

- **Docstrings**: Add detailed docstrings to all classes and methods.
- **Type Hinting**: Use type hints for all public methods.
- **Error Handling**: Raise informative errors for missing options.
- **Validation**: Validate options in `__init__` or `reader`/`writer` methods.
