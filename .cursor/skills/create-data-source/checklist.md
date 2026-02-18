# Checklist

- [ ] Define requirements (read/write/stream)
- [ ] Create implementation file `pyspark_datasources/<name>.py`
- [ ] Implement `DataSource` class
- [ ] Implement `DataSourceReader` (if reading)
- [ ] Implement `DataSourceWriter` (if writing)
- [ ] Register data source in `pyspark_datasources/__init__.py`
- [ ] Add dependencies to `pyproject.toml`
- [ ] Create tests in `tests/test_<name>.py`
- [ ] Run tests and ensure they pass
- [ ] Add documentation in `docs/datasources/<name>.md`
- [ ] Update `mkdocs.yml`
- [ ] Update `README.md`
- [ ] Create a Pull Request
