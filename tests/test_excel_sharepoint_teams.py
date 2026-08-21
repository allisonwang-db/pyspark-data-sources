import sys
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from pyspark_datasources.excel_sharepoint_teams import (
    SharePointExcelDataSource,
    SharePointExcelPartition,
    SharePointExcelReader,
    _download_file,
    _get_bearer_token,
    _read_excel,
    _resolve_site_id,
    _to_str,
    _validate_required_options,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_excel_bytes(data: dict, sheet_name: str = "Sheet1") -> bytes:
    """Build an in-memory Excel file from a dict of {column: [values]}."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name

    headers = list(data.keys())
    ws.append(headers)
    for row in zip(*data.values()):
        ws.append(list(row))

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


_VALID_OPTIONS = {
    "tenant_id": "t1",
    "client_id": "c1",
    "client_secret": "s1",
    "site_host": "contoso.sharepoint.com",
    "site_path": "/sites/MySite",
    "file_path": "/General/data.xlsx",
}


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def test_get_bearer_token_success():
    mock_identity = MagicMock()
    mock_credential = MagicMock()
    mock_token = MagicMock()
    mock_token.token = "my-bearer-token"
    mock_credential.get_token.return_value = mock_token
    mock_identity.ClientSecretCredential.return_value = mock_credential

    with patch.dict(sys.modules, {"azure.identity": mock_identity}):
        token = _get_bearer_token("tenant", "client", "secret")

    assert token == "my-bearer-token"
    mock_identity.ClientSecretCredential.assert_called_once_with(
        tenant_id="tenant", client_id="client", client_secret="secret"
    )


def test_get_bearer_token_auth_failure():
    mock_identity = MagicMock()
    mock_credential = MagicMock()
    mock_credential.get_token.side_effect = Exception("invalid credentials")
    mock_identity.ClientSecretCredential.return_value = mock_credential

    with patch.dict(sys.modules, {"azure.identity": mock_identity}):
        with pytest.raises(ConnectionError, match="Authentication failed"):
            _get_bearer_token("tenant", "client", "bad-secret")


def test_get_bearer_token_missing_library():
    with patch.dict(sys.modules, {"azure.identity": None}):
        with pytest.raises(ImportError, match="azure-identity"):
            _get_bearer_token("t", "c", "s")


# ---------------------------------------------------------------------------
# Site resolution
# ---------------------------------------------------------------------------

def test_resolve_site_id_success():
    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "site-abc123"}
    mock_requests.get.return_value = mock_response
    mock_requests.exceptions.ConnectionError = ConnectionError
    mock_requests.exceptions.Timeout = TimeoutError

    with patch.dict(sys.modules, {"requests": mock_requests}):
        site_id = _resolve_site_id("token", "contoso.sharepoint.com", "/sites/MySite")

    assert site_id == "site-abc123"


def test_resolve_site_id_not_found():
    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_requests.get.return_value = mock_response
    mock_requests.exceptions.ConnectionError = ConnectionError
    mock_requests.exceptions.Timeout = TimeoutError

    with patch.dict(sys.modules, {"requests": mock_requests}):
        with pytest.raises(FileNotFoundError, match="SharePoint site not found"):
            _resolve_site_id("token", "bad.sharepoint.com", "/sites/Missing")


def test_resolve_site_id_permission_denied():
    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 403
    mock_requests.get.return_value = mock_response
    mock_requests.exceptions.ConnectionError = ConnectionError
    mock_requests.exceptions.Timeout = TimeoutError

    with patch.dict(sys.modules, {"requests": mock_requests}):
        with pytest.raises(PermissionError, match="Access denied"):
            _resolve_site_id("expired-token", "contoso.sharepoint.com", "/sites/MySite")


# ---------------------------------------------------------------------------
# File download
# ---------------------------------------------------------------------------

def test_download_file_success():
    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"excel-bytes"
    mock_requests.get.return_value = mock_response
    mock_requests.exceptions.ConnectionError = ConnectionError
    mock_requests.exceptions.Timeout = TimeoutError

    with patch.dict(sys.modules, {"requests": mock_requests}):
        result = _download_file("token", "site-id", "/General/data.xlsx")

    assert result == b"excel-bytes"


def test_download_file_not_found():
    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_requests.get.return_value = mock_response
    mock_requests.exceptions.ConnectionError = ConnectionError
    mock_requests.exceptions.Timeout = TimeoutError

    with patch.dict(sys.modules, {"requests": mock_requests}):
        with pytest.raises(FileNotFoundError, match="File not found"):
            _download_file("token", "site-id", "/General/missing.xlsx")


# ---------------------------------------------------------------------------
# Excel parsing — _read_excel (openpyxl-based)
# ---------------------------------------------------------------------------

def test_read_excel_basic():
    content = _make_excel_bytes({"Name": ["Alice", "Bob"], "Age": [30, 25]})
    headers, rows = _read_excel(content, has_header=True)
    assert headers == ["Name", "Age"]
    assert len(rows) == 2
    assert rows[0][0] == "Alice"
    assert rows[1][1] == 25


def test_read_excel_no_header():
    content = _make_excel_bytes({"Name": ["Alice"], "Age": [30]})
    # has_header=False: all rows (including the header row in the file) become data rows
    headers, rows = _read_excel(content, has_header=False)
    assert headers is None
    assert len(rows) == 2  # header + data row treated as plain data


def test_read_excel_sheet_name():
    content = _make_excel_bytes({"Value": [1, 2]}, sheet_name="Data")
    headers, rows = _read_excel(content, has_header=True, sheet_name="Data")
    assert headers == ["Value"]
    assert len(rows) == 2


def test_read_excel_invalid_sheet():
    content = _make_excel_bytes({"Value": [1]}, sheet_name="Sheet1")
    with pytest.raises(ValueError, match="Sheet 'Missing' not found"):
        _read_excel(content, has_header=True, sheet_name="Missing")


def test_read_excel_nrows():
    content = _make_excel_bytes({"Value": [1, 2, 3, 4, 5]})
    headers, rows = _read_excel(content, has_header=True, nrows="2")
    assert len(rows) == 2


def test_read_excel_nrows_zero_returns_headers_only():
    content = _make_excel_bytes({"Col1": [1, 2], "Col2": [3, 4]})
    headers, rows = _read_excel(content, has_header=True, nrows="0")
    assert headers == ["Col1", "Col2"]
    assert rows == []


def test_read_excel_start_column():
    content = _make_excel_bytes({"A": [1], "B": [2], "C": [3]})
    headers, rows = _read_excel(content, has_header=True, start_column="1")
    assert headers == ["B", "C"]


def test_read_excel_use_columns():
    content = _make_excel_bytes({"Name": ["Alice"], "Age": [30], "City": ["NY"]})
    headers, rows = _read_excel(content, has_header=True, use_columns="Name,City")
    assert headers == ["Name", "City"]
    assert rows[0] == ("Alice", "NY")


def test_read_excel_use_columns_not_found():
    content = _make_excel_bytes({"Name": ["Alice"]})
    with pytest.raises(ValueError, match="None of the requested columns"):
        _read_excel(content, has_header=True, use_columns="Missing")


# ---------------------------------------------------------------------------
# _to_str helper
# ---------------------------------------------------------------------------

def test_to_str_none():
    assert _to_str(None) is None


def test_to_str_string():
    assert _to_str("hello") == "hello"


def test_to_str_int():
    assert _to_str(42) == "42"


def test_to_str_float():
    assert _to_str(3.14) == "3.14"


def test_to_str_datetime():
    import datetime

    dt = datetime.datetime(2024, 1, 15, 10, 30)
    assert _to_str(dt) == "2024-01-15T10:30:00"


def test_to_str_date():
    import datetime

    d = datetime.date(2024, 1, 15)
    assert _to_str(d) == "2024-01-15"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_validate_required_options_all_present():
    _validate_required_options(_VALID_OPTIONS)  # should not raise


def test_validate_required_options_missing():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "tenant_id"}
    with pytest.raises(ValueError, match="tenant_id"):
        _validate_required_options(opts)


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------

def test_datasource_name():
    assert SharePointExcelDataSource.name() == "sharepoint_excel"


def test_datasource_has_header_default():
    ds = SharePointExcelDataSource(_VALID_OPTIONS)
    assert ds._has_header is True


def test_datasource_has_header_false():
    ds = SharePointExcelDataSource({**_VALID_OPTIONS, "has_header": "false"})
    assert ds._has_header is False


def test_datasource_schema_inferred(monkeypatch):
    """schema() returns all-string schema derived from Excel column headers."""
    content = _make_excel_bytes({"Name": ["Alice"], "Score": [99]})

    ds = SharePointExcelDataSource(_VALID_OPTIONS)
    monkeypatch.setattr(ds, "_fetch_excel_bytes", lambda: content)

    schema = ds.schema()
    assert len(schema.fields) == 2
    assert schema.fields[0].name == "Name"
    assert isinstance(schema.fields[0].dataType, StringType)
    assert schema.fields[1].name == "Score"
    assert isinstance(schema.fields[1].dataType, StringType)


def test_datasource_schema_no_header_raises():
    ds = SharePointExcelDataSource({**_VALID_OPTIONS, "has_header": "false"})
    with pytest.raises(ValueError, match="custom schema must be provided"):
        ds.schema()


def test_datasource_schema_missing_options():
    ds = SharePointExcelDataSource({"tenant_id": "t"})
    with pytest.raises(ValueError, match="Missing required option"):
        ds.schema()


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

def test_reader_partitions():
    schema = StructType([StructField("col", StringType())])
    reader = SharePointExcelReader(_VALID_OPTIONS, schema)
    parts = reader.partitions()
    assert len(parts) == 1
    assert isinstance(parts[0], SharePointExcelPartition)


def test_reader_read_success(monkeypatch):
    """read() yields string tuples matching the schema (default has_header=true)."""
    content = _make_excel_bytes({"Name": ["Alice", "Bob"], "City": ["NY", "LA"]})
    schema = StructType([
        StructField("Name", StringType()),
        StructField("City", StringType()),
    ])
    reader = SharePointExcelReader(_VALID_OPTIONS, schema)

    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._get_bearer_token",
        lambda *a: "fake-token",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._resolve_site_id",
        lambda *a: "fake-site-id",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._download_file",
        lambda *a: content,
    )

    rows = list(reader.read(SharePointExcelPartition()))
    assert len(rows) == 2
    assert rows[0] == ("Alice", "NY")
    assert rows[1] == ("Bob", "LA")


def test_reader_read_no_header(monkeypatch):
    """read() with has_header=false treats all rows as data."""
    content = _make_excel_bytes({"Name": ["Alice"], "City": ["NY"]})
    schema = StructType([
        StructField("col0", StringType()),
        StructField("col1", StringType()),
    ])
    opts = {**_VALID_OPTIONS, "has_header": "false"}
    reader = SharePointExcelReader(opts, schema)

    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._get_bearer_token",
        lambda *a: "fake-token",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._resolve_site_id",
        lambda *a: "fake-site-id",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._download_file",
        lambda *a: content,
    )

    rows = list(reader.read(SharePointExcelPartition()))
    # File has header row + 1 data row = 2 rows total when has_header=False
    assert len(rows) == 2


def test_reader_read_missing_file(monkeypatch):
    schema = StructType([StructField("col", StringType())])
    reader = SharePointExcelReader(_VALID_OPTIONS, schema)

    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._get_bearer_token",
        lambda *a: "fake-token",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._resolve_site_id",
        lambda *a: "fake-site-id",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._download_file",
        lambda *a: (_ for _ in ()).throw(FileNotFoundError("File not found")),
    )

    with pytest.raises(FileNotFoundError):
        list(reader.read(SharePointExcelPartition()))


def test_reader_read_invalid_sheet(monkeypatch):
    content = _make_excel_bytes({"Col": [1]}, sheet_name="Sheet1")
    opts = {**_VALID_OPTIONS, "sheet_name": "DoesNotExist"}
    schema = StructType([StructField("Col", StringType())])
    reader = SharePointExcelReader(opts, schema)

    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._get_bearer_token",
        lambda *a: "fake-token",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._resolve_site_id",
        lambda *a: "fake-site-id",
    )
    monkeypatch.setattr(
        "pyspark_datasources.excel_sharepoint_teams._download_file",
        lambda *a: content,
    )

    with pytest.raises(ValueError, match="not found"):
        list(reader.read(SharePointExcelPartition()))
