import stat
import sys
from unittest.mock import MagicMock, patch

import pytest

from pyspark_datasources.sftp import (
    SFTPCommitMessage,
    SFTPDataSourceReader,
    SFTPDataSourceWriter,
    SFTPInputPartition,
)


def test_sftp_reader_partitions():
    options = {
        "host": "localhost",
        "username": "user",
        "password": "pass",
        "path": "/path/to/file.txt"
    }
    
    mock_paramiko = MagicMock()
    mock_sftp = MagicMock()
    mock_transport = MagicMock()
    mock_paramiko.Transport.return_value = mock_transport
    mock_paramiko.SFTPClient.from_transport.return_value = mock_sftp
    
    # Mock stat to be a file
    mock_attr = MagicMock()
    mock_attr.st_mode = stat.S_IFREG | 0o644
    mock_sftp.stat.return_value = mock_attr
    
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        reader = SFTPDataSourceReader(None, options)
        partitions = reader.partitions()
        
        assert len(partitions) == 1
        assert partitions[0].path == "/path/to/file.txt"

def test_sftp_reader_read():
    options = {
        "host": "localhost",
        "username": "user",
        "password": "pass",
        "path": "/path/to/file.txt"
    }
    
    mock_paramiko = MagicMock()
    mock_sftp = MagicMock()
    mock_transport = MagicMock()
    mock_paramiko.Transport.return_value = mock_transport
    mock_paramiko.SFTPClient.from_transport.return_value = mock_sftp
    
    mock_file = MagicMock()
    mock_file.__enter__.return_value = iter(["line1\n", "line2\n"])
    mock_sftp.open.return_value = mock_file
    
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        reader = SFTPDataSourceReader(None, options)
        partition = SFTPInputPartition("/path/to/file.txt")
        rows = list(reader.read(partition))
        
        assert len(rows) == 2
        assert rows[0][0] == "line1"
        assert rows[1][0] == "line2"

def test_sftp_writer_write():
    options = {
        "host": "localhost",
        "username": "user",
        "password": "pass",
        "path": "/output/dir"
    }
    
    mock_paramiko = MagicMock()
    mock_sftp = MagicMock()
    mock_transport = MagicMock()
    mock_paramiko.Transport.return_value = mock_transport
    mock_paramiko.SFTPClient.from_transport.return_value = mock_sftp
    
    mock_file = MagicMock()
    mock_sftp.open.return_value = mock_file
    
    # Mock mkdir to succeed or fail safely
    mock_sftp.stat.side_effect = FileNotFoundError
    
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        writer = SFTPDataSourceWriter(None, options, False)
        iterator = iter([("data1",), ("data2",)])
        msg = writer.write(iterator)
        
        assert isinstance(msg, SFTPCommitMessage)
        assert "/output/dir/part-" in msg.path
        
        # Verify write calls
        handle = mock_file.__enter__.return_value
        assert handle.write.call_count == 2
        handle.write.assert_any_call("data1\n")
        handle.write.assert_any_call("data2\n")

def test_sftp_writer_commit():
    options = {
        "host": "localhost",
        "username": "user",
        "password": "pass",
        "path": "/output/dir"
    }
    
    mock_paramiko = MagicMock()
    mock_sftp = MagicMock()
    mock_transport = MagicMock()
    mock_paramiko.Transport.return_value = mock_transport
    mock_paramiko.SFTPClient.from_transport.return_value = mock_sftp
    
    # Mock listdir to return some files
    mock_sftp.listdir.return_value = ["old_file.txt", "part-new.txt"]
    
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        writer = SFTPDataSourceWriter(None, options, overwrite=True)
        messages = [SFTPCommitMessage(path="/output/dir/part-new.txt")]
        
        writer.commit(messages)
        
        # Verify old_file.txt was removed
        mock_sftp.remove.assert_called_with("/output/dir/old_file.txt")
        # Verify part-new.txt was NOT removed
        assert mock_sftp.remove.call_count == 1

def test_sftp_options_validation():
    # Missing path
    with pytest.raises(ValueError, match="Option 'path' is required"):
        SFTPDataSourceReader(None, {})

    # Missing host (checked in _get_sftp_client called by partitions)
    mock_paramiko = MagicMock()
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        reader = SFTPDataSourceReader(None, {"path": "/path"})
        with pytest.raises(ValueError, match="Option 'host' is required"):
            reader.partitions()
            
    # Missing username
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        reader = SFTPDataSourceReader(None, {"path": "/path", "host": "localhost"})
        with pytest.raises(ValueError, match="Option 'username' is required"):
            reader.partitions()

    # Missing password and key
    with patch.dict(sys.modules, {"paramiko": mock_paramiko}):
        reader = SFTPDataSourceReader(None, {"path": "/path", "host": "localhost", "username": "user"})
        with pytest.raises(ValueError, match="Either 'password' or 'key_filename' must be provided"):
            reader.partitions()
