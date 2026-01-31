import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType


def _get_sftp_client(options):
    try:
        import paramiko
    except ImportError:
        raise ImportError("paramiko is required for SFTP data source. Install it with `pip install paramiko`.")

    host = options.get("host")
    if not host:
        raise ValueError("Option 'host' is required.")
    
    port = int(options.get("port", 22))
    username = options.get("username")
    if not username:
        raise ValueError("Option 'username' is required.")
    
    password = options.get("password")
    key_filename = options.get("key_filename")

    if not password and not key_filename:
        raise ValueError("Either 'password' or 'key_filename' must be provided.")

    transport = paramiko.Transport((host, port))
    if password:
        transport.connect(username=username, password=password)
    else:
        pkey = paramiko.RSAKey.from_private_key_file(key_filename)
        transport.connect(username=username, pkey=pkey)
    
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


class SFTPDataSource(DataSource):
    """
    A data source for reading and writing data to SFTP servers.
    
    Options
    -------
    host : str
        The SFTP server host.
    port : str, optional
        The SFTP server port (default: 22).
    username : str
        The SFTP username.
    password : str, optional
        The SFTP password.
    key_filename : str, optional
        Path to the private key file.
    path : str
        The directory or file path on the SFTP server.
        - If a file path is provided, only that file is read.
        - If a directory path is provided, all files in that directory are read.
    recursive : str, optional
        Whether to recursively list files in subdirectories when `path` is a directory (default: false).
        Only used for reading.
    file_format : str, optional
        The file format (default: text). Currently only 'text' is supported.
    """

    @classmethod
    def name(cls):
        return "sftp"

    def schema(self):
        return "value string"

    def reader(self, schema: StructType) -> "DataSourceReader":
        return SFTPDataSourceReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "DataSourceWriter":
        return SFTPDataSourceWriter(schema, self.options, overwrite)


class SFTPDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        self.path = options.get("path")
        if not self.path:
            raise ValueError("Option 'path' is required.")
        self.recursive = options.get("recursive", "false").lower() == "true"

    def partitions(self) -> list[InputPartition]:
        sftp, transport = _get_sftp_client(self.options)
        try:
            files = []
            try:
                # Check if path is a file or directory
                attr = sftp.stat(self.path)
                import stat
                if stat.S_ISREG(attr.st_mode):
                    files.append(self.path)
                else:
                    # It's a directory
                    self._list_files(sftp, self.path, files)
            except FileNotFoundError:
                # Path might not exist or be accessible
                pass
            
            return [SFTPInputPartition(f) for f in files]
        finally:
            sftp.close()
            transport.close()

    def _list_files(self, sftp, path, files):
        import stat
        for entry in sftp.listdir_attr(path):
            full_path = f"{path}/{entry.filename}"
            if stat.S_ISREG(entry.st_mode):
                files.append(full_path)
            elif stat.S_ISDIR(entry.st_mode) and self.recursive:
                self._list_files(sftp, full_path, files)

    def read(self, partition: InputPartition) -> Iterator[tuple[Any]]:
        sftp, transport = _get_sftp_client(self.options)
        try:
            with sftp.open(partition.path, "r") as f:
                for line in f:
                    # Remove trailing newline characters
                    yield (line.rstrip("\r\n"),)
        finally:
            sftp.close()
            transport.close()


@dataclass
class SFTPInputPartition(InputPartition):
    path: str


@dataclass
class SFTPCommitMessage(WriterCommitMessage):
    path: str


class SFTPDataSourceWriter(DataSourceWriter):
    def __init__(self, schema, options, overwrite):
        self.schema = schema
        self.options = options
        self.overwrite = overwrite
        self.path = options.get("path")
        if not self.path:
            raise ValueError("Option 'path' is required.")

    def write(self, iterator: Iterator[tuple[Any]]) -> SFTPCommitMessage:
        sftp, transport = _get_sftp_client(self.options)
        try:
            # Ensure directory exists (simple check, might fail if nested)
            try:
                sftp.stat(self.path)
            except FileNotFoundError:
                # Try to create it. Note: mkdir doesn't support recursive creation easily
                # For now assume parent exists or path is valid
                try:
                    sftp.mkdir(self.path)
                except Exception:
                    pass

            filename = f"part-{uuid.uuid4()}.txt"
            full_path = f"{self.path}/{filename}"
            
            with sftp.open(full_path, "w") as f:
                for row in iterator:
                    # Assuming single column 'value'
                    f.write(str(row[0]) + "\n")
            
            return SFTPCommitMessage(path=full_path)
        finally:
            sftp.close()
            transport.close()

    def commit(self, messages: list[SFTPCommitMessage]) -> None:
        # In a real implementation, we might move files from temp to final
        # Here we wrote directly to final path.
        # We could handle overwrite here by deleting other files?
        
        if self.overwrite:
            sftp, transport = _get_sftp_client(self.options)
            try:
                # List all files in directory
                existing_files = sftp.listdir(self.path)
                new_files = {msg.path.split('/')[-1] for msg in messages}
                
                for filename in existing_files:
                    if filename not in new_files:
                        try:
                            sftp.remove(f"{self.path}/{filename}")
                        except Exception:
                            pass
            finally:
                sftp.close()
                transport.close()

    def abort(self, messages: list[SFTPCommitMessage]) -> None:
        sftp, transport = _get_sftp_client(self.options)
        try:
            for msg in messages:
                try:
                    sftp.remove(msg.path)
                except Exception:
                    pass
        finally:
            sftp.close()
            transport.close()
