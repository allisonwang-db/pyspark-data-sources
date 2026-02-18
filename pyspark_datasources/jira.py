import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import List

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    WriterCommitMessage,
)
from pyspark.sql.types import Row, StructType

logger = logging.getLogger(__name__)

@dataclass
class JiraCommitMessage(WriterCommitMessage):
    """Commit message for Jira write operations."""
    records_written: int
    batch_id: int

class JiraDataSource(DataSource):
    """
    A DataSource for reading and writing Jira issues.

    Name: `jira`

    Schema: `key string, summary string, status string, description string, created string, updated string, assignee string, reporter string, priority string, issuetype string, project string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import JiraDataSource
    >>> spark.dataSource.register(JiraDataSource)

    Read Jira issues using JQL.

    >>> df = spark.read.format("jira") \\
    ...     .option("url", "https://your-domain.atlassian.net") \\
    ...     .option("username", "email@example.com") \\
    ...     .option("token", "api-token") \\
    ...     .option("jql", "project = PROJ AND status = 'In Progress'") \\
    ...     .load()
    >>> df.show()

    Write Jira issues (Create).

    >>> df.write.format("jira") \\
    ...     .option("url", "https://your-domain.atlassian.net") \\
    ...     .option("username", "email@example.com") \\
    ...     .option("token", "api-token") \\
    ...     .option("project", "PROJ") \\
    ...     .option("issuetype", "Task") \\
    ...     .save()
    """

    @classmethod
    def name(cls):
        return "jira"

    def schema(self):
        return (
            "key string, summary string, status string, description string, "
            "created string, updated string, assignee string, reporter string, "
            "priority string, issuetype string, project string"
        )

    def reader(self, schema: StructType) -> "JiraDataSourceReader":
        return JiraDataSourceReader(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "JiraDataSourceWriter":
        return JiraDataSourceWriter(self.options)


class JiraDataSourceReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.url = options.get("url")
        self.username = options.get("username")
        self.token = options.get("token")
        self.jql = options.get("jql")
        
        if not all([self.url, self.username, self.token]):
            raise ValueError("Jira url, username, and token are required.")
        
        if not self.jql:
            raise ValueError("JQL query is required for reading.")

    def read(self, partition) -> Iterator[Row]:
        try:
            from jira import JIRA
        except ImportError:
            raise ImportError("jira library is required. Install with: pip install jira")

        jira = JIRA(server=self.url, basic_auth=(self.username, self.token))
        
        start_at = 0
        max_results = 50
        
        while True:
            issues = jira.search_issues(self.jql, startAt=start_at, maxResults=max_results)
            if not issues:
                break
                
            for issue in issues:
                yield Row(
                    key=issue.key,
                    summary=issue.fields.summary,
                    status=issue.fields.status.name if hasattr(issue.fields, "status") else None,
                    description=issue.fields.description,
                    created=issue.fields.created,
                    updated=issue.fields.updated,
                    assignee=(
                        issue.fields.assignee.displayName
                        if hasattr(issue.fields, "assignee") and issue.fields.assignee
                        else None
                    ),
                    reporter=(
                        issue.fields.reporter.displayName
                        if hasattr(issue.fields, "reporter") and issue.fields.reporter
                        else None
                    ),
                    priority=(
                        issue.fields.priority.name
                        if hasattr(issue.fields, "priority") and issue.fields.priority
                        else None
                    ),
                    issuetype=(
                        issue.fields.issuetype.name
                        if hasattr(issue.fields, "issuetype")
                        else None
                    ),
                    project=(
                        issue.fields.project.key if hasattr(issue.fields, "project") else None
                    ),
                )
            
            start_at += len(issues)
            if len(issues) < max_results:
                break


class JiraDataSourceWriter(DataSourceWriter):
    def __init__(self, options):
        self.options = options
        self.url = options.get("url")
        self.username = options.get("username")
        self.token = options.get("token")
        self.project = options.get("project")
        self.issuetype = options.get("issuetype", "Task")
        
        if not all([self.url, self.username, self.token]):
            raise ValueError("Jira url, username, and token are required.")

    def write(self, iterator: Iterator[Row]) -> JiraCommitMessage:
        try:
            from jira import JIRA
        except ImportError:
            raise ImportError("jira library is required. Install with: pip install jira")

        from pyspark import TaskContext
        context = TaskContext.get()
        batch_id = context.taskAttemptId() if context else 0

        jira = JIRA(server=self.url, basic_auth=(self.username, self.token))
        
        records_written = 0
        
        for row in iterator:
            fields = row.asDict()
            
            # If key is present, update the issue
            if "key" in fields and fields["key"]:
                issue_key = fields.pop("key")
                # Remove read-only fields or fields that shouldn't be updated directly if present
                for readonly in ["created", "updated", "status", "resolution", "creator"]:
                    fields.pop(readonly, None)
                
                # Filter out None values
                fields = {k: v for k, v in fields.items() if v is not None}
                
                if fields:
                    issue = jira.issue(issue_key)
                    issue.update(fields=fields)
                    records_written += 1
            else:
                # Create new issue
                if "project" not in fields and self.project:
                    fields["project"] = {"key": self.project}
                elif "project" in fields:
                     fields["project"] = {"key": fields["project"]}
                
                if "issuetype" not in fields:
                    fields["issuetype"] = {"name": self.issuetype}
                elif "issuetype" in fields:
                    fields["issuetype"] = {"name": fields["issuetype"]}

                # Remove fields that might be in the schema but not writable on create
                for readonly in ["key", "created", "updated", "status", "resolution", "creator"]:
                    fields.pop(readonly, None)
                
                # Filter out None values
                fields = {k: v for k, v in fields.items() if v is not None}

                jira.create_issue(fields=fields)
                records_written += 1
                
        return JiraCommitMessage(records_written=records_written, batch_id=batch_id)

    def commit(self, messages: List[JiraCommitMessage]) -> None:
        total_written = sum(msg.records_written for msg in messages)
        logger.info(f"Committed {total_written} records to Jira.")

    def abort(self, messages: List[JiraCommitMessage]) -> None:
        logger.warning("Aborted Jira write operation.")
