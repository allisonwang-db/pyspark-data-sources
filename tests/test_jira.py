import sys
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession

from pyspark_datasources.jira import JiraDataSource, JiraDataSourceReader, JiraDataSourceWriter


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_jira_datasource_registration(spark):
    spark.dataSource.register(JiraDataSource)
    assert JiraDataSource.name() == "jira"
    # Verify we can get the reader
    df = (
        spark.read.format("jira")
        .option("url", "x")
        .option("username", "x")
        .option("token", "x")
        .option("jql", "x")
        .load()
    )
    assert df.schema is not None

def test_jira_reader_logic():
    options = {
        "url": "http://jira.com",
        "username": "user",
        "token": "token",
        "jql": "project=PROJ"
    }
    
    # Mock jira module
    mock_jira_module = MagicMock()
    with patch.dict(sys.modules, {"jira": mock_jira_module}):
        mock_jira_instance = mock_jira_module.JIRA.return_value
        
        # Mock issues
        issue1 = MagicMock()
        issue1.key = "PROJ-1"
        issue1.fields.summary = "Summary 1"
        issue1.fields.status.name = "Open"
        issue1.fields.description = "Desc 1"
        issue1.fields.created = "2023-01-01"
        issue1.fields.updated = "2023-01-02"
        issue1.fields.assignee.displayName = "User 1"
        issue1.fields.reporter.displayName = "Reporter 1"
        issue1.fields.priority.name = "High"
        issue1.fields.issuetype.name = "Bug"
        issue1.fields.project.key = "PROJ"

        issue2 = MagicMock()
        issue2.key = "PROJ-2"
        issue2.fields.summary = "Summary 2"
        # Missing some fields to test None handling
        del issue2.fields.status
        del issue2.fields.assignee
        
        # search_issues returns a list of issues
        mock_jira_instance.search_issues.side_effect = [[issue1, issue2], []]
        
        reader = JiraDataSourceReader(options)
        rows = list(reader.read(None))
        
        assert len(rows) == 2
        assert rows[0].key == "PROJ-1"
        assert rows[0].summary == "Summary 1"
        assert rows[0].status == "Open"
        
        assert rows[1].key == "PROJ-2"
        assert rows[1].status is None

def test_jira_writer_logic_create():
    options = {
        "url": "http://jira.com",
        "username": "user",
        "token": "token",
        "project": "PROJ",
        "issuetype": "Task"
    }
    
    mock_jira_module = MagicMock()
    with patch.dict(sys.modules, {"jira": mock_jira_module}):
        mock_jira_instance = mock_jira_module.JIRA.return_value
        
        writer = JiraDataSourceWriter(options)
        
        rows = [
            Row(summary="New Issue", description="Desc", project="PROJ", issuetype="Task", key=None)
        ]
        
        # We need to mock TaskContext because writer uses it
        with patch("pyspark.TaskContext.get") as mock_context:
            mock_context.return_value.taskAttemptId.return_value = 1
            
            msg = writer.write(iter(rows))
            
            assert msg.records_written == 1
            
            mock_jira_instance.create_issue.assert_called_once()
            call_args = mock_jira_instance.create_issue.call_args
            assert call_args[1]['fields']['summary'] == "New Issue"
            assert call_args[1]['fields']['project']['key'] == "PROJ"

def test_jira_writer_logic_update():
    options = {
        "url": "http://jira.com",
        "username": "user",
        "token": "token"
    }
    
    mock_jira_module = MagicMock()
    with patch.dict(sys.modules, {"jira": mock_jira_module}):
        mock_jira_instance = mock_jira_module.JIRA.return_value
        mock_issue = MagicMock()
        mock_jira_instance.issue.return_value = mock_issue
        
        writer = JiraDataSourceWriter(options)
        
        rows = [
            Row(key="PROJ-1", summary="Updated Summary", status="In Progress")
        ]
        
        with patch("pyspark.TaskContext.get") as mock_context:
            mock_context.return_value.taskAttemptId.return_value = 1
            
            msg = writer.write(iter(rows))
            
            assert msg.records_written == 1
            
            mock_jira_instance.issue.assert_called_with("PROJ-1")
            mock_issue.update.assert_called_once()
            call_args = mock_issue.update.call_args
            assert call_args[1]['fields']['summary'] == "Updated Summary"
            assert 'status' not in call_args[1]['fields']
