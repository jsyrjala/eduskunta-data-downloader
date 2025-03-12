"""
Fixtures and configuration for pytest.
"""

import os
import json
import pytest
from unittest.mock import MagicMock, patch

# Mock dlt.resource decorator
@pytest.fixture(autouse=True)
def mock_dlt_resource(monkeypatch):
    """Mock the dlt.resource decorator to assign expected attributes."""
    original_resource = getattr(pytest.importorskip("dlt"), "resource", None)
    
    def mock_resource(*args, **kwargs):
        def decorator(func):
            # Preserve the original behavior if possible
            if original_resource:
                decorated = original_resource(*args, **kwargs)(func)
            else:
                decorated = func
                
            # Ensure resource_name attribute is set
            if 'name' in kwargs:
                decorated.resource_name = kwargs['name']
            if 'write_disposition' in kwargs:
                decorated.write_disposition = kwargs['write_disposition']
            return decorated
        return decorator
    
    monkeypatch.setattr("dlt.resource", mock_resource)
    return mock_resource

# Path relative to the tests directory for sample data
SAMPLE_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def sample_table_list():
    """Return a sample list of tables from the API."""
    return [
        "SaliDBAanestys",
        "SaliDBAanestysAsiakirja",
        "SaliDBAanestysEdustaja",
        "SaliDBAanestysJakauma",
        "HETiedot",
        "HEAsiat",
        "HEIstunto",
        "VaskiData",
    ]


@pytest.fixture
def sample_row_counts():
    """Return a sample of row counts from the API."""
    return {
        "SaliDBAanestys": 41967,
        "SaliDBAanestysAsiakirja": 11536863,
        "SaliDBAanestysEdustaja": 8353394,
        "HETiedot": 5678,
        "HEAsiat": 12345,
    }


@pytest.fixture
def sample_table_data():
    """Return sample data for a table."""
    return {
        "tableName": "SaliDBAanestys",
        "columnCount": 5,
        "columnNames": [
            "AanestysId",
            "IstuntoId",
            "KohtaOtsikko",
            "Aanestystapa",
            "Pvm",
        ],
        "rowCount": 100,
        "rowData": [
            [1, 123, "Test item 1", "Manual", "2023-01-01"],
            [2, 123, "Test item 2", "Manual", "2023-01-01"],
            [3, 124, "Test item 3", "Electronic", "2023-01-02"],
            [4, 124, "Test item 4", "Electronic", "2023-01-02"],
            [5, 125, "Test item 5", "Manual", "2023-01-03"],
        ],
        "page": 0,
        "perPage": 5,
        "hasMore": True,
    }


@pytest.fixture
def mock_requests(monkeypatch):
    """
    Mocks the requests library to return predefined responses
    based on the URL.
    """
    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self.json_data = json_data
            self.status_code = status_code
            self.text = json.dumps(json_data)
            
        def json(self):
            return self.json_data
            
        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception(f"HTTP Error {self.status_code}")
    
    # Create a mock for the requests.get method
    mock_get = MagicMock()
    
    # Dict to store URL patterns and their responses
    responses = {}
    
    # Function to add responses
    def add_response(url_pattern, json_data, status_code=200):
        responses[url_pattern] = (json_data, status_code)
    
    # Function that simulates requests.get behavior
    def mock_requests_get(url, params=None, timeout=None, **kwargs):
        # Match against patterns and known URLs
        for pattern, (data, code) in responses.items():
            if pattern in url:
                return MockResponse(data, code)
        
        # Default response for unknown URLs
        return MockResponse({"error": "Not mocked"}, 404)
    
    # Apply the mock
    mock_get.side_effect = mock_requests_get
    monkeypatch.setattr("requests.get", mock_get)
    
    # Add the helper function as an attribute
    mock_get.add_response = add_response
    
    return mock_get


@pytest.fixture
def mock_duckdb(monkeypatch):
    """Mock DuckDB connection and operations."""
    # Create a mock connection
    mock_conn = MagicMock()
    
    # Mock execute method to return a predefined cursor
    mock_execute = MagicMock()
    mock_conn.execute = mock_execute
    
    # Mock the fetchone and fetchall methods of the cursor
    mock_fetchone = MagicMock(return_value=[0])
    mock_fetchall = MagicMock(return_value=[])
    
    # Set up the mock to return these methods
    mock_execute.return_value.fetchone = mock_fetchone
    mock_execute.return_value.fetchall = mock_fetchall
    
    # Mock the duckdb.connect function to return our mock connection
    mock_connect = MagicMock(return_value=mock_conn)
    monkeypatch.setattr("duckdb.connect", mock_connect)
    
    # Return the mock with some helper attributes
    return {
        "connect": mock_connect,
        "connection": mock_conn,
        "execute": mock_execute,
        "fetchone": mock_fetchone,
        "fetchall": mock_fetchall,
    }