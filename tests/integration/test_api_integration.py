"""
Integration tests for API connectivity and functionality.
These tests will actually call the API if run, so they're marked as integration tests
and will be skipped by default unless explicitly requested.
"""

import pytest

# Import functions to test
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from main import get_all_tables, get_table_row_counts, get_table_info

# Base API URL for validation
API_BASE_URL = "https://avoindata.eduskunta.fi/api/v1"


@pytest.mark.integration
class TestApiIntegration:
    """Integration tests for API functionality."""
    
    def test_api_connectivity(self):
        """Test basic API connectivity."""
        import requests
        response = requests.get(f"{API_BASE_URL}/tables/")
        assert response.status_code == 200
        assert isinstance(response.json(), list)
    
    def test_get_all_tables(self):
        """Test retrieving all tables from the API."""
        tables = get_all_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0
        # Check for some expected tables
        for expected_table in ["SaliDBAanestys", "MemberOfParliament"]:
            assert expected_table in tables, f"Expected table {expected_table} not found"
    
    def test_get_table_row_counts(self):
        """Test retrieving row counts for tables."""
        row_counts = get_table_row_counts()
        assert isinstance(row_counts, dict)
        assert len(row_counts) > 0
        # Check that some tables have reasonable row counts
        for table, count in row_counts.items():
            assert isinstance(count, int)
            assert count >= 0
    
    @pytest.mark.slow
    def test_get_table_info(self):
        """Test retrieving info for a specific table."""
        # Use a table that's likely to exist
        table_info = get_table_info("SaliDBAanestys")
        assert isinstance(table_info, dict)
        assert "row_count" in table_info
        assert "columns" in table_info
        assert isinstance(table_info["columns"], list)
        assert len(table_info["columns"]) > 0