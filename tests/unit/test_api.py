"""
Unit tests for API-related functions in main.py
"""

import pytest
from unittest.mock import patch, MagicMock

# Import the functions to test
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from main import get_all_tables, get_table_row_counts, get_table_info


@pytest.mark.unit
class TestApiFunctions:
    """Tests for API-related functions."""
    
    def test_get_all_tables(self, mock_requests, sample_table_list):
        """Test get_all_tables function."""
        # Setup mock
        mock_requests.add_response('/tables/', sample_table_list)
        
        # Call the function
        result = get_all_tables()
        
        # Verify the result
        assert result == sample_table_list
        assert mock_requests.call_count == 1
        
    def test_get_table_row_counts(self, mock_requests):
        """Test get_table_row_counts function."""
        # Sample data
        sample_data = [
            {"tableName": "SaliDBAanestys", "rowCount": 41967},
            {"tableName": "SaliDBAanestysAsiakirja", "rowCount": 11536863},
        ]
        
        # Setup mock
        mock_requests.add_response('/tables/counts', sample_data)
        
        # Call the function
        result = get_table_row_counts()
        
        # Verify the result
        expected = {
            "SaliDBAanestys": 41967,
            "SaliDBAanestysAsiakirja": 11536863,
        }
        assert result == expected
        assert mock_requests.call_count == 1
    
    def test_get_table_info(self, mock_requests, sample_table_data):
        """Test get_table_info function."""
        # Setup mock
        mock_requests.add_response(
            '/tables/SaliDBAanestys/rows', 
            sample_table_data
        )
        
        # Call the function
        result = get_table_info("SaliDBAanestys")
        
        # Verify the result
        assert result["row_count"] == sample_table_data["rowCount"]
        assert result["columns"] == sample_table_data["columnNames"]
        assert mock_requests.call_count == 1
        
    def test_get_table_info_with_error(self, mock_requests):
        """Test get_table_info function with an error response."""
        # Setup mock to return an error
        mock_requests.add_response(
            '/tables/NonExistentTable/rows', 
            {"error": "Table not found"}, 
            404
        )
        
        # Call the function and verify it raises an exception
        with pytest.raises(Exception):
            get_table_info("NonExistentTable")