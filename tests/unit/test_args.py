"""
Unit tests for argument parsing
"""

import pytest
import argparse

# Import the functions to test
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from main import parse_args


@pytest.mark.unit
class TestArgumentParsing:
    """Tests for argument parsing."""
    
    def test_parse_args_tables(self):
        """Test parse_args with --tables."""
        # Mock command line arguments
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(sys, 'argv', ['main.py', '--tables', 'SaliDBAanestys', 'HETiedot'])
            parser, args = parse_args()
            
            assert args.tables == ['SaliDBAanestys', 'HETiedot']
            assert not args.all
            assert not args.list_tables
    
    def test_parse_args_all(self):
        """Test parse_args with --all."""
        # Mock command line arguments
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(sys, 'argv', ['main.py', '--all'])
            parser, args = parse_args()
            
            assert args.tables is None
            assert args.all
            assert not args.list_tables
    
    def test_parse_args_list_tables(self):
        """Test parse_args with --list-tables."""
        # Mock command line arguments
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(sys, 'argv', ['main.py', '--list-tables'])
            parser, args = parse_args()
            
            assert args.tables is None
            assert not args.all
            assert args.list_tables
    
    def test_parse_args_options(self):
        """Test parse_args with additional options."""
        # Mock command line arguments
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(sys, 'argv', [
                'main.py', 
                '--tables', 'SaliDBAanestys',
                '--db-file', 'custom.duckdb',
                '--concurrency', '10',
                '--rate-limit', '3.5',
                '--limit', '100',
                '--no-progress',
                '--no-color'
            ])
            parser, args = parse_args()
            
            assert args.tables == ['SaliDBAanestys']
            assert args.db_file == 'custom.duckdb'
            assert args.concurrency == 10
            assert args.rate_limit == 3.5
            assert args.limit == 100
            assert args.no_progress
            assert args.no_color
    
    def test_parse_args_defaults(self):
        """Test parse_args default values."""
        # Mock command line arguments with minimal required args
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(sys, 'argv', ['main.py', '--tables', 'SaliDBAanestys'])
            parser, args = parse_args()
            
            # Verify defaults
            assert args.db_file == 'eduskunta.duckdb'
            assert args.concurrency == 5
            assert args.rate_limit == 5.0
            assert args.limit is None
            assert not args.no_progress
            assert not args.no_color