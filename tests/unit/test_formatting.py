"""
Unit tests for formatting and display functions
"""

import pytest
from unittest.mock import patch

# Import the functions to test
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from main import format_text, Colors, Emoji


@pytest.mark.unit
class TestFormatting:
    """Tests for formatting and display functions."""
    
    def test_format_text_with_color(self):
        """Test format_text with color."""
        # Global use_colors is True by default
        with patch('main.use_colors', True):
            result = format_text("Test", Colors.GREEN)
            assert Colors.GREEN in result
            assert Colors.RESET in result
            assert "Test" in result
    
    def test_format_text_with_emoji(self):
        """Test format_text with emoji."""
        # Global use_colors is True by default
        with patch('main.use_colors', True):
            result = format_text("Test", emoji=Emoji.CHECK)
            assert Emoji.CHECK in result
            assert "Test" in result
    
    def test_format_text_with_color_and_emoji(self):
        """Test format_text with color and emoji."""
        # Global use_colors is True by default
        with patch('main.use_colors', True):
            result = format_text("Test", Colors.GREEN, Emoji.CHECK)
            assert Colors.GREEN in result
            assert Colors.RESET in result
            assert Emoji.CHECK in result
            assert "Test" in result
    
    def test_format_text_with_bold(self):
        """Test format_text with bold."""
        # Global use_colors is True by default
        with patch('main.use_colors', True):
            result = format_text("Test", bold=True)
            assert Colors.BOLD in result
            assert Colors.RESET in result
            assert "Test" in result
    
    def test_format_text_with_colors_disabled(self):
        """Test format_text with colors disabled."""
        # Set global use_colors to False
        with patch('main.use_colors', False):
            result = format_text("Test", Colors.GREEN, Emoji.CHECK, bold=True)
            assert Colors.GREEN not in result
            assert Colors.BOLD not in result
            assert Colors.RESET not in result
            assert Emoji.CHECK not in result
            assert result == "Test"