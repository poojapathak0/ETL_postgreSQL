"""
Unit tests for the PostgreSQL Data Format Converter.

This module contains unit tests for the converter modules.
"""

import os
import json
import csv
import pytest
import tempfile
from datetime import date, datetime
from decimal import Decimal

# Add the project root directory to the Python path
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.converters.json_converter import JsonConverter
from src.converters.csv_converter import CSVConverter
from src.converters.mongodb_converter import MongoDBConverter
from src.converters.sql_converter import SQLConverter


# Test configuration
TEST_CONFIG = {
    'postgresql': {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'postgres',
        'password': 'your_password'
    },
    'mongodb': {
        'uri': 'mongodb://localhost:27017/',
        'database': 'test_conversion'
    },
    'output': {
        'output_dir': './output'
    },
    'logging': {
        'level': 'INFO',
        'file': './logs/test.log'
    }
}

# Sample test data
SAMPLE_DATA = [
    {
        'id': 1,
        'name': 'John Doe',
        'email': 'john@example.com',
        'age': 35,
        'address': {'street': '123 Main St', 'city': 'New York', 'zip': '10001'},
        'created_at': datetime(2023, 1, 15, 10, 30, 0),
        'is_active': True,
        'balance': Decimal('123.45')
    },
    {
        'id': 2,
        'name': 'Jane Smith',
        'email': 'jane@example.com',
        'age': 28,
        'address': {'street': '456 Park Ave', 'city': 'Boston', 'zip': '02108'},
        'created_at': datetime(2023, 2, 20, 14, 15, 0),
        'is_active': True,
        'balance': Decimal('678.90')
    }
]


class TestJsonConverter:
    """Tests for the JSON converter."""
    
    def test_convert_to_json(self):
        """Test converting data to JSON."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Convert data to JSON
            converter = JsonConverter(TEST_CONFIG)
            result = converter.convert(SAMPLE_DATA, temp_path)
            
            # Verify conversion was successful
            assert result is True
            
            # Verify the file exists
            assert os.path.exists(temp_path)
            
            # Verify the file contains valid JSON
            with open(temp_path, 'r') as f:
                data = json.load(f)
            
            # Verify the data was converted correctly
            assert len(data) == 2
            assert data[0]['name'] == 'John Doe'
            assert data[1]['name'] == 'Jane Smith'
            
            # Check date conversion
            assert data[0]['created_at'] == '2023-01-15T10:30:00'
            
            # Check decimal conversion
            assert data[0]['balance'] == 123.45
            assert data[1]['balance'] == 678.90
            
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)


class TestCSVConverter:
    """Tests for the CSV converter."""
    
    def test_convert_to_csv(self):
        """Test converting data to CSV."""
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Convert data to CSV
            converter = CSVConverter(TEST_CONFIG)
            result = converter.convert(SAMPLE_DATA, temp_path)
            
            # Verify conversion was successful
            assert result is True
            
            # Verify the file exists
            assert os.path.exists(temp_path)
            
            # Verify the file contains valid CSV data
            with open(temp_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            # Verify the data was converted correctly
            assert len(rows) == 2
            assert rows[0]['name'] == 'John Doe'
            assert rows[1]['name'] == 'Jane Smith'
            
            # Check that numeric values are converted correctly
            assert rows[0]['age'] == '35'
            assert rows[1]['age'] == '28'
            
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)


class TestSQLConverter:
    """Tests for the SQL converter."""
    
    def test_convert_to_sql(self):
        """Test converting data to SQL."""
        with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Configure SQL converter
            sql_config = TEST_CONFIG.copy()
            sql_config['sql'] = {'dialect': 'mysql', 'table_name': 'test_table'}
            
            # Convert data to SQL
            converter = SQLConverter(sql_config)
            result = converter.convert(SAMPLE_DATA, temp_path)
            
            # Verify conversion was successful
            assert result is True
            
            # Verify the file exists
            assert os.path.exists(temp_path)
            
            # Read the SQL file
            with open(temp_path, 'r') as f:
                sql_content = f.read()
            
            # Verify the SQL file contains CREATE TABLE
            assert 'CREATE TABLE' in sql_content
            assert '`name` VARCHAR' in sql_content
            
            # Verify the SQL file contains INSERT statements
            assert 'INSERT INTO' in sql_content
            assert 'John Doe' in sql_content
            assert 'Jane Smith' in sql_content
            
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)


class TestMongoDBConverter:
    """Tests for the MongoDB converter."""
    
    def test_convert_to_mongodb_json(self):
        """Test converting data to MongoDB JSON format."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Convert data to MongoDB format
            converter = MongoDBConverter(TEST_CONFIG)
            result = converter.convert(SAMPLE_DATA, temp_path)
            
            # Verify conversion was successful
            assert result is True
            
            # Verify the file exists
            assert os.path.exists(temp_path)
            
            # Verify the file contains valid JSON
            with open(temp_path, 'r') as f:
                data = json.load(f)
            
            # Verify the data was converted correctly
            assert len(data) == 2
            assert data[0]['name'] == 'John Doe'
            assert data[1]['name'] == 'Jane Smith'
            
            # Check date conversion to MongoDB format
            assert '$date' in data[0]['created_at']
            
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)
