"""
Simple Test Script for PostgreSQL Data Format Converter

This script tests the converters with sample data from a JSON file
without requiring a database connection.
"""

import os
import sys
import json
import logging

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Set up basic logging
logging.basicConfig(level=logging.INFO)

from src.converters.json_converter import JsonConverter
from src.converters.csv_converter import CSVConverter
from src.converters.mongodb_converter import MongoDBConverter
from src.converters.sql_converter import SQLConverter

def main():
    """
    Main function to test the converters with sample data.
    """
    # Load sample data from JSON file
    try:
        with open('test_data.json', 'r') as f:
            data = json.load(f)
        logging.info(f"Loaded {len(data)} records from test_data.json")
    except Exception as e:
        logging.error(f"Failed to load test data: {e}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    
    # Basic configuration
    config = {
        'output': {
            'output_dir': './output'
        },
        'logging': {
            'level': 'INFO',
            'file': './logs/conversion.log'
        }
    }
    
    # Test JSON converter
    try:
        logging.info("Testing JSON converter...")
        json_converter = JsonConverter(config)
        json_converter.convert(data, 'output/test_output.json')
        logging.info("JSON conversion completed successfully")
    except Exception as e:
        logging.error(f"JSON conversion failed: {e}")
    
    # Test CSV converter
    try:
        logging.info("Testing CSV converter...")
        csv_converter = CSVConverter(config)
        csv_converter.convert(data, 'output/test_output.csv')
        logging.info("CSV conversion completed successfully")
    except Exception as e:
        logging.error(f"CSV conversion failed: {e}")
    
    # Test MongoDB JSON converter
    try:
        logging.info("Testing MongoDB converter...")
        mongodb_converter = MongoDBConverter(config)
        mongodb_converter.convert(data, 'output/test_output_mongodb.json')
        logging.info("MongoDB conversion completed successfully")
    except Exception as e:
        logging.error(f"MongoDB conversion failed: {e}")
    
    # Test SQL converter
    try:
        logging.info("Testing SQL converter...")
        sql_config = config.copy()
        sql_config['sql'] = {'dialect': 'mysql', 'table_name': 'test_table'}
        sql_converter = SQLConverter(sql_config)
        sql_converter.convert(data, 'output/test_output.sql')
        logging.info("SQL conversion completed successfully")
    except Exception as e:
        logging.error(f"SQL conversion failed: {e}")
    
    logging.info("All tests completed")

if __name__ == "__main__":
    main()
