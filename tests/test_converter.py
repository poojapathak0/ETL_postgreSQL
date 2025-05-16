"""
Test Script for PostgreSQL Data Format Converter

This script demonstrates how to use the PostgreSQL Data Format Converter
with a sample database. It creates a test table, inserts sample data,
and converts it to different formats.
"""

import os
import sys
import logging
import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from utils.db_connector import PostgreSQLConnector
from src.converters.json_converter import JsonConverter
from src.converters.mongodb_converter import MongoDBConverter
from src.converters.csv_converter import CSVConverter
from src.converters.sql_converter import SQLConverter
from utils.logger import setup_logger


# Configuration for the test
CONFIG = {
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


def setup_logging():
    """Set up logging for the test script."""
    os.makedirs('./logs', exist_ok=True)
    setup_logger('./logs/test.log', logging.INFO)


def create_test_database():
    """Create a test PostgreSQL database and sample table with data."""
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=CONFIG['postgresql']['host'],
            port=CONFIG['postgresql']['port'],
            user=CONFIG['postgresql']['user'],
            password=CONFIG['postgresql']['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Drop the test database if it exists
        cursor.execute(f"DROP DATABASE IF EXISTS {CONFIG['postgresql']['database']}")
        
        # Create a new test database
        cursor.execute(f"CREATE DATABASE {CONFIG['postgresql']['database']}")
        logging.info(f"Created test database: {CONFIG['postgresql']['database']}")
        
        # Close connection to server
        cursor.close()
        conn.close()
        
        # Connect to the new database
        conn = psycopg2.connect(
            host=CONFIG['postgresql']['host'],
            port=CONFIG['postgresql']['port'],
            database=CONFIG['postgresql']['database'],
            user=CONFIG['postgresql']['user'],
            password=CONFIG['postgresql']['password']
        )
        cursor = conn.cursor()
        
        # Create a sample table
        cursor.execute("""
        CREATE TABLE customers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INT,
            address JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
        """)
        
        # Insert sample data
        sample_data = [
            (
                "John Doe", 
                "john@example.com", 
                35, 
                '{"street": "123 Main St", "city": "New York", "zip": "10001"}',
                True
            ),
            (
                "Jane Smith", 
                "jane@example.com", 
                28, 
                '{"street": "456 Park Ave", "city": "Boston", "zip": "02108"}',
                True
            ),
            (
                "Bob Johnson", 
                "bob@example.com", 
                42, 
                '{"street": "789 Oak Dr", "city": "San Francisco", "zip": "94102"}',
                False
            ),
            (
                "Alice Brown", 
                "alice@example.com", 
                31, 
                '{"street": "101 Pine St", "city": "Seattle", "zip": "98101"}',
                True
            ),
            (
                "Charlie Wilson", 
                "charlie@example.com", 
                45, 
                '{"street": "202 Maple Ave", "city": "Chicago", "zip": "60601"}',
                False
            )
        ]
        
        for data in sample_data:
            cursor.execute("""
            INSERT INTO customers (name, email, age, address, is_active)
            VALUES (%s, %s, %s, %s, %s)
            """, data)
            
        conn.commit()
        logging.info("Created sample table 'customers' with 5 records")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logging.error(f"Failed to create test database: {e}")
        return False


def run_converters():
    """Run all converters to demonstrate functionality."""
    # Create output directory
    os.makedirs('./output', exist_ok=True)
    
    # Initialize PostgreSQL connector
    pg_connector = PostgreSQLConnector(CONFIG['postgresql'])
    
    # Get the data from the customers table
    data = pg_connector.get_table_data('customers')
    logging.info(f"Retrieved {len(data)} records from 'customers' table")
    
    # Convert to JSON
    json_converter = JsonConverter(CONFIG)
    json_converter.convert(data, './output/customers.json')
    
    # Convert to CSV
    csv_converter = CSVConverter(CONFIG)
    csv_converter.convert(data, './output/customers.csv')
    
    # Convert to SQL (MySQL format)
    sql_config = CONFIG.copy()
    sql_config['sql'] = {'dialect': 'mysql', 'table_name': 'customers'}
    sql_converter = SQLConverter(sql_config)
    sql_converter.convert(data, './output/customers_mysql.sql')
    
    # Convert to MongoDB compatible format
    mongodb_converter = MongoDBConverter(CONFIG)
    mongodb_converter.convert(data, './output/customers_mongodb.json')
    
    logging.info("All conversions completed successfully")


def main():
    """Main function to run the demonstration."""
    parser = argparse.ArgumentParser(description='Run a demonstration of the PostgreSQL Data Format Converter')
    parser.add_argument('--skip-db-setup', action='store_true', help='Skip database setup (use if already set up)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logging.info("Starting PostgreSQL Data Format Converter demonstration")
    
    # Create test database unless skipped
    if not args.skip_db_setup:
        if not create_test_database():
            logging.error("Failed to set up test database, exiting")
            return
    
    # Run the converters
    run_converters()
    
    logging.info("Demonstration completed successfully!")
    logging.info("Output files have been saved to the './output' directory")


if __name__ == "__main__":
    main()
