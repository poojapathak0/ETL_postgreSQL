#!/usr/bin/env python
"""
PostgreSQL Data Format Converter - Main Application

This is the main entry point for the PostgreSQL Data Format Converter tool,
which allows users to convert PostgreSQL data into various formats like
JSON, MongoDB (BSON), CSV, and SQL.
"""

import argparse
import os
import sys
import yaml
import logging
from tqdm import tqdm
from dotenv import load_dotenv

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import converter modules
from src.converters.json_converter import JsonConverter
from src.converters.mongodb_converter import MongoDBConverter
from src.converters.csv_converter import CSVConverter
from src.converters.sql_converter import SQLConverter
from utils.db_connector import PostgreSQLConnector
from utils.logger import setup_logger


def load_config(config_path):
    """
    Load configuration from a YAML file.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        dict: Configuration settings
    """
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        sys.exit(1)


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Convert PostgreSQL data to various formats'
    )
    
    parser.add_argument(
        '--config', 
        type=str,
        default='config/default_config.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        choices=['json', 'mongodb', 'csv', 'sql'],
        help='Output format (json, mongodb, csv, or sql)'
    )
    
    parser.add_argument(
        '--query',
        type=str,
        help='SQL query to extract data (overrides table argument)'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        help='PostgreSQL table to extract data from'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Output file or location'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser.parse_args()


def get_converter(format_type, config):
    """
    Get the appropriate converter based on the format type.
    
    Args:
        format_type (str): Type of converter to use
        config (dict): Configuration settings
        
    Returns:
        object: Converter instance
    """
    converters = {
        'json': JsonConverter,
        'mongodb': MongoDBConverter,
        'csv': CSVConverter,
        'sql': SQLConverter
    }
    
    if format_type not in converters:
        logging.error(f"Unsupported format: {format_type}")
        sys.exit(1)
        
    return converters[format_type](config)


def main():
    """
    Main entry point for the application.
    """
    # Load environment variables
    load_dotenv()
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(config['logging']['file'], log_level)
    
    logging.info("Starting PostgreSQL Data Format Converter")
    
    # Get the format type (command line argument or from config)
    format_type = args.format or config['output']['default_format']
    logging.info(f"Output format: {format_type}")
    
    # Initialize PostgreSQL connector
    try:
        pg_connector = PostgreSQLConnector(config['postgresql'])
        logging.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)
    
    # Initialize the appropriate converter
    converter = get_converter(format_type, config)
    
    # Extract data from PostgreSQL
    try:
        if args.query:
            logging.info(f"Executing custom query: {args.query}")
            data = pg_connector.execute_query(args.query)
        elif args.table:
            logging.info(f"Extracting data from table: {args.table}")
            data = pg_connector.get_table_data(args.table)
        else:
            logging.error("No query or table specified")
            sys.exit(1)
            
        logging.info(f"Successfully extracted {len(data)} records")
    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        sys.exit(1)
    
    # Convert and output the data
    try:
        output_path = args.output or os.path.join(
            config['output']['output_dir'], 
            f"output.{format_type}"
        )
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        logging.info(f"Converting data to {format_type} format")
        converter.convert(data, output_path)
        logging.info(f"Data successfully converted and saved to {output_path}")
    except Exception as e:
        logging.error(f"Conversion failed: {e}")
        sys.exit(1)
    
    logging.info("Conversion process completed successfully")


if __name__ == "__main__":
    main()
