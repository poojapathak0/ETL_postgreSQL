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


def list_tables(pg_connector, schema_name='public'):
    """
    List tables in a given schema.
    
    Args:
        pg_connector (PostgreSQLConnector): Instance of PostgreSQLConnector.
        schema_name (str): Name of the schema to list tables from.
        
    Returns:
        list: A list of table names.
    """
    query = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{schema_name}';"
    try:
        tables_data = pg_connector.execute_query(query)
        return [row['tablename'] for row in tables_data]
    except Exception as e:
        logging.error(f"Failed to list tables: {e}")
        return []

def interactive_mode(config, pg_connector):
    """
    Run the converter in interactive mode.
    
    Args:
        config (dict): Configuration settings.
        pg_connector (PostgreSQLConnector): Instance of PostgreSQLConnector.
        
    Returns:
        tuple: (table_name, format_type) or (None, None) if user aborts.
    """
    print("\nWelcome to the PostgreSQL Data Format Converter - Interactive Mode!")
    
    # Confirm database
    default_db = config.get('postgresql', {}).get('database', 'your_default_db')
    db_name = input(f"Enter PostgreSQL database name (default: {default_db}): ") or default_db
    
    # Update config if a different DB is entered (in-memory for this session)
    # This assumes pg_connector can be re-initialized or its connection can be changed.
    # For simplicity, we'll assume the initial pg_connector used the default_db or was already configured.
    # If the user enters a different DB, we might need to re-initialize pg_connector
    # or modify its connection parameters if the class supports it.
    # For this example, we'll log if it's different but proceed with the initial pg_connector.
    if db_name != pg_connector.config.get('database'):
        logging.info(f"Connecting to database: {db_name} (Note: Initial connection was to {pg_connector.config.get('database')})")
        # Ideally, re-initialize or update pg_connector here if db_name is different
        # For now, we'll assume the user wants to use the initially configured connection if they
        # entered a different DB name but the connector doesn't support dynamic change.
        # Or, we could modify PostgreSQLConnector to accept dbname on execute_query/get_table_data.

    print(f"\nFetching tables from database '{db_name}', schema 'public'...")
    tables = list_tables(pg_connector)
    
    if not tables:
        print("No tables found in the 'public' schema or failed to connect.")
        return None, None
        
    print("\nAvailable tables:")
    for i, table in enumerate(tables):
        print(f"{i + 1}. {table}")
        
    while True:
        try:
            choice = input("Select a table by number (or 'q' to quit): ")
            if choice.lower() == 'q':
                return None, None
            table_index = int(choice) - 1
            if 0 <= table_index < len(tables):
                selected_table = tables[table_index]
                break
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")
            
    print(f"\nYou selected table: {selected_table}")
    
    # Get format
    supported_formats = ['json', 'csv', 'mongodb', 'sql']
    print("\nSupported output formats:")
    for i, fmt in enumerate(supported_formats):
        print(f"{i + 1}. {fmt}")
        
    while True:
        try:
            choice = input("Select an output format by number (or 'q' to quit): ")
            if choice.lower() == 'q':
                return None, None
            format_index = int(choice) - 1
            if 0 <= format_index < len(supported_formats):
                selected_format = supported_formats[format_index]
                break
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")
            
    print(f"You selected format: {selected_format}")
    
    return selected_table, selected_format

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
    log_level = logging.DEBUG if args.verbose else config.get('logging', {}).get('level', 'INFO').upper()
    log_file = config.get('logging', {}).get('file', './logs/conversion.log')
    setup_logger(log_file, log_level)
    
    logging.info("Starting PostgreSQL Data Format Converter")
    
    # Initialize PostgreSQL connector
    try:
        pg_connector = PostgreSQLConnector(config['postgresql'])
        logging.info(f"Successfully connected to PostgreSQL database: {config['postgresql'].get('database')}")
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

    table_to_convert = args.table
    format_type = args.format or config['output']['default_format']
    query_to_execute = args.query

    if not query_to_execute and not table_to_convert:
        # Enter interactive mode
        selected_table, selected_format = interactive_mode(config, pg_connector)
        if selected_table and selected_format:
            table_to_convert = selected_table
            format_type = selected_format
            query_to_execute = f"SELECT * FROM public.{table_to_convert};" # Default query for selected table
        else:
            logging.info("Interactive mode exited by user or no selection made.")
            print("Exiting application.")
            sys.exit(0)
    
    logging.info(f"Output format: {format_type}")
    
    # Initialize the appropriate converter
    converter = get_converter(format_type, config)
    
    # Extract data from PostgreSQL
    try:
        if query_to_execute:
            logging.info(f"Executing custom query: {query_to_execute}")
            data = pg_connector.execute_query(query_to_execute)
        # The case for args.table without args.query is now handled by constructing query_to_execute
        # elif table_to_convert: 
        #     logging.info(f"Extracting data from table: {table_to_convert}")
        #     # Construct a default query if only table name is provided
        #     default_query = f"SELECT * FROM public.{table_to_convert};" 
        #     data = pg_connector.execute_query(default_query)
        else:
            # This condition should ideally not be met if interactive mode forces a selection
            # or CLI args are validated properly.
            logging.error("No query or table specified, and interactive mode did not yield a selection.")
            sys.exit(1)
            
        if data is None: # Check if execute_query returned None (e.g. on error)
            logging.error("Failed to extract data. Query might have failed or returned no result.")
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
