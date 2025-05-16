"""
SQL Converter Module

This module converts PostgreSQL data to SQL statements (for other databases).
"""

import os
import logging
from tqdm import tqdm
from datetime import datetime, date
from decimal import Decimal
from .base_converter import BaseConverter


class SQLConverter(BaseConverter):
    """
    A class for converting PostgreSQL data to SQL statements.
    """
    
    def __init__(self, config):
        """
        Initialize the SQL converter.
        
        Args:
            config (dict): Configuration settings
        """
        super().__init__(config)
        # Get SQL-specific configuration
        self.sql_config = config.get('sql', {})
        self.target_dialect = self.sql_config.get('dialect', 'mysql')
        
    def convert(self, data, output_path):
        """
        Convert the data to SQL statements and save to file.
        
        Args:
            data (list): Data to convert (list of dictionaries)
            output_path (str): Path to save the SQL file
            
        Returns:
            bool: True if conversion successful, False otherwise
        """
        if not self.validate_data(data):
            return False
            
        try:
            # Preprocess the data
            processed_data = self.preprocess_data(data)
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Determine table name from config or output path
            table_name = self.sql_config.get('table_name')
            if not table_name:
                # Extract from output path
                base_filename = os.path.basename(output_path)
                table_name = os.path.splitext(base_filename)[0]
            
            logging.info(f"Generating SQL statements for table '{table_name}'")
            
            # Generate SQL statements
            sql_statements = self._generate_sql(processed_data, table_name)
            
            # Write SQL to file
            with open(output_path, 'w', encoding='utf-8') as sql_file:
                sql_file.write(sql_statements)
                
            logging.info(f"Successfully wrote SQL statements to {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"SQL conversion failed: {e}")
            return False
    
    def _generate_sql(self, data, table_name):
        """
        Generate SQL statements for the data.
        
        Args:
            data (list): Data to convert
            table_name (str): Name of the target table
            
        Returns:
            str: SQL statements
        """
        if not data:
            return f"-- No data to convert for table {table_name}\n"
        
        # Start with CREATE TABLE statement
        sql = self._generate_create_table(data, table_name)
        
        # Add INSERT statements
        sql += "\n\n-- Data insertion statements\n"
        
        batch_size = self.sql_config.get('batch_size', 100)
        use_batch_insert = self.sql_config.get('use_batch_insert', True)
        
        if use_batch_insert:
            # Generate batch INSERT statements
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                sql += self._generate_batch_insert(batch, table_name)
                sql += "\n"
        else:
            # Generate individual INSERT statements
            for item in data:
                sql += self._generate_single_insert(item, table_name)
                sql += "\n"
                
        return sql
    
    def _generate_create_table(self, data, table_name):
        """
        Generate CREATE TABLE statement based on the data.
        
        Args:
            data (list): Sample data to infer column types
            table_name (str): Name of the table
            
        Returns:
            str: CREATE TABLE statement
        """
        if not data:
            return f"-- Cannot generate CREATE TABLE for {table_name} - no data provided\n"
            
        # Get column names and infer types from first row
        columns = []
        for key, value in data[0].items():
            column_type = self._infer_sql_type(value)
            columns.append(f"`{key}` {column_type}")
            
        # Create the statement
        sql = f"CREATE TABLE `{table_name}` (\n"
        sql += ",\n".join(f"    {col}" for col in columns)
        sql += "\n);\n"
        
        return sql
    
    def _infer_sql_type(self, value):
        """
        Infer SQL column type from Python value.
        
        Args:
            value: The Python value
            
        Returns:
            str: SQL column type
        """
        if value is None:
            return "VARCHAR(255)"
            
        if isinstance(value, int):
            return "INT"
            
        if isinstance(value, (float, Decimal)):
            return "DECIMAL(18, 6)"
            
        if isinstance(value, bool):
            return "BOOLEAN"
            
        if isinstance(value, datetime):
            return "DATETIME"
            
        if isinstance(value, date):
            return "DATE"
            
        if isinstance(value, (dict, list)):
            # For JSON data types
            if self.target_dialect in ('mysql', 'mariadb'):
                return "JSON"
            elif self.target_dialect == 'postgresql':
                return "JSONB"
            else:
                return "TEXT"
                
        if isinstance(value, bytes):
            return "BLOB"
            
        # Default to VARCHAR with length based on actual value
        if isinstance(value, str):
            length = min(len(value) * 2, 255)  # Double the length to account for unicode, cap at 255
            return f"VARCHAR({max(length, 1)})"
            
        return "VARCHAR(255)"  # Default fallback
    
    def _generate_batch_insert(self, data_batch, table_name):
        """
        Generate a batch INSERT statement for multiple rows.
        
        Args:
            data_batch (list): Batch of data rows
            table_name (str): Name of the table
            
        Returns:
            str: Batch INSERT statement
        """
        if not data_batch:
            return ""
            
        # Get column names from first row
        columns = list(data_batch[0].keys())
        
        # Start the INSERT statement
        sql = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES\n"
        
        # Add value tuples for each row
        values = []
        for item in data_batch:
            value_str = "("
            value_str += ", ".join(self._format_sql_value(item.get(col)) for col in columns)
            value_str += ")"
            values.append(value_str)
            
        sql += ",\n".join(values) + ";"
        
        return sql
    
    def _generate_single_insert(self, data_item, table_name):
        """
        Generate a single INSERT statement for one row.
        
        Args:
            data_item (dict): Data row
            table_name (str): Name of the table
            
        Returns:
            str: INSERT statement
        """
        # Get column names
        columns = list(data_item.keys())
        
        # Format values
        values = [self._format_sql_value(data_item[col]) for col in columns]
        
        # Create the statement
        sql = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) "
        sql += f"VALUES ({', '.join(values)});"
        
        return sql
    
    def _format_sql_value(self, value):
        """
        Format a Python value for use in SQL statements.
        
        Args:
            value: Python value to format
            
        Returns:
            str: Formatted SQL value
        """
        if value is None:
            return "NULL"
            
        if isinstance(value, (int, float, Decimal)):
            return str(value)
            
        if isinstance(value, bool):
            if self.target_dialect in ('mysql', 'mariadb'):
                return "1" if value else "0"
            else:
                return "TRUE" if value else "FALSE"
                
        if isinstance(value, (datetime, date)):
            return f"'{value.isoformat()}'"
            
        if isinstance(value, (dict, list)):
            import json
            json_str = json.dumps(value)
            return f"'{json_str.replace("'", "''")}'"
            
        if isinstance(value, bytes):
            hex_str = value.hex()
            if self.target_dialect in ('mysql', 'mariadb'):
                return f"X'{hex_str}'"
            else:
                return f"'\\x{hex_str}'"
                
        # Escape single quotes and wrap in quotes
        return f"'{str(value).replace("'", "''")}'"
    
    def preprocess_data(self, data):
        """
        Preprocess the data before conversion to SQL.
        
        Args:
            data (list): Data to preprocess
            
        Returns:
            list: Preprocessed data
        """
        logging.info("Preprocessing data for SQL conversion")
        
        # For large datasets, show a progress bar
        if len(data) > 1000:
            processed_data = []
            for item in tqdm(data, desc="Processing data for SQL"):
                processed_data.append(self._process_item(item))
            return processed_data
        else:
            # For smaller datasets, process without progress bar
            return [self._process_item(item) for item in data]
    
    def _process_item(self, item):
        """
        Process a single data item for SQL conversion.
        
        Args:
            item (dict): Data item to process
            
        Returns:
            dict: Processed data item
        """
        # Create a new dictionary to avoid modifying the original
        processed_item = {}
        
        for key, value in item.items():
            # Handle column names that need to be modified for SQL compatibility
            new_key = self._sanitize_column_name(key)
            processed_item[new_key] = value
            
        return processed_item
    
    def _sanitize_column_name(self, column_name):
        """
        Sanitize column names for SQL compatibility.
        
        Args:
            column_name (str): Original column name
            
        Returns:
            str: Sanitized column name
        """
        # Replace spaces with underscores
        sanitized = column_name.replace(' ', '_')
        
        # Replace other problematic characters
        for char in ['(', ')', '.', ',', ';', ':', '!', '?', '%', '$', '#', '@', '*', '+', '/', '\\']:
            sanitized = sanitized.replace(char, '_')
            
        # Ensure the column name doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'c_' + sanitized
            
        return sanitized
