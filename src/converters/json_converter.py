"""
JSON Converter Module

This module converts PostgreSQL data to JSON format.
"""

import os
import json
import logging
from tqdm import tqdm
from datetime import datetime, date
from decimal import Decimal
from .base_converter import BaseConverter


class JsonConverter(BaseConverter):
    """
    A class for converting PostgreSQL data to JSON format.
    """
    
    def __init__(self, config):
        """
        Initialize the JSON converter.
        
        Args:
            config (dict): Configuration settings
        """
        super().__init__(config)
        
    def convert(self, data, output_path):
        """
        Convert the data to JSON and save to file.
        
        Args:
            data (list): Data to convert (list of dictionaries)
            output_path (str): Path to save the JSON file
            
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
            
            # Write the data to the JSON file
            logging.info(f"Writing {len(processed_data)} records to {output_path}")
            
            with open(output_path, 'w', encoding='utf-8') as json_file:
                json.dump(processed_data, json_file, cls=PostgreSQLJSONEncoder, 
                         indent=2, ensure_ascii=False)
                
            logging.info(f"Successfully wrote data to {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"JSON conversion failed: {e}")
            return False
    
    def preprocess_data(self, data):
        """
        Preprocess the data before conversion to JSON.
        
        Args:
            data (list): Data to preprocess
            
        Returns:
            list: Preprocessed data
        """
        logging.info("Preprocessing data for JSON conversion")
        
        # For large datasets, show a progress bar
        if len(data) > 1000:
            processed_data = []
            for item in tqdm(data, desc="Processing data for JSON"):
                processed_data.append(self._process_item(item))
            return processed_data
        else:
            # For smaller datasets, process without progress bar
            return [self._process_item(item) for item in data]
    
    def _process_item(self, item):
        """
        Process a single data item for JSON conversion.
        
        Args:
            item (dict): Data item to process
            
        Returns:
            dict: Processed data item
        """
        # Create a new dictionary to avoid modifying the original
        processed_item = {}
        
        for key, value in item.items():
            # Handle None values
            if value is None:
                processed_item[key] = None
                continue
                
            # Process different data types as needed
            # (the PostgreSQLJSONEncoder will handle most types)
            processed_item[key] = value
            
        return processed_item


class PostgreSQLJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for PostgreSQL data types.
    """
    
    def default(self, obj):
        # Handle date and datetime objects
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
            
        # Handle Decimal objects
        if isinstance(obj, Decimal):
            return float(obj)
            
        # Handle bytes objects
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
            
        # Handle custom PostgreSQL types or other types as needed
        # Add more type handlers as required
        
        # Let the base class handle anything we don't explicitly handle
        return super().default(obj)
