"""
CSV Converter Module

This module converts PostgreSQL data to CSV format.
"""

import os
import csv
import logging
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
from decimal import Decimal
from .base_converter import BaseConverter


class CSVConverter(BaseConverter):
    """
    A class for converting PostgreSQL data to CSV format.
    """
    
    def __init__(self, config):
        """
        Initialize the CSV converter.
        
        Args:
            config (dict): Configuration settings
        """
        super().__init__(config)
        
    def convert(self, data, output_path):
        """
        Convert the data to CSV and save to file.
        
        Args:
            data (list): Data to convert (list of dictionaries)
            output_path (str): Path to save the CSV file
            
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
            
            # Use pandas for efficient CSV conversion
            df = pd.DataFrame(processed_data)
            
            logging.info(f"Writing {len(processed_data)} records to {output_path}")
            
            # Get CSV settings from config or use defaults
            csv_settings = self.config.get('csv', {})
            delimiter = csv_settings.get('delimiter', ',')
            quotechar = csv_settings.get('quotechar', '"')
            encoding = csv_settings.get('encoding', 'utf-8')
            
            # Write to CSV
            df.to_csv(
                output_path,
                index=False,
                sep=delimiter,
                quotechar=quotechar,
                encoding=encoding,
                quoting=csv.QUOTE_MINIMAL
            )
                
            logging.info(f"Successfully wrote data to {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"CSV conversion failed: {e}")
            return False
    
    def preprocess_data(self, data):
        """
        Preprocess the data before conversion to CSV.
        
        Args:
            data (list): Data to preprocess
            
        Returns:
            list: Preprocessed data
        """
        logging.info("Preprocessing data for CSV conversion")
        
        # For large datasets, show a progress bar
        if len(data) > 1000:
            processed_data = []
            for item in tqdm(data, desc="Processing data for CSV"):
                processed_data.append(self._process_item(item))
            return processed_data
        else:
            # For smaller datasets, process without progress bar
            return [self._process_item(item) for item in data]
    
    def _process_item(self, item):
        """
        Process a single data item for CSV conversion.
        
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
                processed_item[key] = ''
                continue
                
            # Convert complex data types to strings
            if isinstance(value, (dict, list)):
                processed_item[key] = str(value)
            elif isinstance(value, (datetime, date)):
                processed_item[key] = value.isoformat()
            elif isinstance(value, Decimal):
                processed_item[key] = float(value)
            elif isinstance(value, bytes):
                processed_item[key] = value.hex()
            else:
                processed_item[key] = value
            
        return processed_item
    
    def validate_data(self, data):
        """
        Validate the data before conversion to CSV.
        
        Args:
            data (list): Data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if not super().validate_data(data):
            return False
            
        # Additional CSV-specific validation
        if not data:
            return True  # Empty data is valid but will result in an empty CSV
            
        # Check if all dictionaries have the same keys
        # (CSV requires consistent columns)
        first_item_keys = set(data[0].keys())
        
        for i, item in enumerate(data[1:], 1):
            if set(item.keys()) != first_item_keys:
                logging.warning(f"Data item {i} has different keys than the first item")
                # Not returning False here, just warning the user
                
        return True
