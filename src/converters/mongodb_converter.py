"""
MongoDB Converter Module

This module converts PostgreSQL data to MongoDB format and can load it into a MongoDB database.
"""

import os
import json
import logging
from tqdm import tqdm
from pymongo import MongoClient
from datetime import datetime, date
from decimal import Decimal
from .base_converter import BaseConverter


class MongoDBConverter(BaseConverter):
    """
    A class for converting PostgreSQL data to MongoDB format.
    """
    
    def __init__(self, config):
        """
        Initialize the MongoDB converter.
        
        Args:
            config (dict): Configuration settings
        """
        super().__init__(config)
        self.mongo_config = config.get('mongodb', {})
        
    def convert(self, data, output_path):
        """
        Convert the data to MongoDB format.
        If output_path is 'mongodb://', data will be inserted into MongoDB.
        Otherwise, it will be saved as BSON-compatible JSON.
        
        Args:
            data (list): Data to convert (list of dictionaries)
            output_path (str): Path to save the data or 'mongodb://'
            
        Returns:
            bool: True if conversion successful, False otherwise
        """
        if not self.validate_data(data):
            return False
            
        try:
            # Preprocess the data
            processed_data = self.preprocess_data(data)
            
            # Determine if we need to insert into MongoDB or save to file
            if output_path.startswith('mongodb://'):
                return self._insert_to_mongodb(processed_data)
            else:
                return self._save_to_file(processed_data, output_path)
                
        except Exception as e:
            logging.error(f"MongoDB conversion failed: {e}")
            return False
    
    def _insert_to_mongodb(self, data):
        """
        Insert the data into a MongoDB database.
        
        Args:
            data (list): Data to insert
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        try:
            # Connect to MongoDB
            logging.info(f"Connecting to MongoDB at {self.mongo_config['uri']}")
            client = MongoClient(self.mongo_config['uri'])
            db = client[self.mongo_config['database']]
            
            # Determine collection name (could be configured or derived)
            collection_name = self.mongo_config.get('collection', 'postgresql_data')
            collection = db[collection_name]
            
            # Insert the data
            logging.info(f"Inserting {len(data)} records into MongoDB collection '{collection_name}'")
            
            if len(data) > 1000:
                # For large datasets, show progress and insert in batches
                batch_size = 500
                for i in tqdm(range(0, len(data), batch_size), desc="Inserting into MongoDB"):
                    batch = data[i:i+batch_size]
                    collection.insert_many(batch)
            else:
                # For smaller datasets, insert all at once
                collection.insert_many(data)
                
            logging.info(f"Successfully inserted {len(data)} records into MongoDB")
            return True
            
        except Exception as e:
            logging.error(f"MongoDB insertion failed: {e}")
            return False
            
    def _save_to_file(self, data, output_path):
        """
        Save the data to a file in BSON-compatible JSON format.
        
        Args:
            data (list): Data to save
            output_path (str): Path to save the file
            
        Returns:
            bool: True if save successful, False otherwise
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write the data to the file
            logging.info(f"Writing {len(data)} records to {output_path}")
            
            with open(output_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, cls=MongoDBJSONEncoder, 
                         indent=2, ensure_ascii=False)
                
            logging.info(f"Successfully wrote data to {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"File writing failed: {e}")
            return False
    
    def preprocess_data(self, data):
        """
        Preprocess the data before conversion to MongoDB format.
        
        Args:
            data (list): Data to preprocess
            
        Returns:
            list: Preprocessed data
        """
        logging.info("Preprocessing data for MongoDB conversion")
        
        # For large datasets, show a progress bar
        if len(data) > 1000:
            processed_data = []
            for item in tqdm(data, desc="Processing data for MongoDB"):
                processed_data.append(self._process_item(item))
            return processed_data
        else:
            # For smaller datasets, process without progress bar
            return [self._process_item(item) for item in data]
    
    def _process_item(self, item):
        """
        Process a single data item for MongoDB conversion.
        
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
                
            # Convert keys that start with numbers or contain dots
            if key[0].isdigit() or '.' in key:
                new_key = f"_{key}" if key[0].isdigit() else key.replace('.', '_')
                processed_item[new_key] = value
            else:
                processed_item[key] = value
            
        return processed_item


class MongoDBJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for MongoDB-compatible data.
    """
    
    def default(self, obj):
        # Handle date and datetime objects
        if isinstance(obj, (datetime, date)):
            return {"$date": obj.isoformat()}
            
        # Handle Decimal objects
        if isinstance(obj, Decimal):
            return float(obj)
            
        # Handle bytes objects
        if isinstance(obj, bytes):
            return {"$binary": obj.hex(), "$type": "00"}
            
        # Let the base class handle anything we don't explicitly handle
        return super().default(obj)
