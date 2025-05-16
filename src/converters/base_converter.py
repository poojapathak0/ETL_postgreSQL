"""
Base Converter Module

This module provides the base class for all data format converters.
"""

import logging
from abc import ABC, abstractmethod


class BaseConverter(ABC):
    """
    Abstract base class for all data converters.
    """
    
    def __init__(self, config):
        """
        Initialize the converter with configuration settings.
        
        Args:
            config (dict): Configuration settings
        """
        self.config = config
        
    @abstractmethod
    def convert(self, data, output_path):
        """
        Convert the data to the target format.
        
        Args:
            data (list): Data to convert (list of dictionaries)
            output_path (str): Path to save the converted data
            
        Returns:
            bool: True if conversion successful, False otherwise
        """
        pass
    
    def preprocess_data(self, data):
        """
        Preprocess the data before conversion.
        Can be overridden by subclasses.
        
        Args:
            data (list): Data to preprocess
            
        Returns:
            list: Preprocessed data
        """
        logging.info("Preprocessing data before conversion")
        return data
    
    def validate_data(self, data):
        """
        Validate the data before conversion.
        Can be overridden by subclasses.
        
        Args:
            data (list): Data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if not data:
            logging.warning("Empty dataset provided for conversion")
            return False
            
        if not isinstance(data, list):
            logging.error("Data must be a list of dictionaries")
            return False
            
        if not all(isinstance(item, dict) for item in data):
            logging.error("All data items must be dictionaries")
            return False
            
        return True
