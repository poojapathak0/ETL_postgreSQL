"""
Logger Module

This module sets up logging for the application.
"""

import os
import logging
from logging.handlers import RotatingFileHandler


def setup_logger(log_file, log_level=logging.INFO):
    """
    Set up the application logger.
    
    Args:
        log_file (str): Path to the log file
        log_level (int): Logging level
    """
    # Create log directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create handlers
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5
    )
    console_handler = logging.StreamHandler()
    
    # Set levels
    file_handler.setLevel(log_level)
    console_handler.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logging.info("Logger initialized")
