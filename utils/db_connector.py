"""
Database Connector Module

This module provides connection utilities for PostgreSQL databases.
"""

import logging
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor


class PostgreSQLConnector:
    """
    A class for connecting to and retrieving data from PostgreSQL databases.
    """
    
    def __init__(self, config):
        """
        Initialize the PostgreSQL connector with configuration settings.
        
        Args:
            config (dict): PostgreSQL connection configuration
        """
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """
        Establish a connection to the PostgreSQL database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            
            # Use RealDictCursor to return data as dictionaries
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            return False
            
    def disconnect(self):
        """
        Close the database connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        
    def execute_query(self, query, params=None):
        """
        Execute a SQL query and return the results.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            list: Query results as a list of dictionaries
        """
        try:
            if not self.connection or self.connection.closed:
                self.connect()
                
            self.cursor.execute(query, params)
            results = self.cursor.fetchall()
            
            # Convert to list of dictionaries
            return [dict(row) for row in results]
        except Exception as e:
            logging.error(f"Query execution error: {e}")
            raise
            
    def get_table_data(self, table_name, limit=None):
        """
        Get data from a specific table.
        
        Args:
            table_name (str): Name of the table to query
            limit (int, optional): Maximum number of rows to return
            
        Returns:
            list: Table data as a list of dictionaries
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query)
        
    def get_table_schema(self, table_name):
        """
        Get the schema information for a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            list: Table schema information
        """
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        return self.execute_query(query, (table_name,))
    
    def get_all_tables(self):
        """
        Get a list of all tables in the database.
        
        Returns:
            list: Names of all tables in the database
        """
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        results = self.execute_query(query)
        return [row['table_name'] for row in results]
