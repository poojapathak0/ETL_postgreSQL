# PostgreSQL Data Format Converter

## Overview

The PostgreSQL Data Format Converter is a powerful Python tool designed to convert data from PostgreSQL databases into various target formats, including JSON, MongoDB, CSV, and SQL. This project provides a flexible, efficient, and user-friendly solution for data engineers, database administrators, and developers who need to migrate or process data across different platforms.

## Features

- **Multiple Export Formats**: Convert PostgreSQL data to JSON, MongoDB format, CSV, or SQL statements
- **High Performance**: Optimized for large datasets with progress tracking
- **Type Safety**: Proper handling of complex PostgreSQL data types
- **Flexible Configuration**: Customize the conversion process through configuration
- **Comprehensive Logging**: Detailed logs to track the conversion process
- **Command-Line Interface**: Easy to use from the command line

## Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL database
- MongoDB (optional, only needed for direct MongoDB imports)

### Steps

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd postgresql-data-converter
   ```

2. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure your database settings in `config/default_config.yaml`

## Usage

### Basic Usage

The converter can be run from the command line:

```bash
python main.py --format json --table customers --output ./output/customers.json
```

### Command Line Options

- `--config`: Path to configuration file (default: `config/default_config.yaml`)
- `--format`: Output format - `json`, `mongodb`, `csv`, or `sql`
- `--query`: Custom SQL query to extract data
- `--table`: PostgreSQL table to extract data from
- `--output`: Output file or location
- `--verbose`: Enable verbose output

### Configuration File

The configuration file (`config/default_config.yaml`) contains settings for:

- PostgreSQL connection details
- MongoDB connection details
- Output settings
- Logging configuration

Example:

```yaml
postgresql:
  host: localhost
  port: 5432
  database: mydb
  user: postgres
  password: your_password
  
mongodb:
  uri: mongodb://localhost:27017/
  database: converted_data

output:
  default_format: json
  output_dir: ./output
  
logging:
  level: INFO
  file: ./logs/conversion.log
```

### Examples

1. Convert a table to JSON:
   ```bash
   python main.py --format json --table customers --output ./output/customers.json
   ```

2. Convert a custom query result to CSV:
   ```bash
   python main.py --format csv --query "SELECT * FROM customers WHERE age > 30" --output ./output/customers_over_30.csv
   ```

3. Convert a table to MongoDB format and save as JSON:
   ```bash
   python main.py --format mongodb --table customers --output ./output/customers_mongo.json
   ```

4. Convert a table to SQL statements:
   ```bash
   python main.py --format sql --table customers --output ./output/customers.sql
   ```

5. Run with a custom configuration:
   ```bash
   python main.py --config ./my_config.yaml --format json --table customers
   ```

## Project Structure

```
postgresql-data-converter/
├── config/                      # Configuration files
│   └── default_config.yaml      # Default configuration
├── logs/                        # Log files (created automatically)
├── output/                      # Output files (created automatically)
├── src/                         # Source code
│   ├── __init__.py
│   └── converters/              # Converter modules
│       ├── __init__.py
│       ├── base_converter.py    # Base converter class
│       ├── csv_converter.py     # CSV converter
│       ├── json_converter.py    # JSON converter
│       ├── mongodb_converter.py # MongoDB converter
│       └── sql_converter.py     # SQL converter
├── tests/                       # Test scripts
│   ├── test_converter.py        # Demo test script
│   └── test_unit.py             # Unit tests
├── utils/                       # Utility modules
│   ├── __init__.py
│   ├── db_connector.py          # Database connector
│   └── logger.py                # Logging setup
├── main.py                      # Main application entry point
└── requirements.txt             # Project dependencies
```

## Understanding the Code (For Beginners)

### Main Components Explained

1. **main.py**: The entry point of the application. It:
   - Parses command line arguments
   - Loads the configuration
   - Sets up logging
   - Connects to the PostgreSQL database
   - Selects the right converter
   - Runs the conversion process

2. **db_connector.py**: Connects to PostgreSQL and retrieves data. Think of it as a bridge between our app and the database.

3. **Converters**: These are like translators that take PostgreSQL data and convert it to different formats:
   - **BaseConverter**: The parent class with common functionality
   - **JsonConverter**: Converts to JSON format
   - **CSVConverter**: Converts to CSV spreadsheet format
   - **MongoDBConverter**: Converts to MongoDB format
   - **SQLConverter**: Converts to SQL statements for other databases

4. **logger.py**: Keeps track of what's happening in the application. Like a diary for the app.

### How Data Flows Through the Application

1. You run a command like `python main.py --format json --table customers`
2. The application:
   - Reads your command (`--format json --table customers`)
   - Connects to PostgreSQL
   - Gets data from the `customers` table
   - Passes the data to the JSON converter
   - The converter transforms the data to JSON format
   - The converter saves the data to a file

It's like a factory where raw materials (PostgreSQL data) go in one end, get processed by machines (converters), and come out as finished products (JSON, CSV, etc.) at the other end.

## Testing

The project includes two types of tests:

1. **Demonstration Test**: 
   ```bash
   python tests/test_converter.py
   ```
   This creates a test database, populates it with sample data, and demonstrates conversion to all formats.

2. **Unit Tests**:
   ```bash
   pytest tests/test_unit.py
   ```
   These test the individual converter components.

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   - Ensure PostgreSQL is running
   - Check that connection details in config file are correct
   - Verify network connectivity to the database server

2. **Permission Errors**:
   - Ensure you have write permissions to the output directory
   - Check that database user has necessary permissions

3. **Data Type Errors**:
   - Some PostgreSQL types might need custom handling
   - Check the logs for details on conversion errors

### Logging

Logs are saved to the `logs` directory by default. Check these logs for detailed information about any issues.

## Extending the Project

### Adding a New Converter

To add a new converter for a different format:

1. Create a new file `src/converters/your_format_converter.py`
2. Implement a class that inherits from `BaseConverter`
3. Override the `convert` method
4. Add your converter to `src/converters/__init__.py`
5. Update the `get_converter` function in `main.py`

### Customizing Data Processing

You can customize how data is processed by overriding the `preprocess_data` method in your converter.

## License

[Your License Here]

## Contributors

[Your Name/Organization]

---

Feel free to contribute to this project by opening issues or submitting pull requests!
