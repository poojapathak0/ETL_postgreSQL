# Getting Started with PostgreSQL Data Format Converter

This guide will help you get started with the PostgreSQL Data Format Converter quickly.

## Quick Installation

1. Clone the repository or download the source code
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Basic Usage Example

Let's walk through a simple example of converting data from PostgreSQL to JSON format.

### Step 1: Configure your database connection

Edit the `config/default_config.yaml` file to set your PostgreSQL credentials:

```yaml
postgresql:
  host: localhost
  port: 5432
  database: your_database_name
  user: your_username
  password: your_password
```

### Step 2: Run the converter

```bash
python main.py --format json --table your_table_name --output ./output/data.json
```

This command will:
1. Connect to your PostgreSQL database
2. Fetch all data from the table `your_table_name`
3. Convert the data to JSON format
4. Save the result to `./output/data.json`

### Step 3: Check the results

Open the output file `./output/data.json` to view your converted data.

## Next Steps

- Try converting to other formats like CSV, MongoDB, or SQL
- Use a custom SQL query instead of fetching a whole table
- Explore the configuration options in the README

## Need Help?

If you run into any issues, check the log file in the `logs` directory or refer to the Troubleshooting section in the README file.
