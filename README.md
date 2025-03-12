# Eduskunta Data Downloader

This tool downloads data from the Finnish Parliament (Eduskunta) Open Data API and stores it in a DuckDB database. It features concurrent downloads with rate limiting to be respectful of the API service.

## Data Source

The data comes from the official Finnish Parliament Open Data service:
- Main website: [https://avoindata.eduskunta.fi/](https://avoindata.eduskunta.fi/)
- API documentation: [https://avoindata.eduskunta.fi/swagger/apidocs.html](https://avoindata.eduskunta.fi/swagger/apidocs.html)
- API base URL: [https://avoindata.eduskunta.fi/api/v1](https://avoindata.eduskunta.fi/api/v1)
- Data license: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

This service provides comprehensive parliamentary data including:
- Voting records
- Parliamentary documents
- Members of Parliament information
- Committee information
- Parliamentary sessions and proceedings
- Legislative proposals and amendments

The data is regularly updated by the Finnish Parliament and is provided as a public service to promote transparency and democratic participation.

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Run the script with parameters:
```
python main.py --tables SaliDBAanestys
```
or see help:
```
python main.py
```

## Command-line Options

The tool provides several command-line options:

```
python main.py --help
```

Options:
- `--list-tables`: List all available tables with their columns and exit
- `--tables TABLE1 TABLE2 ...`: Download specific tables
- `--all`: Download all available tables
- `--db-file FILENAME`: Specify output DuckDB filename (default: eduskunta.duckdb)
- `--concurrency N`: Set the number of concurrent API requests (default: 5)
- `--rate-limit N.N`: Set API rate limit in requests per second (default: 5.0)
- `--limit N`: Limit the number of rows to download per table
- `--no-progress`: Disable progress bar and ETA display
- `--no-color`: Disable colors and emojis in output

Examples:
```bash
# List all available tables with their columns
python main.py --list-tables

# Download specific tables
python main.py --tables SaliDBAanestys VaskiData

# Download all tables (important tables are downloaded first)
python main.py --all

# Download to a specific database file
python main.py --tables SaliDBAanestys --db-file parliament_votes.duckdb

# Optimize download speed with more connections but respect API rate limits
python main.py --tables SaliDBAanestys --concurrency 10 --rate-limit 8.0

# Download only the first 100 rows from a table (useful for testing)
python main.py --tables SaliDBAanestys --limit 100
```

## Data Exploration and Export

### Exploring Data

The repository includes a simple data exploration tool:

```
python explore_data.py [db_file]
```

This interactive tool allows you to:
1. View table schemas
2. See sample data
3. Run custom SQL queries

### Exporting Data

You can export tables or query results to CSV, Excel, or JSON formats using the export tool:

```
python export_data.py --table TABLE_NAME [options]
```

Options:
- `--list`: List all available tables in the database
- `--table TABLE_NAME`: Table to export
- `--format {csv,excel,json,parquet}`: Output format (default: csv)
- `--output-dir DIRECTORY`: Directory to save exported files (default: current directory)
- `--limit N`: Maximum number of rows to export
- `--where "CONDITION"`: Filter condition (SQL WHERE clause)
- `--query "SQL"`: Custom SQL query to export (overrides --table)
- `--db-file FILE`: Path to DuckDB database file (default: eduskunta.duckdb)
- `--compression {snappy,gzip,brotli,zstd,none}`: Compression algorithm for Parquet files (default: snappy)

Examples:
```bash
# List available tables
python export_data.py --list

# Export a table to CSV
python export_data.py --table SaliDBAanestys --limit 100

# Export filtered data to Excel
python export_data.py --table SaliDBAanestys --where "IstuntoId = 123" --format excel

# Export custom query results to JSON
python export_data.py --query "SELECT * FROM parliament_data.salidbaanestyspaikat WHERE AanestysId = 1000" --format json

# Export data to Parquet format with different compression
python export_data.py --table SaliDBAanestys --format parquet --compression gzip
```

## Available Tables

### Priority Tables
The following tables are considered most important and are downloaded first when using the `--all` option:

- SaliDBAanestys - Voting records
- SaliDBAanestysPaikat - Voting positions/seats
- VaskiData - Parliamentary documents
- HETiedot - Member of Parliament information
- HEAsiat - Parliamentary matters/issues
- HEIstunto - Parliamentary session data
- ToimenpiteenVastuutaho - Actions and responsible parties

### Other Tables
The API provides many additional tables. Use `--list-tables` to see all available tables with their row counts.

## API Documentation

The complete API documentation is available in the `eduskunta-openapi.yml` file.

## Querying the Data Programmatically

You can query the downloaded data using DuckDB:

```python
import duckdb

# Connect to the database
conn = duckdb.connect("eduskunta.duckdb")

# Query example
result = conn.execute("SELECT * FROM parliament_data.salidbaanestyspaikat LIMIT 10").fetchall()
print(result)
```