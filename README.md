# Eduskunta Data Downloader

This tool downloads data from the Finnish Parliament (Eduskunta) Open Data API and stores it in a DuckDB database. It features concurrent downloads with rate limiting to be respectful of the API service.

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
- `--no-progress`: Disable progress bar and ETA display

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
```

## Data Exploration

The repository includes a simple data exploration tool:

```
python explore_data.py [db_file]
```

This interactive tool allows you to:
1. View table schemas
2. See sample data
3. Run custom SQL queries

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