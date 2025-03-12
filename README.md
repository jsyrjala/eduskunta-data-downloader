# Eduskunta Data Downloader

This tool downloads data from the Finnish Parliament (Eduskunta) Open Data API and stores it in a DuckDB database.

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Run the script:
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

Examples:
```bash
# List all available tables with their columns
python main.py --list-tables

# Download specific tables
python main.py --tables SaliDBAanestys VaskiData

# Download all tables
python main.py --all

# Download to a specific database file
python main.py --tables SaliDBAanestys --db-file parliament_votes.duckdb
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

Some example tables include:
- SaliDBAanestys (Votes)
- VaskiData (Documents)

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