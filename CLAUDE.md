# CLAUDE.md - Project Guide

## Build & Test Commands
- Install dependencies: `pip install -r requirements.txt`
- Show help: `python main.py`
- List available tables: `python main.py --list-tables`
- Download specific tables: `python main.py --tables TABLE1 TABLE2`
- Download all tables: `python main.py --all`
- Control concurrency: `python main.py --concurrency 10`
- Set API rate limit: `python main.py --rate-limit 10.0`
- Limit rows per table: `python main.py --tables TABLE1 --limit 100`
- Disable colors/emojis: `python main.py --tables TABLE1 --no-color`
- Explore downloaded data: `python explore_data.py`
- Export data to CSV: `python export_data.py --table TABLE_NAME`
- Export data to Excel: `python export_data.py --table TABLE_NAME --format excel`
- Export data to JSON: `python export_data.py --table TABLE_NAME --format json`
- Export data to Parquet: `python export_data.py --table TABLE_NAME --format parquet`
- Choose Parquet compression: `python export_data.py --table TABLE_NAME --format parquet --compression gzip`
- List exportable tables: `python export_data.py --list`
- Export with custom query: `python export_data.py --query "SELECT * FROM parliament_data.TABLE_NAME WHERE CONDITION"`
- Run tests: `pytest`
- Run single test: `pytest tests/test_file.py::test_function`
- Lint: `flake8 .`
- Type check: `mypy .`

## Code Style Guidelines
- Use Python with requests and dlt libraries
- Requests for API calls to https://avoindata.eduskunta.fi/api/v1
- dlt for data loading/transformations to DuckDB
- Follow PEP 8 style guidelines with 4-space indentation
- Use snake_case for variables and functions
- Add type annotations for function parameters and return values
- Handle API errors with appropriate exception catching
- Use dictionary unpacking for clean parameter passing
- Paginate API requests for large datasets (perPage parameter)
- Document functions with docstrings
- Reference OpenAPI specs at https://avoindata.eduskunta.fi/swagger/apidocs.html
- Error handling: Catch and log specific exceptions
- Do not modify API response structure when storing in DuckDB
- Use rate limiting to avoid overwhelming the API
- Implement proper retry mechanism with exponential backoff