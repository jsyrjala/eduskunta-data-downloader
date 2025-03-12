# CLAUDE.md - Project Guide

## Build & Test Commands
- Install dependencies: `pip install -r requirements.txt`
- Run data pipeline: `python main.py`
- List available tables: `python main.py --list-tables`
- Download specific tables: `python main.py --tables TABLE1 TABLE2`
- Download all tables: `python main.py --all`
- Explore downloaded data: `python explore_data.py`
- Run tests (when added): `pytest`
- Run single test (when added): `pytest tests/test_file.py::test_function`
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