#!/Users/jsyrjala/nitor/sok/python/bin/python
"""
Finnish Parliament API downloader.
Downloads data from the Eduskunta API and stores it in a DuckDB database.
"""

import requests
import dlt
import dlt.destinations  # For older dlt versions
from typing import Dict, List, Any, Optional, Tuple
import time
import datetime
import argparse
import sys
import asyncio
import aiohttp
import concurrent.futures

BASE_URL = "https://avoindata.eduskunta.fi/api/v1"
# Maximum supported items per page by the API
PER_PAGE = 100
# Default number of concurrent API requests (can be overridden with --concurrency)
DEFAULT_CONCURRENT_REQUESTS = 3
# Small delay between API requests to avoid overwhelming the server (in seconds)
API_DELAY = 0.2

def get_all_tables() -> List[str]:
    """Get a list of all available tables from the Eduskunta API."""
    response = requests.get(f"{BASE_URL}/tables/")
    response.raise_for_status()
    return response.json()

def get_table_row_counts() -> Dict[str, int]:
    """Get row counts for all tables using the dedicated endpoint."""
    response = requests.get(f"{BASE_URL}/tables/counts")
    response.raise_for_status()
    data = response.json()
    # Convert to a dictionary for easier lookup
    return {item["tableName"]: item["rowCount"] for item in data}

def get_table_info(table_name: str) -> Dict[str, Any]:
    """Get information for a specific table."""
    response = requests.get(f"{BASE_URL}/tables/{table_name}/rows", params={"page": 0, "perPage": 1})
    response.raise_for_status()
    data = response.json()
    return {
        "row_count": data.get("rowCount", "unknown"),
        "columns": data.get("columnNames", [])
    }
    
def fetch_page_with_retry(table_name: str, page: int, per_page: int, retries=3) -> Tuple[int, Dict]:
    """Fetch a page of data from the API with retries using regular requests."""
    url = f"{BASE_URL}/tables/{table_name}/rows"
    params = {"page": page, "perPage": per_page}
    
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            return page, data
        except Exception as e:
            if attempt == retries - 1:  # Last attempt
                raise
            
            # Exponential backoff for retries
            retry_delay = 0.5 * (2 ** attempt)
            print(f"Retrying page {page} after error: {str(e)[:60]}... (attempt {attempt+1}/{retries})")
            time.sleep(retry_delay)

@dlt.resource(name="eduskunta_table", write_disposition="replace")
def eduskunta_table(table_name: str, show_progress: bool = True, max_concurrent_requests: int = DEFAULT_CONCURRENT_REQUESTS):
    """
    dlt resource that yields all rows from a table by paginating through the results.
    Uses the maximum supported page size (100 items per page) with concurrent requests via ThreadPoolExecutor.
    """
    # Get the initial row count to calculate total pages
    print(f"Getting metadata for {table_name}... (using {max_concurrent_requests} concurrent connections)")
    
    # First, get the row count for this table
    try:
        row_counts = get_table_row_counts()
        row_count = row_counts.get(table_name, None)
        
        if row_count is not None and row_count > 0:
            total_pages = (row_count + PER_PAGE - 1) // PER_PAGE
        else:
            total_pages = None
    except Exception as e:
        print(f"Warning: Couldn't get row counts: {e}")
        total_pages = None
    
    # Get the first page (always needed)
    response = requests.get(
        f"{BASE_URL}/tables/{table_name}/rows",
        params={"page": 0, "perPage": PER_PAGE}
    )
    response.raise_for_status()
    data = response.json()
    
    # Get column names
    column_names = data.get("columnNames", [])
    
    # Process first page data
    first_page_rows = data.get("rowData", [])
    for row in first_page_rows:
        yield dict(zip(column_names, row))
    
    # Update total pages if needed
    if total_pages is None and "rowCount" in data and data["rowCount"] > 0:
        total_pages = (data["rowCount"] + PER_PAGE - 1) // PER_PAGE
    
    # Show initial progress
    if show_progress and total_pages:
        progress_percent = 1 / total_pages
        bar_width = 20
        filled_width = int(bar_width * progress_percent)
        bar = '█' * filled_width + '░' * (bar_width - filled_width)
        print(f"Downloaded page 1/{total_pages} of {table_name} [{bar}] {progress_percent:.1%}")
    else:
        print(f"Downloaded page 1/{total_pages} of {table_name}")
    
    # If there's only one page or no more pages, we're done
    has_more = data.get("hasMore", False)
    if not has_more or total_pages <= 1:
        print(f"Downloaded 1 page from {table_name}")
        return
    
    # Start time tracking for ETA calculation
    start_time = time.time()
    
    # Use ThreadPoolExecutor for concurrent requests
    # This is simpler and more reliable than asyncio for this use case
    processed_pages = {0: first_page_rows}  # Already processed page 0
    page_times = []  # For ETA calculation
    
    # Use ThreadPoolExecutor to download remaining pages
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
        # Submit all page requests (from page 1 onwards)
        future_to_page = {
            executor.submit(fetch_page_with_retry, table_name, page, PER_PAGE): page
            for page in range(1, total_pages)
        }
        
        # Process as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_page):
            page = future_to_page[future]
            
            try:
                # Get the result
                page_num, page_data = future.result()
                
                # Store the data
                if "rowData" in page_data:
                    processed_pages[page_num] = page_data["rowData"]
                
                # Record time for ETA calculation
                completion_time = time.time()
                page_times.append(completion_time - start_time)
                
                # Update completed count for progress display
                completed += 1
                
                # Calculate ETA based on average time per page and workers
                if len(page_times) > 0:
                    # Use the most recent times for better accuracy
                    recent_times = page_times[-min(5, len(page_times)):]
                    avg_time_per_page = sum(recent_times) / len(recent_times) / max_concurrent_requests
                    
                    # Calculate remaining time
                    remaining_pages = total_pages - (completed + 1)  # +1 for first page
                    est_remaining_seconds = remaining_pages * avg_time_per_page
                    
                    # Format estimated time remaining
                    if est_remaining_seconds < 60:
                        eta = f"{est_remaining_seconds:.1f}s"
                    elif est_remaining_seconds < 3600:
                        eta = f"{est_remaining_seconds/60:.1f}m"
                    else:
                        eta = f"{est_remaining_seconds/3600:.1f}h"
                    
                    # Update progress display
                    if show_progress:
                        progress_percent = (completed + 1) / total_pages  # +1 for first page
                        bar_width = 20
                        filled_width = int(bar_width * progress_percent)
                        bar = '█' * filled_width + '░' * (bar_width - filled_width)
                        print(f"Downloaded {completed+1}/{total_pages} pages of {table_name} [{bar}] {progress_percent:.1%} (ETA: {eta})")
                    else:
                        print(f"Downloaded {completed+1}/{total_pages} pages of {table_name} (ETA: {eta})")
                
            except Exception as e:
                print(f"Error processing page {page}: {e}")
    
    # Process all downloaded pages in order
    for page_num in range(1, total_pages):
        if page_num in processed_pages:
            for row in processed_pages[page_num]:
                yield dict(zip(column_names, row))
    
    # Print summary when download finishes
    total_time = time.time() - start_time
    if total_time < 60:
        time_str = f"{total_time:.1f} seconds"
    elif total_time < 3600:
        time_str = f"{total_time/60:.1f} minutes"
    else:
        time_str = f"{total_time/3600:.1f} hours"
    
    print(f"Downloaded {completed + 1} pages from {table_name} in {time_str}")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download data from Finnish Parliament API")
    parser.add_argument("--tables", nargs="+", help="Specific tables to download")
    parser.add_argument("--all", action="store_true", help="Download all available tables")
    parser.add_argument("--list-tables", action="store_true", help="List all available tables and exit")
    parser.add_argument("--show-columns", action="store_true", help="Show column names when listing tables")
    parser.add_argument("--db-file", default="eduskunta.duckdb", help="Output DuckDB filename")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar and ETA display")
    parser.add_argument("--concurrency", type=int, default=5, help="Number of concurrent API requests (default: 5)")
    return parser.parse_args()

def main():
    args = parse_args()

    print("Retrieving available tables...")
    all_tables = get_all_tables()
    print(f"Found {len(all_tables)} tables in the API")

    if args.list_tables:
        # Get row counts for all tables at once - more efficient
        print("Retrieving row counts...")
        row_counts = get_table_row_counts()
        
        print("\nAvailable tables:")
        for table in all_tables:
            try:
                # Get row count from our pre-fetched data
                row_count = row_counts.get(table, "unknown")
                
                # Get additional table info for columns
                info = get_table_info(table)
                
                # Just display the number of columns when row count is 1 or 0
                if row_count == 1 or row_count == 0:
                    print(f"{table} - {len(info['columns'])} columns")
                else:
                    print(f"{table} - {row_count} rows, {len(info['columns'])} columns")

                # Print column names with formatting if requested
                if args.show_columns and info['columns']:
                    print("   Columns:")
                    # One column per line
                    for column in info['columns']:
                        print(f"   - {column}")
                    print()  # Empty line between tables
            except Exception as e:
                print(f"{table} - Error getting info: {e}")
                print()
        return

    # Determine which tables to load
    tables_to_load = []
    if args.all:
        tables_to_load = all_tables
    elif args.tables:
        tables_to_load = args.tables
    else:
        # Default to a few example tables if nothing specified
        tables_to_load = ["SaliDBAanestys"]
        print("No tables specified. Using default table for example.")
        print("Use --tables to specify tables or --all to download all tables.")

    # Initialize the pipeline with DuckDB destination
    # Check if dlt version supports destination_options
    import inspect
    pipeline_sig = inspect.signature(dlt.pipeline)

    if "destination_options" in pipeline_sig.parameters:
        # Newer dlt version
        pipeline = dlt.pipeline(
            pipeline_name="eduskunta",
            destination="duckdb",
            dataset_name="parliament_data",
            destination_options={"file_path": args.db_file}
        )
    else:
        # Older dlt version - configure directly with duckdb destination
        pipeline = dlt.pipeline(
            pipeline_name="eduskunta",
            destination=dlt.destinations.duckdb(file_path=args.db_file),
            dataset_name="parliament_data"
        )

    # Download each table
    successful_tables = 0
    for table in tables_to_load:
        if table in all_tables:
            try:
                print(f"\nLoading table: {table}")
                load_info = pipeline.run(
                    eduskunta_table(
                        table_name=table, 
                        show_progress=not args.no_progress,
                        max_concurrent_requests=args.concurrency
                    ),
                    table_name=table.lower()
                )
                print(f"Load info: {load_info}")
                successful_tables += 1
            except Exception as e:
                print(f"Error loading {table}: {e}")
        else:
            print(f"Table {table} not found in API")

    # Print summary
    print(f"\nDownload complete. {successful_tables} of {len(tables_to_load)} tables loaded successfully.")
    print(f"Data loaded to DuckDB file: {args.db_file}")
    print(f"Tables created in schema: parliament_data")
    print("\nTo explore the data, run: python explore_data.py")

if __name__ == "__main__":
    main()
