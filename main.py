#!/Users/jsyrjala/nitor/sok/python/bin/python
"""
Finnish Parliament API downloader.
Downloads data from the Eduskunta API and stores it in a DuckDB database.
"""

import requests
import dlt
import dlt.destinations  # For older dlt versions
import duckdb
from typing import Dict, List, Any, Optional, Tuple
import time
import datetime
import argparse
import sys
import asyncio
import aiohttp
import concurrent.futures
import threading

BASE_URL = "https://avoindata.eduskunta.fi/api/v1"
# Maximum supported items per page by the API
PER_PAGE = 100
# Default number of concurrent API requests (can be overridden with --concurrency)
DEFAULT_CONCURRENT_REQUESTS = 3
# Default rate limit in requests per second (can be overridden with --rate-limit)
DEFAULT_RATE_LIMIT = 5.0

# ANSI Colors and formatting
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    BLUE = "\033[34m"
    GREEN = "\033[32m"
    CYAN = "\033[36m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_BLUE = "\033[94m"
    GREY = "\033[90m"
    
# Emoji set
class Emoji:
    DOWNLOAD = "📥"
    CHECK = "✅"
    WARNING = "⚠️"
    ERROR = "❌"
    STAR = "⭐"
    TIME = "⏱️"
    DATABASE = "🗄️"
    STATS = "📊"
    PAGES = "📄"
    ROWS = "📋"
    PROGRESS = "🔄"
    INFO = "ℹ️"
    PRIORITY = "🔑"
    SPEED = "⚡"
    NETWORK = "🌐"
# Token bucket for rate limiting
class RateLimiter:
    def __init__(self, rate_limit: float):
        """Initialize rate limiter with tokens per second"""
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.last_update = time.time()
        self.lock = threading.Lock()
        
    def acquire(self):
        """Acquire a token. Blocks if no tokens are available."""
        with self.lock:
            # Refill the bucket based on time passed
            now = time.time()
            time_passed = now - self.last_update
            self.tokens = min(self.rate_limit, self.tokens + time_passed * self.rate_limit)
            self.last_update = now
            
            # If we don't have a full token, we need to wait
            if self.tokens < 1.0:
                sleep_time = (1.0 - self.tokens) / self.rate_limit
                time.sleep(sleep_time)
                self.tokens = 0.0
                self.last_update = time.time()
            else:
                # Consume one token
                self.tokens -= 1.0

# Global rate limiter instance, will be initialized in main()
rate_limiter = None

# Global flag for colored output, will be initialized in main()
use_colors = True

def format_text(text, color=None, emoji=None, bold=False):
    """
    Format text with color and emoji if enabled.
    If colors are disabled, returns plain text without ANSI codes and emojis.
    """
    if not use_colors:
        return text
    
    result = ""
    if emoji:
        result += f"{emoji} "
    
    if color or bold:
        styles = []
        if color:
            styles.append(color)
        if bold:
            styles.append(Colors.BOLD)
        
        result += f"{''.join(styles)}{text}{Colors.RESET}"
    else:
        result += text
        
    return result

def get_all_tables() -> List[str]:
    """Get a list of all available tables from the Eduskunta API."""
    if rate_limiter:
        rate_limiter.acquire()
    response = requests.get(f"{BASE_URL}/tables/")
    response.raise_for_status()
    return response.json()

def get_table_row_counts() -> Dict[str, int]:
    """Get row counts for all tables using the dedicated endpoint."""
    if rate_limiter:
        rate_limiter.acquire()
    response = requests.get(f"{BASE_URL}/tables/counts")
    response.raise_for_status()
    data = response.json()
    # Convert to a dictionary for easier lookup
    return {item["tableName"]: item["rowCount"] for item in data}

def get_table_info(table_name: str) -> Dict[str, Any]:
    """Get information for a specific table."""
    if rate_limiter:
        rate_limiter.acquire()
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
            # Acquire a token from the rate limiter before making the request
            if rate_limiter:
                rate_limiter.acquire()
                
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

# Global dictionary to track page counts for each table
table_page_counts = {}

@dlt.resource(name="eduskunta_table", write_disposition="replace")
def eduskunta_table(table_name: str, show_progress: bool = True, max_concurrent_requests: int = DEFAULT_CONCURRENT_REQUESTS):
    """
    dlt resource that yields all rows from a table by paginating through the results.
    Uses the maximum supported page size (100 items per page) with concurrent requests via ThreadPoolExecutor.
    """
    # Get the initial row count to calculate total pages
    table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
    conn_text = format_text(str(max_concurrent_requests), Colors.BRIGHT_CYAN, Emoji.NETWORK, bold=True)
    
    print(f"Getting metadata for {table_text}... (using {conn_text} concurrent connections)")
    
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
    
    # Get the first page (always needed) with rate limiting
    if rate_limiter:
        rate_limiter.acquire()
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
    
    # Show initial progress with inline updating
    if show_progress and total_pages:
        progress_percent = 1 / total_pages
        bar_width = 20
        filled_width = int(bar_width * progress_percent)
        bar = '█' * filled_width + '░' * (bar_width - filled_width)
        
        count_text = format_text(f"1/{total_pages}", Colors.BRIGHT_CYAN, bold=True)
        table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
        bar_text = format_text(f"[{bar}]", Colors.YELLOW)
        percent_text = format_text(f"{progress_percent:.1%}", Colors.BRIGHT_GREEN, bold=True)
        download_icon = format_text("", Emoji.DOWNLOAD)
        
        print(f"\r{download_icon}Downloaded {count_text} pages of {table_text} {bar_text} {percent_text}\033[K", end='', flush=True)
    else:
        count_text = format_text(f"1/{total_pages}", Colors.BRIGHT_CYAN, bold=True)
        table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
        download_icon = format_text("", Emoji.DOWNLOAD)
        
        print(f"\r{download_icon}Downloaded {count_text} pages of {table_text}\033[K", end='', flush=True)
    
    # If there's only one page or no more pages, we're done
    has_more = data.get("hasMore", False)
    if not has_more or total_pages <= 1:
        # Print a newline to move to the next line after progress display
        print()
        
        # Format completion message
        table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
        check_icon = format_text("", Emoji.CHECK)
        
        print(f"{check_icon}Downloaded 1 page from {table_text}")
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
                    
                    # Update progress display using a single line with ANSI escape codes
                    if show_progress:
                        progress_percent = (completed + 1) / total_pages  # +1 for first page
                        bar_width = 20
                        filled_width = int(bar_width * progress_percent)
                        bar = '█' * filled_width + '░' * (bar_width - filled_width)
                        
                        # Use carriage return to move cursor to start of line and overwrite previous output
                        # Use \033[K to clear to the end of line
                        
                        # Determine color based on progress
                        if progress_percent < 0.3:
                            color = Colors.YELLOW
                        elif progress_percent < 0.7:
                            color = Colors.CYAN
                        else:
                            color = Colors.GREEN
                            
                        count_text = format_text(f"{completed+1}/{total_pages}", Colors.BRIGHT_CYAN, bold=True)
                        table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
                        bar_text = format_text(f"[{bar}]", color)
                        percent_text = format_text(f"{progress_percent:.1%}", Colors.BRIGHT_GREEN, bold=True)
                        eta_text = format_text(f"ETA: {eta}", Colors.GREY, Emoji.TIME)
                        
                        progress_msg = f"Downloaded {count_text} pages of {table_text} {bar_text} {percent_text} {eta_text}\033[K"
                        print(f"\r{progress_msg}", end='', flush=True)
                    else:
                        count_text = format_text(f"{completed+1}/{total_pages}", Colors.BRIGHT_CYAN, bold=True)
                        table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
                        eta_text = format_text(f"ETA: {eta}", Colors.GREY, Emoji.TIME)
                        
                        progress_msg = f"Downloaded {count_text} pages of {table_text} {eta_text}\033[K"
                        print(f"\r{progress_msg}", end='', flush=True)
                
            except Exception as e:
                print(f"Error processing page {page}: {e}")
    
    # Process all downloaded pages in order
    for page_num in range(1, total_pages):
        if page_num in processed_pages:
            for row in processed_pages[page_num]:
                yield dict(zip(column_names, row))
    
    # Print a newline to move to the next line after progress display
    print()
    
    # Print summary when download finishes
    total_time = time.time() - start_time
    if total_time < 60:
        time_str = f"{total_time:.1f} seconds"
    elif total_time < 3600:
        time_str = f"{total_time/60:.1f} minutes"
    else:
        time_str = f"{total_time/3600:.1f} hours"
    
    # Total pages includes the first page (page 0) plus all other pages
    total_pages = completed + 1
    
    # Update global page count tracker
    table_page_counts[table_name] = total_pages
    
    # Format completion message
    pages_text = format_text(f"{total_pages}", Colors.BRIGHT_CYAN, bold=True)
    table_text = format_text(table_name, Colors.BRIGHT_YELLOW, bold=True)
    time_text = format_text(time_str, Colors.BRIGHT_GREEN, Emoji.TIME, bold=True)
    check_icon = format_text("", Emoji.CHECK)
    
    print(f"{check_icon}Downloaded {pages_text} pages from {table_text} in {time_text}")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download data from Finnish Parliament API")
    parser.add_argument("--tables", nargs="+", help="Specific tables to download")
    parser.add_argument("--all", action="store_true", help="Download all available tables")
    parser.add_argument("--list-tables", action="store_true", help="List all available tables and exit")
    parser.add_argument("--show-columns", action="store_true", help="Show column names when listing tables")
    parser.add_argument("--db-file", default="eduskunta.duckdb", help="Output DuckDB filename")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar and ETA display")
    parser.add_argument("--no-color", action="store_true", help="Disable colors and emojis in output")
    parser.add_argument("--concurrency", type=int, default=5, help="Number of concurrent API requests (default: 5)")
    parser.add_argument("--rate-limit", type=float, default=DEFAULT_RATE_LIMIT, 
                        help=f"API rate limit in requests per second (default: {DEFAULT_RATE_LIMIT})")
    return parser, parser.parse_args()

def main():
    parser, args = parse_args()
    
    # Check if no action was specified
    if not (args.tables or args.all or args.list_tables):
        parser.print_help()
        print("\nNo action specified. Please use --tables, --all, or --list-tables.")
        return
    
    # Initialize the global rate limiter with the provided rate limit
    global rate_limiter, use_colors
    rate_limiter = RateLimiter(args.rate_limit)
    
    # Set the global color flag based on command-line option
    use_colors = not args.no_color
    
    # Start time for total download timer
    start_time = time.time()
    
    # Dictionary to store table download summaries
    table_summaries = {}

    print(format_text("Retrieving available tables...", Colors.CYAN, Emoji.INFO))
    all_tables = get_all_tables()
    count_text = format_text(str(len(all_tables)), Colors.BRIGHT_CYAN, bold=True)
    print(format_text(f"Found {count_text} tables in the API", Colors.GREEN, Emoji.CHECK))

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

    # Define a list of important tables to prioritize
    important_tables = [
        "SaliDBAanestys",       # Voting records
        "SaliDBAanestysPaikat", # Voting positions/seats
        "VaskiData",            # Parliamentary documents
        "HETiedot",             # Member of Parliament information
        "HEAsiat",              # Parliamentary matters/issues
        "HEIstunto",            # Parliamentary session data
        "ToimenpiteenVastuutaho" # Actions and responsible parties
    ]
    
    # Determine which tables to load
    tables_to_load = []
    if args.all:
        # When downloading all tables, prioritize important ones first
        prioritized_tables = []
        
        # First add all important tables that exist in the API
        for table in important_tables:
            if table in all_tables:
                prioritized_tables.append(table)
        
        # Then add all other tables
        for table in all_tables:
            if table not in prioritized_tables:
                prioritized_tables.append(table)
        
        tables_to_load = prioritized_tables
    elif args.tables:
        tables_to_load = args.tables
    else:
        # We shouldn't reach here because we already checked at the beginning of main()
        # But just in case, handle it gracefully
        print("No tables specified. Please use --tables or --all to specify which tables to download.")
        return

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
    
    # Show message about priority tables if downloading all
    if args.all:
        prioritized_count = sum(1 for table in important_tables if table in all_tables)
        count_text = format_text(str(prioritized_count), Colors.BRIGHT_MAGENTA, Emoji.PRIORITY, bold=True)
        print(f"\n{format_text('Prioritizing download of', Colors.CYAN)} {count_text} {format_text('important tables first, followed by remaining tables', Colors.CYAN)}")
    
    for table in tables_to_load:
        if table in all_tables:
            try:
                # Add indicator and colors if this is a priority table
                if table in important_tables:
                    priority_text = format_text("PRIORITY", Colors.BRIGHT_MAGENTA, Emoji.PRIORITY, bold=True)
                    table_text = format_text(table, Colors.BRIGHT_YELLOW, bold=True)
                    print(f"\nLoading {priority_text} table: {table_text}")
                else:
                    table_text = format_text(table, Colors.BRIGHT_YELLOW, bold=True)
                    print(f"\nLoading table: {table_text}")
                
                # Measure time for this table
                table_start_time = time.time()
                
                load_info = pipeline.run(
                    eduskunta_table(
                        table_name=table, 
                        show_progress=not args.no_progress,
                        max_concurrent_requests=args.concurrency
                    ),
                    table_name=table.lower()
                )
                
                # Calculate table download time
                table_time = time.time() - table_start_time
                if table_time < 60:
                    table_time_str = f"{table_time:.1f} seconds"
                elif table_time < 3600:
                    table_time_str = f"{table_time/60:.1f} minutes"
                else:
                    table_time_str = f"{table_time/3600:.1f} hours"
                
                # Get row count from API and from DuckDB for verification
                try:
                    # Get expected row count from API
                    api_row_count = None
                    try:
                        row_counts = get_table_row_counts()
                        if table in row_counts:
                            api_row_count = row_counts[table]
                    except Exception:
                        # Try alternate method if the first failed
                        try:
                            if rate_limiter:
                                rate_limiter.acquire()
                            response = requests.get(f"{BASE_URL}/tables/{table}/rows", params={"page": 0, "perPage": 1})
                            data = response.json()
                            if "rowCount" in data:
                                api_row_count = data["rowCount"]
                        except Exception:
                            pass
                    
                    # Get actual row count from database
                    conn = duckdb.connect(args.db_file)
                    result = conn.execute(f"SELECT COUNT(*) FROM parliament_data.{table.lower()}").fetchone()
                    db_row_count = result[0] if result else 0
                    conn.close()
                    
                    # Store both counts and check for discrepancies
                    if api_row_count is not None and db_row_count != api_row_count:
                        verification_status = f"WARNING: API reported {api_row_count} rows, DuckDB contains {db_row_count} rows"
                    else:
                        verification_status = "OK"
                        
                    row_count = db_row_count
                except Exception as e:
                    row_count = "unknown"
                    verification_status = f"ERROR: {str(e)[:50]}..."
                    
                # Get page count from our global tracker
                pages = table_page_counts.get(table, "?")
                
                # Store summary info
                table_summaries[table] = {
                    'rows': row_count,
                    'pages': pages,
                    'time': table_time_str,
                    'verification': verification_status,
                    'priority': table in important_tables
                }
                
                print(f"Load info: {load_info}")
                successful_tables += 1
            except Exception as e:
                print(f"Error loading {table}: {e}")
        else:
            print(f"Table {table} not found in API")

    # Calculate and format total download time
    end_time = time.time()
    total_download_time = end_time - start_time
    if total_download_time < 60:
        time_str = f"{total_download_time:.1f} seconds"
    elif total_download_time < 3600:
        time_str = f"{total_download_time/60:.1f} minutes"
    else:
        time_str = f"{total_download_time/3600:.1f} hours"
    
    # Count total rows downloaded
    total_rows = 0
    try:
        if successful_tables > 0:
            conn = duckdb.connect(args.db_file)
            for table in tables_to_load:
                if table in all_tables:
                    try:
                        result = conn.execute(f"SELECT COUNT(*) FROM parliament_data.{table.lower()}").fetchone()
                        if result:
                            total_rows += result[0]
                    except Exception:
                        # Table might not exist if download failed
                        pass
            conn.close()
    except Exception as e:
        print(f"Warning: Couldn't count rows in database: {e}")

    # Print detailed summary with colors and emojis
    divider = format_text("="*60, Colors.GREY)
    print("\n" + divider)
    print(format_text("DOWNLOAD SUMMARY", Colors.BRIGHT_MAGENTA, Emoji.STATS, bold=True))
    print(divider)
    
    # Format all summary items
    time_text = format_text(time_str, Colors.BRIGHT_GREEN, Emoji.TIME, bold=True)
    processed_text = format_text(str(len(tables_to_load)), Colors.BRIGHT_CYAN, bold=True)
    success_text = format_text(str(successful_tables), Colors.BRIGHT_GREEN, Emoji.CHECK, bold=True)
    rows_text = format_text(f"{total_rows:,}", Colors.BRIGHT_CYAN, Emoji.ROWS, bold=True)
    
    print(f"Total time: {time_text}")
    print(f"Tables processed: {processed_text}")
    print(f"Tables loaded successfully: {success_text}")
    print(f"Total rows downloaded: {rows_text}")
    
    # Calculate download rates
    if total_download_time > 0:
        rows_per_second = total_rows / total_download_time
        speed_text = format_text(f"{rows_per_second:.1f} rows/second", Colors.BRIGHT_CYAN, Emoji.SPEED, bold=True)
        print(f"Download rate: {speed_text}")
    
    # Add verification summary
    if successful_tables > 0 and table_summaries:
        verification_issues = sum(1 for summary in table_summaries.values() 
                                if summary.get('verification', '') != "OK" and summary.get('verification', ''))
        if verification_issues == 0:
            verification_text = format_text("All tables verified (API row counts match DuckDB counts)", Colors.BRIGHT_GREEN, Emoji.CHECK, bold=True)
            print(f"Data verification: {verification_text}")
        else:
            issues_text = format_text(str(verification_issues), Colors.BRIGHT_RED, bold=True)
            verification_text = format_text("tables with row count discrepancies", Colors.BRIGHT_RED, Emoji.WARNING)
            print(f"Data verification: {issues_text} {verification_text}")
    
    # Database and connection settings
    db_text = format_text(args.db_file, Colors.BRIGHT_YELLOW, Emoji.DATABASE, bold=True)
    conn_text = format_text(f"{args.concurrency} connections", Colors.BRIGHT_CYAN, Emoji.NETWORK, bold=True)
    rate_text = format_text(f"{args.rate_limit} requests/second", Colors.BRIGHT_CYAN, Emoji.SPEED, bold=True)
    
    print(f"Database file: {db_text}")
    print(f"Concurrency level: {conn_text}")
    print(f"Rate limit: {rate_text}")
    print(divider)
    
    # Print individual table summaries if there were successful downloads
    if successful_tables > 0 and table_summaries:
        # Count priority tables downloaded
        priority_tables_downloaded = sum(1 for summary in table_summaries.values() if summary.get('priority', False))
        
        details_text = format_text("TABLE DETAILS", Colors.BRIGHT_MAGENTA, bold=True)
        priority_count = format_text(str(priority_tables_downloaded), Colors.BRIGHT_MAGENTA, Emoji.PRIORITY, bold=True)
        priority_info = format_text("priority tables", Colors.CYAN)
        
        print(f"\n{details_text} ({priority_count} {priority_info}):")
        for table, summary in table_summaries.items():
            rows = summary.get('rows', 'unknown')
            pages = summary.get('pages', 'unknown')
            time_taken = summary.get('time', 'unknown')
            verification = summary.get('verification', '')
            
            # Get priority status
            is_priority = summary.get('priority', False)
            priority_indicator = format_text("", Emoji.PRIORITY) if is_priority else ""
            
            # Format table name and statistics
            table_text = format_text(table, Colors.BRIGHT_YELLOW, bold=True)
            rows_text = format_text(f"{rows:,} rows", Colors.BRIGHT_CYAN, Emoji.ROWS)
            pages_text = format_text(f"{pages} pages", Colors.CYAN, Emoji.PAGES)
            time_text = format_text(f"({time_taken})", Colors.GREY, Emoji.TIME)
            
            # Add verification and priority information
            if verification and verification != "OK":
                verification_text = format_text(f"VERIFICATION: {verification}", Colors.BRIGHT_RED, Emoji.WARNING)
                print(f"- {priority_indicator}{table_text}: {rows_text} in {pages_text} {time_text} - {verification_text}")
            else:
                check_icon = format_text("", Emoji.CHECK)
                print(f"- {priority_indicator}{table_text}: {rows_text} in {pages_text} {time_text} {check_icon}")
    
    # Print final message
    explore_cmd = format_text("python explore_data.py", Colors.BRIGHT_CYAN, bold=True)
    print(f"\n{format_text('To explore the data, run:', Colors.GREEN, Emoji.INFO)} {explore_cmd}")

if __name__ == "__main__":
    main()
