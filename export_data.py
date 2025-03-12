#!/usr/bin/env python
"""
Export tool for Eduskunta data.
Allows exporting tables from the DuckDB database to CSV, Excel, JSON, or Parquet formats.
"""

import os
import sys
import argparse
import duckdb
import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def list_tables(conn):
    """List all available tables in the database."""
    try:
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='parliament_data'
        """).fetchall()
    except Exception:
        # Try default schema if parliament_data schema doesn't exist
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='main'
        """).fetchall()
    
    return [t[0] for t in tables]

def get_schema_for_table(conn, table_name):
    """Get the schema for a table."""
    try:
        schema = conn.execute(f"DESCRIBE parliament_data.{table_name}").fetchall()
        return "parliament_data", schema
    except Exception:
        try:
            schema = conn.execute(f"DESCRIBE main.{table_name}").fetchall()
            return "main", schema
        except Exception as e:
            print(f"Error: {e}")
            return None, None

def get_query_df(conn, query):
    """Execute a query and return the results as a pandas DataFrame."""
    try:
        df = conn.execute(query).df()
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def export_to_csv(df, output_path):
    """Export a DataFrame to CSV."""
    try:
        df.to_csv(output_path, index=False)
        print(f"Data exported to CSV: {output_path}")
        return True
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return False

def export_to_excel(df, output_path):
    """Export a DataFrame to Excel.
    
    Converts timezone-aware datetime columns to timezone-naive 
    since Excel doesn't support timezone information.
    """
    try:
        # Create a copy of the dataframe to avoid modifying the original
        df_excel = df.copy()
        
        # Define a function to safely convert datetime values
        def convert_datetime(x):
            if pd.isna(x):
                return None
            try:
                # Use a more robust approach to handle timezone conversion
                if hasattr(x, 'tzinfo') and x.tzinfo is not None:
                    return x.replace(tzinfo=None)
                return x
            except:
                # If conversion fails, return the original value
                return x
        
        # Convert all datetime columns
        for col in df_excel.columns:
            # Check if column contains datetime values
            if pd.api.types.is_datetime64_any_dtype(df_excel[col]):
                # Apply conversion function to each value
                df_excel[col] = df_excel[col].apply(convert_datetime)
        
        # Export to Excel
        df_excel.to_excel(output_path, index=False)
        print(f"Data exported to Excel: {output_path}")
        print("Note: Timezone information has been removed from datetime columns for Excel compatibility.")
        return True
    except Exception as e:
        print(f"Error exporting to Excel: {e}")
        
        # Try an alternative approach if the first method fails
        try:
            print("Trying alternative export method...")
            # Convert the entire DataFrame to strings to remove timezone info
            df_str = df.astype(str)
            df_str.to_excel(output_path, index=False)
            print(f"Data exported to Excel: {output_path}")
            print("Note: All data has been converted to text format for Excel compatibility.")
            return True
        except Exception as e2:
            print(f"Alternative method also failed: {e2}")
            return False

def export_to_json(df, output_path, orient="records"):
    """Export a DataFrame to JSON."""
    try:
        # Convert DataFrame to JSON
        json_data = df.to_json(orient=orient)
        
        # Pretty-print JSON with indentation
        parsed_data = json.loads(json_data)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=2)
        
        print(f"Data exported to JSON: {output_path}")
        return True
    except Exception as e:
        print(f"Error exporting to JSON: {e}")
        return False

def export_to_parquet(df, output_path, compression="snappy"):
    """
    Export a DataFrame to Parquet format.
    
    Args:
        df: Pandas DataFrame to export
        output_path: Path to save the Parquet file
        compression: Compression algorithm (snappy, gzip, brotli, zstd, or None)
    """
    try:
        # Convert pandas DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with specified compression
        pq.write_table(table, output_path, compression=compression)
        
        print(f"Data exported to Parquet: {output_path}")
        print(f"Compression: {compression}")
        return True
    except Exception as e:
        print(f"Error exporting to Parquet: {e}")
        
        # Try with different compression if the first one fails
        if compression != "snappy" and compression is not None:
            try:
                print(f"Trying with snappy compression instead...")
                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_path, compression="snappy")
                print(f"Data exported to Parquet: {output_path}")
                print(f"Compression: snappy (fallback)")
                return True
            except Exception as e2:
                print(f"Alternative compression also failed: {e2}")
                
        return False

def export_data(db_file, table_name=None, output_format="csv", output_dir=".",
                query=None, limit=None, where=None, compression="snappy"):
    """
    Export data from DuckDB to specified format.
    
    Args:
        db_file: Path to DuckDB database file
        table_name: Table to export (ignored if query is provided)
        output_format: csv, excel, or json
        output_dir: Directory to save exported files
        query: Custom SQL query to export (overrides table_name)
        limit: Maximum number of rows to export
        where: WHERE clause to filter data (ignored if query is provided)
    """
    try:
        # Connect to the database
        conn = duckdb.connect(db_file)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # If no table name and no query, list tables and exit
        if table_name is None and query is None:
            tables = list_tables(conn)
            print(f"Available tables ({len(tables)}):")
            for i, table in enumerate(tables, 1):
                try:
                    schema_name, _ = get_schema_for_table(conn, table)
                    count = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table}").fetchone()[0]
                    print(f"{i}. {table} ({count:,} rows)")
                except Exception:
                    print(f"{i}. {table} (error getting row count)")
            return
        
        # Prepare query
        if query is None:
            # Find the schema for the table
            schema_name, schema = get_schema_for_table(conn, table_name)
            if schema is None:
                print(f"Table '{table_name}' not found in database")
                return
            
            # Build query based on table name
            query = f"SELECT * FROM {schema_name}.{table_name}"
            
            if where:
                query += f" WHERE {where}"
                
            if limit:
                query += f" LIMIT {limit}"
        
        # Execute query and get DataFrame
        df = get_query_df(conn, query)
        if df is None or len(df) == 0:
            print("No data returned from query or table")
            return
        
        # Determine output filename
        if table_name:
            base_filename = table_name
        else:
            base_filename = "custom_query"
        
        # Export based on format
        if output_format.lower() == "csv":
            output_path = os.path.join(output_dir, f"{base_filename}.csv")
            export_to_csv(df, output_path)
        
        elif output_format.lower() == "excel":
            output_path = os.path.join(output_dir, f"{base_filename}.xlsx")
            export_to_excel(df, output_path)
        
        elif output_format.lower() == "json":
            output_path = os.path.join(output_dir, f"{base_filename}.json")
            export_to_json(df, output_path)
            
        elif output_format.lower() == "parquet":
            output_path = os.path.join(output_dir, f"{base_filename}.parquet")
            # Handle "none" as None for compression
            comp = None if compression.lower() == "none" else compression.lower()
            export_to_parquet(df, output_path, compression=comp)
        
        else:
            print(f"Unsupported output format: {output_format}")
            return
        
        # Print summary
        print(f"\nExport summary:")
        print(f"- Rows exported: {len(df):,}")
        print(f"- Columns: {len(df.columns)}")
        print(f"- Format: {output_format}")
        print(f"- File size: {os.path.getsize(output_path):,} bytes")
        
    except Exception as e:
        print(f"Error: {e}")
    
def main():
    """Parse command line arguments and export data."""
    parser = argparse.ArgumentParser(description="Export Eduskunta data to CSV, Excel, or JSON")
    
    parser.add_argument("--db-file", default="eduskunta.duckdb",
                      help="Path to DuckDB database file (default: eduskunta.duckdb)")
    
    parser.add_argument("--table", 
                      help="Table to export (use --list to see available tables)")
    
    parser.add_argument("--list", action="store_true",
                      help="List available tables and exit")
    
    parser.add_argument("--format", choices=["csv", "excel", "json", "parquet"], default="csv",
                      help="Output format (default: csv)")
    
    parser.add_argument("--output-dir", default=".",
                      help="Directory to save exported files (default: current directory)")
    
    parser.add_argument("--query",
                      help="Custom SQL query to export (overrides --table)")
    
    parser.add_argument("--limit", type=int,
                      help="Maximum number of rows to export")
    
    parser.add_argument("--where",
                      help="WHERE clause to filter data (ignored if --query is provided)")
                      
    parser.add_argument("--compression", choices=["snappy", "gzip", "brotli", "zstd", "none"], default="snappy",
                      help="Compression for Parquet files (default: snappy)")
    
    args = parser.parse_args()
    
    # List tables if requested
    if args.list:
        export_data(args.db_file)
        return
    
    # Export data
    export_data(
        db_file=args.db_file,
        table_name=args.table,
        output_format=args.format,
        output_dir=args.output_dir,
        query=args.query,
        limit=args.limit,
        where=args.where,
        compression=args.compression
    )

if __name__ == "__main__":
    main()