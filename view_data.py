#!/Users/jsyrjala/nitor/sok/python/bin/python
"""
Simple script to view the downloaded data from the Finnish Parliament API.
"""

import duckdb
import sys

def main():
    db_file = sys.argv[1] if len(sys.argv) > 1 else "eduskunta.duckdb"
    
    # Connect to the database
    conn = duckdb.connect(db_file)
    
    # List all tables
    tables = conn.execute("""
        SELECT table_name, table_schema 
        FROM information_schema.tables 
        WHERE table_schema != 'information_schema'
    """).fetchall()
    
    print(f"Found {len(tables)} tables in the database:")
    for i, (table, schema) in enumerate(tables, 1):
        count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        print(f"{i}. {schema}.{table} ({count} rows)")
    
    # For each table, show the first 5 rows
    for table, schema in tables:
        if table.startswith("_dlt"):
            continue  # Skip internal dlt tables
        
        print(f"\n=== {schema}.{table} ===")
        print("Schema:")
        schema_info = conn.execute(f"DESCRIBE {schema}.{table}").fetchall()
        for col in schema_info:
            print(f"- {col[0]}: {col[1]}")
        
        print("\nSample data:")
        rows = conn.execute(f"SELECT * FROM {schema}.{table} LIMIT 5").fetchall()
        headers = [col[0] for col in conn.description]
        print(" | ".join(headers))
        print("-" * 80)
        for row in rows:
            print(" | ".join(str(val) for val in row))

if __name__ == "__main__":
    main()