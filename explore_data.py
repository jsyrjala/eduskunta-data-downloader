#!/Users/jsyrjala/nitor/sok/python/bin/python
"""
Interactive tool to explore the downloaded Finnish Parliament data.
Allows viewing table schemas, sample data, and running custom queries.
"""

import duckdb
import sys

def explore_database(db_file="eduskunta.duckdb"):
    """
    Simple utility to explore the downloaded Eduskunta data.
    """
    try:
        # Connect to the database
        conn = duckdb.connect(db_file)
        
        # List all tables in the database
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
        
        print(f"Found {len(tables)} tables in the database:")
        for i, (table,) in enumerate(tables, 1):
            # Get row count for each table
            schema_name = "parliament_data"
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table}").fetchone()[0]
            except Exception:
                schema_name = "main"
                count = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table}").fetchone()[0]
            
            print(f"{i}. {table} ({count} rows)")
        
        # Interactive exploration if run directly
        if __name__ == "__main__":
            while True:
                print("\nOptions:")
                print("1. Show table schema")
                print("2. Show sample data")
                print("3. Run custom query")
                print("4. Exit")
                
                choice = input("\nEnter your choice (1-4): ")
                
                if choice == "1":
                    table_name = input("Enter table name: ")
                    try:
                        # Try with parliament_data schema first
                        try:
                            schema = conn.execute(f"DESCRIBE parliament_data.{table_name}").fetchall()
                            schema_name = "parliament_data"
                        except Exception:
                            schema = conn.execute(f"DESCRIBE main.{table_name}").fetchall()
                            schema_name = "main"
                        
                        print(f"\nSchema for {table_name} in {schema_name} schema:")
                        for col in schema:
                            print(f"- {col[0]}: {col[1]}")
                    except Exception as e:
                        print(f"Error: {e}")
                
                elif choice == "2":
                    table_name = input("Enter table name: ")
                    limit = input("Number of rows to show (default 5): ") or "5"
                    try:
                        # Try with parliament_data schema first
                        try:
                            rows = conn.execute(f"SELECT * FROM parliament_data.{table_name} LIMIT {limit}").fetchall()
                            schema_name = "parliament_data"
                        except Exception:
                            rows = conn.execute(f"SELECT * FROM main.{table_name} LIMIT {limit}").fetchall()
                            schema_name = "main"
                        
                        headers = [col[0] for col in conn.description]
                        print(f"\nSample data from {schema_name}.{table_name}:")
                        print(" | ".join(headers))
                        print("-" * 80)
                        for row in rows:
                            print(" | ".join(str(val) for val in row))
                    except Exception as e:
                        print(f"Error: {e}")
                
                elif choice == "3":
                    query = input("Enter SQL query: ")
                    try:
                        result = conn.execute(query).fetchall()
                        headers = [col[0] for col in conn.description]
                        print("\nResults:")
                        print(" | ".join(headers))
                        print("-" * 80)
                        for row in result[:10]:  # Limit to 10 rows
                            print(" | ".join(str(val) for val in row))
                        
                        if len(result) > 10:
                            print(f"... and {len(result) - 10} more rows")
                    except Exception as e:
                        print(f"Error: {e}")
                
                elif choice == "4":
                    break
                
                else:
                    print("Invalid choice, please try again")
        
        return conn
    
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

if __name__ == "__main__":
    db_file = sys.argv[1] if len(sys.argv) > 1 else "eduskunta.duckdb"
    explore_database(db_file)