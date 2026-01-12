"""
Simple test script for PilotDB with DuckDB
This script:
1. Creates a DuckDB database from a CSV file
2. Runs approximate queries using PilotDB
3. Compares results with exact queries
"""

import duckdb
import time
import os
import pilotdb
# Configuration
CSV_FILE = "sample_data.csv"
DB_PATH = "test_sample.duckdb"
TABLE_NAME = "products"

def setup_database():
    """Create DuckDB database and load CSV data"""
    print("🔧 Setting up DuckDB database...")
    
    # Remove existing database if present
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to DuckDB
    conn = duckdb.connect(DB_PATH)
    
    # Load CSV into a table
    conn.execute(f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT * FROM read_csv_auto('{CSV_FILE}')
    """)
    
    # Show table info
    result = conn.execute(f"DESCRIBE {TABLE_NAME}").fetchall()
    print(f"✅ Table '{TABLE_NAME}' created with schema:")
    for row in result:
        print(f"   {row}")
    
    # Show row count
    count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"✅ Loaded {count} rows from {CSV_FILE}\n")
    
    conn.close()

def test_duckdb_queries():
    """Test basic DuckDB queries"""
    print("=" * 60)
    print("Testing DuckDB Queries (Baseline)")
    print("=" * 60)
    print()
    
    db_config = DB_PATH
    conn = duckdb.connect(db_config)
    
    # Query 1: Simple aggregate
    query1 = f"SELECT COUNT(*) as total_products FROM {TABLE_NAME}"
    print(f"📊 Query 1: Count all products")
    print(f"   Query: {query1}")
    start = time.time()
    result1 = conn.execute(query1).fetchall()
    elapsed1 = time.time() - start
    print(f"   Result: {result1}")
    print(f"   Time: {elapsed1:.4f}s\n")
    
    # Query 2: Sum with filter
    query2 = f"SELECT SUM(price * quantity) as total_revenue FROM {TABLE_NAME} WHERE category = 'Electronics'"
    print(f"📊 Query 2: Total revenue from Electronics")
    print(f"   Query: {query2}")
    start = time.time()
    result2 = conn.execute(query2).fetchall()
    elapsed2 = time.time() - start
    print(f"   Result: {result2}")
    print(f"   Time: {elapsed2:.4f}s\n")
    
    # Query 3: Group by aggregate
    query3 = f"SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM {TABLE_NAME} GROUP BY category ORDER BY category"
    print(f"📊 Query 3: Count and average price by category")
    print(f"   Query: {query3}")
    start = time.time()
    result3 = conn.execute(query3).fetchall()
    elapsed3 = time.time() - start
    print(f"   Result:")
    for row in result3:
        print(f"      {row}")
    print(f"   Time: {elapsed3:.4f}s\n")
    
    conn.close()
    return result1, result2, result3, elapsed1, elapsed2, elapsed3

def test_pilotdb_queries():
    """Test PilotDB approximate queries"""
    print("=" * 60)
    print("Testing PilotDB Approximate Queries")
    print("=" * 60)
    print()
        
    db_config = {
        "dbms": "duckdb",
        "path": DB_PATH
    }
    
    # Query 1: Simple aggregate
    query1 = f"SELECT COUNT(*) as total_products FROM {TABLE_NAME}"
    print(f"🎯 Query 1 (AQP): Count all products")
    print(f"   Query: {query1}")
    print(f"   Error threshold: 5%, Failure probability: 5%")
    try:
        conn = pilotdb.connect("duckdb", db_config)
        start = time.time()
        result1 = pilotdb.run(
            conn,
            query=query1,
            error=0.05,
            probability=0.05
        )
        elapsed1 = time.time() - start
        pilotdb.close(conn)
        print(f"   Result: {result1}")
        print(f"   Time: {elapsed1:.4f}s\n")
    except Exception as e:
        print(f"   Error: {e}\n")
        result1, elapsed1 = None, None
    
    # Query 2: Sum with filter
    query2 = f"SELECT SUM(price * quantity) as total_revenue FROM {TABLE_NAME} WHERE category = 'Electronics'"
    print(f"🎯 Query 2 (AQP): Total revenue from Electronics")
    print(f"   Query: {query2}")
    print(f"   Error threshold: 5%, Failure probability: 5%")
    try:
        conn = pilotdb.connect("duckdb", db_config)
        start = time.time()
        result2 = pilotdb.run(
            conn,
            query=query2,
            error=0.05,
            probability=0.05
        )
        elapsed2 = time.time() - start
        pilotdb.close(conn)
        print(f"   Result: {result2}")
        print(f"   Time: {elapsed2:.4f}s\n")
    except Exception as e:
        print(f"   Error: {e}\n")
        result2, elapsed2 = None, None
    
    # Query 3: Group by aggregate
    query3 = f"SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM {TABLE_NAME} GROUP BY category ORDER BY category"
    print(f"🎯 Query 3 (AQP): Count and average price by category")
    print(f"   Query: {query3}")
    print(f"   Error threshold: 10%, Failure probability: 10%")
    try:
        conn = pilotdb.connect("duckdb", db_config)
        start = time.time()
        result3 = pilotdb.run(
            conn,
            query=query3,
            error=0.1,
            probability=0.1
        )
        elapsed3 = time.time() - start
        pilotdb.close(conn)
        print(f"   Result: {result3}")
        print(f"   Time: {elapsed3:.4f}s\n")
    except Exception as e:
        print(f"   Error: {e}\n")
        result3, elapsed3 = None, None
    
    return result1, result2, result3, elapsed1, elapsed2, elapsed3

def main():
    print("=" * 60)
    print("PilotDB Test with DuckDB and CSV Data")
    print("=" * 60)
    print()
    
    # Step 1: Setup database
    setup_database()
    
    # Step 2: Test DuckDB queries
    duckdb_results = test_duckdb_queries()
    
    # Step 3: Test PilotDB queries
    pilotdb_results = test_pilotdb_queries()
    
    # Step 4: Comparison summary
    if all(r is not None for r in pilotdb_results[3:]):
        print("=" * 60)
        print("PERFORMANCE SUMMARY")
        print("=" * 60)
        print()
        print(f"Query 1 (Count all):")
        print(f"  DuckDB:     {duckdb_results[3]:.4f}s")
        print(f"  PilotDB:    {pilotdb_results[3]:.4f}s")
        if pilotdb_results[3] > 0:
            speedup = duckdb_results[3] / pilotdb_results[3]
            print(f"  Speedup:    {speedup:.2f}x")
        print()
        
        print(f"Query 2 (Sum with filter):")
        print(f"  DuckDB:     {duckdb_results[4]:.4f}s")
        print(f"  PilotDB:    {pilotdb_results[4]:.4f}s")
        if pilotdb_results[4] > 0:
            speedup = duckdb_results[4] / pilotdb_results[4]
            print(f"  Speedup:    {speedup:.2f}x")
        print()
        
        print(f"Query 3 (Group by):")
        print(f"  DuckDB:     {duckdb_results[5]:.4f}s")
        print(f"  PilotDB:    {pilotdb_results[5]:.4f}s")
        if pilotdb_results[5] > 0:
            speedup = duckdb_results[5] / pilotdb_results[5]
            print(f"  Speedup:    {speedup:.2f}x")
        print()
    
    print("=" * 60)
    print("✅ Test completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
