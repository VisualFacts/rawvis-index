#!/usr/bin/env python3
"""Execute spatial bbox queries on DuckDB via PilotDB with full aggregates (COUNT/MIN/MAX/SUM/AVG/SUM_OF_SQUARES)."""

import duckdb
import csv
import re
import argparse
import sys
import time

HAS_PILOTDB = False
try:
    import pilotdb
    HAS_PILOTDB = True
except ImportError:
    pass

# Regex to parse bbox format: (lonMin..lonMax),(latMin..latMax)
BBOX_PATTERN = re.compile(
    r"^\s*\(\s*([+-]?\d+(?:\.\d+)?)\s*\.\.\s*([+-]?\d+(?:\.\d+)?)\s*\)\s*,\s*\(\s*([+-]?\d+(?:\.\d+)?)\s*\.\.\s*([+-]?\d+(?:\.\d+)?)\s*\)"
)

def get_column_count(csv_path):
    """Get the number of columns in the CSV file."""
    try:
        con = duckdb.connect(":memory:")
        result = con.execute(f"SELECT * FROM read_csv_auto('{csv_path}') LIMIT 0").fetchall()
        col_count = len(con.description) if con.description else 0
        con.close()
        return col_count if col_count > 0 else 10
    except Exception as e:
        print(f"WARNING: Could not determine column count: {e}", file=sys.stderr)
        return 10

def format_column_name(col_index, use_two_digit):
    """Format column index as 'columnN' or 'columnNN' based on dataset size."""
    if use_two_digit:
        return f"column{col_index:02d}"
    else:
        return f"column{col_index}"

def build_aggregate_query(table_ref, lon_col, lat_col, lon_min, lon_max, lat_min, lat_max, measure_cols, col_count):
    """Build SQL aggregate query with COUNT/MIN/MAX/SUM/AVG/SUM_OF_SQUARES for each measure column."""
    use_two_digit = col_count > 10
    
    formatted_measure_cols = [format_column_name(col_idx, use_two_digit) for col_idx in measure_cols]
    
    select_parts = []
    for col in formatted_measure_cols:
        alias_base = col.replace('[', '').replace(']', '')
        select_parts.append(f"count({col}) as count_{alias_base}")
        select_parts.append(f"min({col}) as min_{alias_base}")
        select_parts.append(f"max({col}) as max_{alias_base}")
        select_parts.append(f"sum({col}) as sum_{alias_base}")
        select_parts.append(f"avg({col}) as avg_{alias_base}")
        select_parts.append(f"sum({col} * {col}) as sum_of_squares_{alias_base}")
    
    select_clause = ", ".join(select_parts)
    where_clause = f"{lon_col} > {lon_min} AND {lon_col} < {lon_max} AND {lat_col} > {lat_min} AND {lat_col} < {lat_max}"
    
    sql = f"SELECT {select_clause} FROM {table_ref} WHERE {where_clause};"
    return sql


def parse_queries(queries_file):
    """Generator yielding (lon_min, lon_max, lat_min, lat_max) from taxi_new format."""
    with open(queries_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            match = BBOX_PATTERN.match(line)
            if not match:
                print(f"Warning: Could not parse query: {line}", file=sys.stderr)
                continue
            
            lon_min, lon_max, lat_min, lat_max = map(float, match.groups())
            yield lon_min, lon_max, lat_min, lat_max


def format_result_as_stats(result):
    """Format query result as {null=Stats{...}} and extract sum."""
    if result is None or (hasattr(result, 'empty') and result.empty):
        return '{}', ''
    
    # Handle pandas DataFrame
    if hasattr(result, 'iloc'):
        result = tuple(result.iloc[0])
    
    # result is a tuple with: count, min, max, sum, avg, sum_of_squares for each measure column
    # We process the first measure column
    count = result[0]
    min_val = result[1]
    max_val = result[2]
    sum_val = result[3]
    avg_val = result[4]
    sum_of_squares = result[5]
    
    # Calculate population standard deviation: sqrt(sum_of_squares/count - (sum/count)^2)
    if count > 0:
        mean = sum_val / count if sum_val is not None else 0
        variance = (sum_of_squares / count) - (mean ** 2) if sum_of_squares is not None else 0
        std_dev = variance ** 0.5 if variance >= 0 else 0
    else:
        std_dev = 0
        mean = 0
    
    stats_str = f"{{null=Stats{{count={int(count)}, mean={mean}, populationStandardDeviation={std_dev}, min={min_val}, max={max_val}}}}}"
    return stats_str, sum_val


def run_queries_with_pilotdb(queries_file, csv_path, lon_col, lat_col, measure_cols, error_bound, failure_prob, output_file, explain=False):
    """Execute queries via PilotDB for approximate results with full aggregates."""
    if not HAS_PILOTDB:
        print("ERROR: PilotDB not available. Install pilotdb.", file=sys.stderr)
        return
    
    import os
    
    col_count = get_column_count(csv_path)
    use_two_digit = col_count > 10
    
    # Create DuckDB database from CSV
    db_path = ".query_table.duckdb"
    table_name = "query_table"
    
    # Remove existing database if present
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create table in DuckDB
    con = duckdb.connect(db_path)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
    con.close()
    
    lon_col_formatted = format_column_name(int(lon_col), use_two_digit)
    lat_col_formatted = format_column_name(int(lat_col), use_two_digit)
    
    # Connect via PilotDB to the DuckDB database
    db_config = {"dbms": "duckdb", "path": db_path}
    pilot_con = pilotdb.connect("duckdb", db_config)
    
    fieldnames = ['csv', 'errorBound', 'initMode', 'initCatBudget (Gb)', 'initCatBudget (nodes)', 'binCount', 'i', 'query', 'indexUtil', 'Tree Node Count', 'Leaf tiles', 'Overlapped tiles', 'Fully Contained Tiles', 'Expanded nodes', 'I/Os', 'Time (sec)', 'Query Result', 'Query Result Sum']
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, (lon_min, lon_max, lat_min, lat_max) in enumerate(parse_queries(queries_file)):
            sql = build_aggregate_query(
                table_name,
                lon_col_formatted, lat_col_formatted,
                lon_min, lon_max, lat_min, lat_max,
                measure_cols, col_count
            )
            
            if explain:
                print(f"[Query {i}] {sql}")
            
            try:
                start = time.time()
                result = pilotdb.run(pilot_con, query=sql, error=error_bound, probability=failure_prob)
                elapsed = time.time() - start
                
                result_str, result_sum = format_result_as_stats(result)
                
                writer.writerow({
                    'csv': csv_path,
                    'errorBound': error_bound,
                    'initMode': '',
                    'initCatBudget (Gb)': '',
                    'initCatBudget (nodes)': '',
                    'binCount': '',
                    'i': i,
                    'query': sql.replace('SELECT ', 'SELECT ').rstrip(';'),
                    'indexUtil': '',
                    'Tree Node Count': '',
                    'Leaf tiles': '',
                    'Overlapped tiles': '',
                    'Fully Contained Tiles': '',
                    'Expanded nodes': '',
                    'I/Os': '',
                    'Time (sec)': elapsed,
                    'Query Result': result_str,
                    'Query Result Sum': result_sum
                })
            except Exception as e:
                print(f"ERROR in query {i}: {e}", file=sys.stderr)
                writer.writerow({
                    'csv': csv_path,
                    'errorBound': error_bound,
                    'initMode': '',
                    'initCatBudget (Gb)': '',
                    'initCatBudget (nodes)': '',
                    'binCount': '',
                    'i': i,
                    'query': sql.replace('SELECT ', 'SELECT ').rstrip(';'),
                    'indexUtil': '',
                    'Tree Node Count': '',
                    'Leaf tiles': '',
                    'Overlapped tiles': '',
                    'Fully Contained Tiles': '',
                    'Expanded nodes': '',
                    'I/Os': '',
                    'Time (sec)': '',
                    'Query Result': '',
                    'Query Result Sum': ''
                })
    
    pilotdb.close(pilot_con)
    print(f"Results written to {output_file}")
    
    # Clean up database
    if os.path.exists(db_path):
        os.remove(db_path)


def run_queries_with_duckdb(queries_file, csv_path, lon_col, lat_col, measure_cols, output_file, explain=False):
    """Execute queries directly via DuckDB for exact results with full aggregates."""
    con = duckdb.connect(":memory:")
    
    col_count = get_column_count(csv_path)
    use_two_digit = col_count > 10
    
    # Create table from CSV
    table_name = "query_table"
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
    
    lon_col_formatted = format_column_name(int(lon_col), use_two_digit)
    lat_col_formatted = format_column_name(int(lat_col), use_two_digit)
    
    fieldnames = ['csv', 'errorBound', 'initMode', 'initCatBudget (Gb)', 'initCatBudget (nodes)', 'binCount', 'i', 'query', 'indexUtil', 'Tree Node Count', 'Leaf tiles', 'Overlapped tiles', 'Fully Contained Tiles', 'Expanded nodes', 'I/Os', 'Time (sec)', 'Query Result', 'Query Result Sum']
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, (lon_min, lon_max, lat_min, lat_max) in enumerate(parse_queries(queries_file)):
            sql = build_aggregate_query(
                table_name,
                lon_col_formatted, lat_col_formatted,
                lon_min, lon_max, lat_min, lat_max,
                measure_cols, col_count
            )
            
            if explain:
                print(f"[Query {i}] {sql}")
            
            try:
                start = time.time()
                result = con.execute(sql).fetchone()
                elapsed = time.time() - start
                
                result_str, result_sum = format_result_as_stats(result)
                
                writer.writerow({
                    'csv': csv_path,
                    'errorBound': 0,
                    'initMode': '',
                    'initCatBudget (Gb)': '',
                    'initCatBudget (nodes)': '',
                    'binCount': '',
                    'i': i,
                    'query': sql.replace('SELECT ', 'SELECT ').rstrip(';'),
                    'indexUtil': '',
                    'Tree Node Count': '',
                    'Leaf tiles': '',
                    'Overlapped tiles': '',
                    'Fully Contained Tiles': '',
                    'Expanded nodes': '',
                    'I/Os': '',
                    'Time (sec)': elapsed,
                    'Query Result': result_str,
                    'Query Result Sum': result_sum
                })
            except Exception as e:
                print(f"ERROR in query {i}: {e}", file=sys.stderr)
                writer.writerow({
                    'csv': csv_path,
                    'errorBound': 0,
                    'initMode': '',
                    'initCatBudget (Gb)': '',
                    'initCatBudget (nodes)': '',
                    'binCount': '',
                    'i': i,
                    'query': sql.replace('SELECT ', 'SELECT ').rstrip(';'),
                    'indexUtil': '',
                    'Tree Node Count': '',
                    'Leaf tiles': '',
                    'Overlapped tiles': '',
                    'Fully Contained Tiles': '',
                    'Expanded nodes': '',
                    'I/Os': '',
                    'Time (sec)': '',
                    'Query Result': '',
                    'Query Result Sum': ''
                })
    
    con.close()
    print(f"Results written to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Execute spatial bbox queries with full aggregates (DuckDB/PilotDB)")
    parser.add_argument('--queries-file', required=True, help='File with taxi_new format queries')
    parser.add_argument('--csv', required=True, help='Path to CSV file')
    parser.add_argument('--lon-col', required=True, type=int, help='Longitude column INDEX (0-based)')
    parser.add_argument('--lat-col', required=True, type=int, help='Latitude column INDEX (0-based)')
    parser.add_argument('--measure-cols', nargs='+', type=int, default=[], help='Measure column INDEXes (0-based)')
    parser.add_argument('--error', type=float, help='Error bound for PilotDB (activates approximate mode)')
    parser.add_argument('--probability', type=float, default=0.05, help='Failure probability for PilotDB (default 0.05)')
    parser.add_argument('--out', required=True, help='Output CSV file')
    parser.add_argument('--explain', action='store_true', help='Print generated SQL')
    
    args = parser.parse_args()
    
    if args.error is not None:
        run_queries_with_pilotdb(args.queries_file, args.csv, args.lon_col, args.lat_col, args.measure_cols, args.error, args.probability, args.out, args.explain)
    else:
        run_queries_with_duckdb(args.queries_file, args.csv, args.lon_col, args.lat_col, args.measure_cols, args.out, args.explain)

if __name__ == '__main__':
    main()
