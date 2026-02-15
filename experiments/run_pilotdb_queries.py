#!/usr/bin/env python3
"""Execute bbox aggregate queries via PilotDB approximate execution.

Reads dataset configuration from experiment_scenarios.yaml (same source of truth
as the Java experiments) and runs spatial range queries over a CSV-backed DuckDB table.

Usage examples:
    # Scenario-based (preferred): all config derived from YAML
    python run_pilotdb_queries.py --scenario synth10_pan --queries-file queries.txt \
        --num-measures 4 --error 0.05 --out results.csv

    # Explicit overrides (standalone use)
    python run_pilotdb_queries.py --csv data.csv --x-col 0 --y-col 1 \
        --measure-cols 2 3 4 --queries-file queries.txt --error 0.05 --out results.csv
"""

import argparse
import csv
import os
import re
import sys
import time

import duckdb
import pilotdb

try:
    import yaml
except ImportError:
    yaml = None


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_scenario_config(config_file, scenario_name, num_measures=None):
    """Load dataset config for a scenario from experiment_scenarios.yaml.

    Returns dict with keys: csv, xColumn, yColumn, measureCols, delimiter, hasHeader, nullstr.
    """
    if yaml is None:
        print("ERROR: PyYAML is required for --scenario mode. Install with: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    scenarios = config.get('scenarios', {})
    if scenario_name not in scenarios:
        print(f"ERROR: scenario '{scenario_name}' not found in {config_file}", file=sys.stderr)
        sys.exit(1)

    dataset_name = scenarios[scenario_name]['dataset']
    datasets = config.get('datasets', {})
    if dataset_name not in datasets:
        print(f"ERROR: dataset '{dataset_name}' not found in {config_file}", file=sys.stderr)
        sys.exit(1)

    ds = datasets[dataset_name]
    measure_cols = list(ds['measureCols'])
    if num_measures is not None:
        measure_cols = measure_cols[:num_measures]

    return {
        'csv': ds['csv'],
        'xColumn': ds['xColumn'],
        'yColumn': ds['yColumn'],
        'measureCols': measure_cols,
        'delimiter': ds.get('delimiter', ','),
        'hasHeader': ds.get('hasHeader', False),
        'nullstr': ds.get('nullstr', None),
        'validationFilters': ds.get('validationFilters', []),
    }


# ---------------------------------------------------------------------------
# Query parsing
# ---------------------------------------------------------------------------

# (xMin..xMax),(yMin..yMax)  — parentheses only
_BBOX_PARENS = re.compile(
    r"^\s*\(\s*([+-]?\d+(?:\.\d+)?)\s*\.\.\s*([+-]?\d+(?:\.\d+)?)\s*\)"
    r"\s*,\s*"
    r"\(\s*([+-]?\d+(?:\.\d+)?)\s*\.\.\s*([+-]?\d+(?:\.\d+)?)\s*\)"
)

# [xMin..xMax),[yMin..yMax]  — any bracket style
_BBOX_GENERIC = re.compile(
    r"^\s*[\(\[]\s*([+-]?\d+(?:\.\d+)?)\s*\.\.\s*([+-]?\d+(?:\.\d+)?)\s*[\)\]]"
    r"\s*,\s*"
    r"[\(\[]\s*([+-]?\d+(?:\.\d+)?)\s*\.\.\s*([+-]?\d+(?:\.\d+)?)\s*[\)\]]"
)


def parse_queries(queries_file):
    """Yield (x_min, x_max, y_min, y_max) from a query file.

    Supports plain bbox lines and Java-serialized Query lines
    (rect|filters|groupBy|measures|opType — only the rect part is used).
    """
    with open(queries_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Java serialized format: take everything before '|'
            bbox_text = line.split('|', 1)[0]

            m = _BBOX_PARENS.match(bbox_text) or _BBOX_GENERIC.match(bbox_text)
            if not m:
                print(f"Warning: Could not parse query: {line}", file=sys.stderr)
                continue

            x_min, x_max, y_min, y_max = map(float, m.groups())
            yield x_min, x_max, y_min, y_max


# ---------------------------------------------------------------------------
# CSV read options (must match Java DuckDBQueryExecutor.buildReadCsvOptions)
# ---------------------------------------------------------------------------

def _build_read_csv_options(nullstr=None, delimiter=None, has_header=None):
    """Build the options string for DuckDB's read_csv_auto, matching the Java side.

    The Java DuckDBQueryExecutor always sets ignore_errors=true and adds
    nullstr when the dataset config provides one (e.g. SDSS uses '\\N').
    """
    parts = ["ignore_errors = true"]
    if nullstr:
        parts.append(f"nullstr = '{nullstr}'")
    if delimiter:
        # DuckDB expects the delimiter as a string literal
        parts.append(f"delim = '{delimiter}'")
    if has_header is not None:
        parts.append(f"header = {'true' if has_header else 'false'}")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Column naming helpers
# ---------------------------------------------------------------------------

def _get_column_count(csv_path, csv_options="ignore_errors = true"):
    """Return the number of columns in a CSV via DuckDB introspection."""
    try:
        con = duckdb.connect(":memory:")
        con.execute(f"SELECT * FROM read_csv_auto('{csv_path}', {csv_options}) LIMIT 0").fetchall()
        count = len(con.description) if con.description else 10
        con.close()
        return count
    except Exception as e:
        print(f"WARNING: Could not determine column count: {e}", file=sys.stderr)
        return 10


def _col_name(col_index, col_count):
    """Format a 0-based column index as DuckDB's auto-generated name (column0 or column00)."""
    if col_count > 10:
        return f"column{col_index:02d}"
    return f"column{col_index}"


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

# Negate operator map: validation filters define *invalid* conditions,
# so we negate them to keep only valid rows (same as Java side).
_NEGATE_OP = {
    '<': '>=',
    '>': '<=',
    '<=': '>',
    '>=': '<',
}

_VFILTER_RE = re.compile(r'^(\d+)\s*([<>]=?)\s*([+-]?\d+(?:\.\d+)?)$')


def _parse_validation_filters(filter_strings, col_count):
    """Parse validation filter strings (e.g. '12<0') into SQL WHERE conditions.

    Each filter defines an *invalid* condition. We negate the operator so the
    SQL keeps only valid rows, matching the Java DataValidationFilter behaviour.
    Returns a list of SQL condition strings, e.g. ['column12 >= 0.0'].
    """
    conditions = []
    for fs in filter_strings:
        m = _VFILTER_RE.match(fs.strip())
        if not m:
            print(f"WARNING: Could not parse validation filter: {fs!r}", file=sys.stderr)
            continue
        col_idx = int(m.group(1))
        op = m.group(2)
        value = float(m.group(3))
        neg_op = _NEGATE_OP.get(op)
        if neg_op is None:
            print(f"WARNING: Unknown operator in validation filter: {fs!r}", file=sys.stderr)
            continue
        col_name = _col_name(col_idx, col_count)
        conditions.append(f"{col_name} {neg_op} {value}")
    return conditions


def _build_query(table, x_col, y_col, x_min, x_max, y_min, y_max, measure_cols, col_count,
                 validation_filters=None):
    """Build an aggregate SQL query over a bbox range for the given measure columns.

    Aggregates per measure column: SUM, AVG.
    Only linear aggregates that PilotDB can guarantee error bounds for.
    validation_filters: list of filter strings from YAML (e.g. ['12<0', '12>400']).
    """
    cols = [_col_name(idx, col_count) for idx in measure_cols]

    select_parts = []
    for c in cols:
        cast_c = f"CAST({c} AS FLOAT)"
        select_parts += [
            f"sum({cast_c}) as sum_{c}",
            f"avg({cast_c}) as avg_{c}",
        ]

    # Cast x/y columns and bounds to FLOAT to match Java DuckDB queries
    where = (f"CAST({x_col} AS FLOAT) > CAST({x_min} AS FLOAT) "
             f"AND CAST({x_col} AS FLOAT) < CAST({x_max} AS FLOAT) "
             f"AND CAST({y_col} AS FLOAT) > CAST({y_min} AS FLOAT) "
             f"AND CAST({y_col} AS FLOAT) < CAST({y_max} AS FLOAT)")

    if validation_filters:
        vf_conditions = _parse_validation_filters(validation_filters, col_count)
        if vf_conditions:
            where += " AND " + " AND ".join(vf_conditions)

    return f"SELECT {', '.join(select_parts)} FROM {table} WHERE {where};"


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

def _format_stats(result, measure_cols):
    """Convert PilotDB result into per-measure Stats strings matching Java format.

    Returns (stats_str, sum_str) where both are keyed by measure column index,
    e.g. "{null={12=Stats{...}, 17=Stats{...}}}" and "{12=1.23E8, 17=4.56E8}".
    """
    if result is None:
        return '{}', ''
    # PilotDB returns a DataFrame
    if hasattr(result, 'iloc'):
        result = tuple(result.iloc[0])
    if hasattr(result, 'empty') and result.empty:
        return '{}', ''

    # Layout: 2 values per measure column (sum, avg)
    stats_parts = []
    sum_parts = []
    for j, col_idx in enumerate(measure_cols):
        offset = j * 2
        sum_val, avg_val = result[offset:offset + 2]

        stats_parts.append(
            f"{col_idx}=Stats{{sum={sum_val}, avg={avg_val}}}")
        sum_parts.append(f"{col_idx}={sum_val}")

    stats_str = "{null={" + ", ".join(stats_parts) + "}}"
    sum_str = "{" + ", ".join(sum_parts) + "}"
    return stats_str, sum_str


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

# Output CSV columns - matches DuckDB experiment format (minus version/rowCount).
_CSV_FIELDS = ['csv', 'i', 'Time (sec)', 'Query', 'errorBound', 'Query Result', 'Query Result Sum']


def _write_row(writer, csv_path, i, elapsed, sql, error_bound, result_str, result_sum):
    writer.writerow({
        'csv': csv_path,
        'i': i,
        'Time (sec)': elapsed,
        'Query': sql,
        'errorBound': error_bound,
        'Query Result': result_str,
        'Query Result Sum': result_sum,
    })


def run_queries(queries_file, csv_path, x_col, y_col, measure_cols,
                output_file, error_bound, failure_prob=0.05, explain=False,
                validation_filters=None, nullstr=None, delimiter=None,
                has_header=None):
    """Execute bbox queries via PilotDB approximate execution.

    Args:
        queries_file: Path to file with one bbox query per line.
        csv_path: Path to the dataset CSV.
        x_col: 0-based index of the X (first spatial) dimension column.
        y_col: 0-based index of the Y (second spatial) dimension column.
        measure_cols: List of 0-based column indices to aggregate over.
        output_file: Destination CSV for results.
        error_bound: PilotDB error bound (e.g. 0.05).
        failure_prob: PilotDB failure probability (default 0.05).
        explain: Print each generated SQL statement.
        validation_filters: List of filter strings (e.g. ['12<0', '12>400']) that
            define invalid rows to exclude, matching Java experiment behaviour.
        nullstr: Null string representation in the CSV (e.g. '\\N' for SDSS).
        delimiter: Column delimiter (e.g. '\\t' for tab-separated).
        has_header: Whether the CSV has a header row.
    """
    csv_options = _build_read_csv_options(nullstr=nullstr, delimiter=delimiter,
                                          has_header=has_header)
    col_count = _get_column_count(csv_path, csv_options=csv_options)
    x_col_name = _col_name(int(x_col), col_count)
    y_col_name = _col_name(int(y_col), col_count)
    table = "query_table"

    # --- Setup database (timed - added to query 0 for fair comparison) ---
    # Use in-memory DuckDB, same as the Java DuckDB experiment.
    init_start = time.time()
    db_config = {"dbms": "duckdb", "path": ":memory:"}
    pilot_con = pilotdb.connect("duckdb", db_config)
    con = pilot_con["conn"]

    # Apply the same DuckDB settings as the Java experiment runner
    memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT')
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    temp_dir = os.environ.get('DUCKDB_TEMP_DIR')
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")

    create_sql = f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_path}', {csv_options})"
    if explain:
        print(f"  CREATE: {create_sql}")
    con.execute(create_sql)
    init_elapsed = time.time() - init_start

    # --- Execute queries ---
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=_CSV_FIELDS)
        writer.writeheader()

        for i, (x_min, x_max, y_min, y_max) in enumerate(parse_queries(queries_file)):
            sql = _build_query(table, x_col_name, y_col_name,
                               x_min, x_max, y_min, y_max, measure_cols, col_count,
                               validation_filters=validation_filters)
            print(f"[Query {i}] ({x_min},{x_max}),({y_min},{y_max})")
            if explain:
                print(f"  SQL: {sql}")

            try:
                start = time.time()
                result = pilotdb.run(pilot_con, query=sql,
                                     error=error_bound, probability=failure_prob)
                elapsed = time.time() - start

                result_str, result_sum = _format_stats(result, measure_cols)
                # Include initialization time in query 0 (matches Valinor/DuckDB behavior)
                if i == 0:
                    elapsed += init_elapsed
                _write_row(writer, csv_path, i, elapsed, sql, error_bound, result_str, result_sum)

            except Exception as e:
                print(f"ERROR in query {i}: {e}", file=sys.stderr)
                _write_row(writer, csv_path, i, '', sql, error_bound, '', '')

    # --- Cleanup ---
    pilotdb.close(pilot_con)

    print(f"Results written to {output_file}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Execute bbox aggregate queries via PilotDB approximate execution.")
    parser.add_argument('--queries-file', required=True,
                        help='File with bbox queries (one per line)')
    parser.add_argument('--out', required=True, help='Output CSV file')
    parser.add_argument('--error', type=float, required=True,
                        help='Error bound for PilotDB (e.g. 0.05)')

    # Scenario-based config (preferred - reads everything from YAML)
    parser.add_argument('--scenario',
                        help='Scenario name from experiment_scenarios.yaml')
    parser.add_argument('--config-file',
                        default='src/main/resources/experiments/experiment_scenarios.yaml',
                        help='Path to experiment_scenarios.yaml')
    parser.add_argument('--num-measures', type=int,
                        help='Number of measure columns to use (first N from config)')

    # Explicit overrides (standalone use, or to override YAML values)
    parser.add_argument('--csv', help='Path to CSV file')
    parser.add_argument('--x-col', type=int, help='X column index (0-based)')
    parser.add_argument('--y-col', type=int, help='Y column index (0-based)')
    parser.add_argument('--measure-cols', nargs='+', type=int, default=None,
                        help='Measure column indices (0-based)')

    # PilotDB options
    parser.add_argument('--probability', type=float, default=0.05,
                        help='Failure probability for PilotDB (default 0.05)')
    parser.add_argument('--explain', action='store_true',
                        help='Print generated SQL for each query')

    args = parser.parse_args()

    # Resolve config: YAML scenario first, then explicit CLI overrides
    validation_filters = None
    if args.scenario:
        cfg = load_scenario_config(args.config_file, args.scenario, args.num_measures)
        csv_path = args.csv or cfg['csv']
        x_col = args.x_col if args.x_col is not None else cfg['xColumn']
        y_col = args.y_col if args.y_col is not None else cfg['yColumn']
        measure_cols = args.measure_cols if args.measure_cols is not None else cfg['measureCols']
        validation_filters = cfg.get('validationFilters', []) or None
    else:
        csv_path = args.csv
        x_col = args.x_col
        y_col = args.y_col
        measure_cols = args.measure_cols or []
        if csv_path is None or x_col is None or y_col is None:
            parser.error('Either --scenario or --csv/--x-col/--y-col must be provided.')

    # Extract CSV parsing options from config (nullstr, delimiter, hasHeader)
    nullstr = cfg.get('nullstr') if args.scenario else None
    delimiter = cfg.get('delimiter') if args.scenario else None
    has_header = cfg.get('hasHeader') if args.scenario else None

    run_queries(args.queries_file, csv_path, x_col, y_col, measure_cols,
                args.out, error_bound=args.error, failure_prob=args.probability,
                explain=args.explain, validation_filters=validation_filters,
                nullstr=nullstr, delimiter=delimiter, has_header=has_header)


if __name__ == '__main__':
    main()
