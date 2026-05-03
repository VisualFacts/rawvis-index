#!/usr/bin/env python3
"""
Experiment results analysis script.

Discovers all result CSV files under a base directory, validates them,
and produces summary tables covering:
  - Data health report (per-file validation)
  - Scenario overview (all methods side-by-side, comprehensive metrics)
  - Error bound sweep (per scenario)
  - Measure column sweep (per scenario)
  - Selectivity sweep (synth10 only)
  - Scalability sweep (synth10 only)
  - Init cost breakdown (per scenario)
  - Valinor-A vs Valinor-S comparison

Usage:
    python experiments/analysis/analyze_results.py experiments/results/
    python experiments/analysis/analyze_results.py experiments/results/ --scenario gaia_dr3_random
    python experiments/analysis/analyze_results.py experiments/results/ -o analysis_report.txt
"""

import argparse
import csv
import io
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# =============================================================================
# Constants
# =============================================================================

# Default parameter values (used when filtering for overview table)
DEFAULT_MCOLS = 4
DEFAULT_ERROR = 0.01

# Used only for old result files that do not carry the current _n<N> suffix.
FALLBACK_EXPECTED_QUERIES: Optional[int] = None

# Method directory → canonical method name
METHOD_DIR_MAP = {
    'valinor_a': 'valinor_a',
    'valinor_s': 'valinor_s',
    'duckdb/tableProjected': 'duckdb_projected',
    'pilotdb': 'pilotdb',
}

# Display labels
METHOD_LABELS = {
    'valinor_a': 'Valinor-A',
    'valinor_s': 'Valinor-S',
    'duckdb_projected': 'DuckDB',
    'pilotdb': 'PilotDB',
}

# Method display order
METHOD_ORDER = ['valinor_a', 'valinor_s', 'duckdb_projected', 'pilotdb']

# Selectivity scenarios (ordered by selectivity)
SELECTIVITY_SCENARIOS = [
    ('synth10_300M_clustered_sel001', 0.01),
    ('synth10_300M_clustered_sel01', 0.1),
    ('synth10_300M_clustered_sel1', 1.0),
    ('synth10_300M_clustered_sel5', 5.0),
    ('synth10_300M_clustered_sel10', 10.0),
]

# Scalability scenarios (ordered by size)
SCALABILITY_SCENARIOS = [
    ('synth10_50M_clustered_sel1', '50M'),
    ('synth10_100M_clustered_sel1', '100M'),
    ('synth10_300M_clustered_sel1', '300M'),
    ('synth10_500M_clustered_sel1', '500M'),
]

# Scenario display order for overview
SCENARIO_ORDER = [
    'synth10_300M_clustered_sel1',
    'synth10_300M_random_sel1',
    'taxi_clustered',
    'taxi_random',
    'taxi_exploratory',
    'gaia_dr3_clustered',
    'gaia_dr3_random',
    'gaia_dr3_exploratory',
    'ebird_us_clustered',
    'ebird_us_random',
    'ebird_us_exploratory',
]


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class ResultFile:
    """Metadata parsed from a result CSV file path."""
    path: Path
    scenario: str
    method: str       # canonical: valinor_a, valinor_s, duckdb_projected, pilotdb
    mcols: int
    error_bound: float
    run: int
    query_count: Optional[int] = None


@dataclass
class QueryRow:
    """Parsed data from one CSV row."""
    i: int
    time_sec: float
    ios: int = 0
    leaf_tiles: int = 0
    overlapped_tiles: int = 0
    contained_with_stats: int = 0
    contained_without_stats: int = 0
    sampling_tiles: int = 0
    sampling_rate: float = 0.0
    sampling_rounds: int = 0
    sampling_status: str = ''
    is_error: bool = False
    error_msg: str = ''
    # Init timing breakdown (only for i=0)
    init_scan: float = 0.0
    init_gc: float = 0.0
    init_partition: float = 0.0
    init_wire: float = 0.0
    init_global_stats: float = 0.0
    init_total: float = 0.0
    heap_committed_mb: float = 0.0
    heap_live_mb: float = 0.0
    init_scan_path: str = ''
    init_partition_path: str = ''
    # DuckDB/PilotDB init breakdown
    init_table_creation: float = 0.0
    init_index_creation: float = 0.0
    init_first_query: float = 0.0


@dataclass
class FileSummary:
    """Aggregated metrics for one result file."""
    result_file: ResultFile
    status: str = 'OK'           # OK, ERROR_ALL, ERROR_PARTIAL, SHORT, PARSE_ERROR
    status_detail: str = ''
    total_rows: int = 0
    valid_rows: int = 0
    error_rows: int = 0
    # Init (q0)
    init_time: float = float('nan')
    init_scan: float = float('nan')
    init_gc: float = float('nan')
    init_partition: float = float('nan')
    heap_live_mb: float = float('nan')
    init_scan_path: str = ''
    init_partition_path: str = ''
    # DuckDB/PilotDB init breakdown
    init_table_creation: float = float('nan')
    init_index_creation: float = float('nan')
    init_first_query: float = float('nan')
    # Query metrics (q1+
    query_count: int = 0
    mean_time: float = float('nan')
    median_time: float = float('nan')
    min_time: float = float('nan')
    max_time: float = float('nan')
    p95_time: float = float('nan')
    total_query_time: float = float('nan')    # sum of q1..qN
    total_workload_time: float = float('nan') # q0 + sum of q1..qN
    # I/O (valinor only)
    mean_ios: float = float('nan')
    total_ios: int = 0
    # Tile utilization (valinor only, q1+)
    mean_tile_util: float = float('nan')  # contained_with_stats / leaf_tiles
    # Index size
    final_leaf_tiles: int = 0
    # Sampling
    mean_sampling_rate: float = float('nan')
    sampling_status_counts: dict[str, int] = field(default_factory=dict)
    exactification_fallback_queries: int = 0
    unconverged_queries: int = 0
    # IO cost
    mean_us_per_io: float = float('nan')


# =============================================================================
# File discovery
# =============================================================================

def discover_results(base_dir: Path) -> list[ResultFile]:
    """Walk the results directory and parse file metadata from paths."""
    results = []
    base = base_dir.resolve()

    for csv_path in sorted(base.rglob('*.csv')):
        rel = csv_path.relative_to(base)
        parts = rel.parts  # e.g., ('gaia_dr3_random', 'valinor_a', 'results_mcols4_error0.01_res500_str0_n500_run1.csv')

        if len(parts) < 2:
            continue

        scenario = parts[0]
        filename = parts[-1]

        # Determine method from directory structure
        method = None
        if len(parts) == 3 and parts[1] == 'duckdb':
            method_dir = f"duckdb/{parts[2].replace('/' + filename, '')}"
            # Actually parts would be ('scenario', 'duckdb', 'tableProjected', 'file.csv') for len=4
        if len(parts) == 4 and parts[1] == 'duckdb':
            method_dir = f"duckdb/{parts[2]}"
            method = METHOD_DIR_MAP.get(method_dir)
        elif len(parts) == 3:
            method_dir = parts[1]
            method = METHOD_DIR_MAP.get(method_dir)

        if method is None:
            continue

        # Parse filename
        rf = _parse_filename(filename, scenario, method, csv_path)
        if rf is not None:
            results.append(rf)

    return results


# Valinor: results_mcols{N}_error{E}_res{R}_str{S}_n{Q}[_outK{K}]_run{R}.csv
_RE_VALINOR = re.compile(
    r'results_mcols(\d+)_error([\d.]+)_res\d+_str[\d.]+(?:_n(\d+))?(?:_outK\d+)?_run(\d+)\.csv'
)
# DuckDB: results_mcols{N}_n{Q}_run{R}.csv
_RE_DUCKDB = re.compile(
    r'results_mcols(\d+)(?:_n(\d+))?_run(\d+)\.csv'
)
# PilotDB: results_mcols{N}_error{E}_n{Q}_run{R}.csv
_RE_PILOTDB = re.compile(
    r'results_mcols(\d+)_error([\d.]+)(?:_n(\d+))?_run(\d+)\.csv'
)


def _parse_filename(filename: str, scenario: str, method: str, path: Path) -> Optional[ResultFile]:
    """Parse mcols, error_bound, run from filename."""
    if method in ('valinor_a', 'valinor_s'):
        m = _RE_VALINOR.match(filename)
        if not m:
            return None
        return ResultFile(
            path=path, scenario=scenario, method=method,
            mcols=int(m.group(1)), error_bound=float(m.group(2)),
            run=int(m.group(4)), query_count=_optional_int(m.group(3)),
        )
    elif method == 'duckdb_projected':
        m = _RE_DUCKDB.match(filename)
        if not m:
            return None
        return ResultFile(
            path=path, scenario=scenario, method=method,
            mcols=int(m.group(1)), error_bound=0.0,
            run=int(m.group(3)), query_count=_optional_int(m.group(2)),
        )
    elif method == 'pilotdb':
        m = _RE_PILOTDB.match(filename)
        if not m:
            return None
        return ResultFile(
            path=path, scenario=scenario, method=method,
            mcols=int(m.group(1)), error_bound=float(m.group(2)),
            run=int(m.group(4)), query_count=_optional_int(m.group(3)),
        )
    return None


def _optional_int(value: str | None) -> Optional[int]:
    return int(value) if value is not None else None


# =============================================================================
# CSV parsing
# =============================================================================

def _parse_init_timing(s: str) -> dict:
    """Parse Init Timing string like '{scan=33.29, setup=1.56, ..., total=41.78}'.
    Numeric values are stored as float; non-numeric values are stored as str."""
    result = {}
    if not s or s == '':
        return result
    s = s.strip().strip('{}').strip('"')
    for pair in s.split(','):
        pair = pair.strip()
        if '=' not in pair:
            continue
        key, val = pair.split('=', 1)
        key = key.strip()
        val = val.strip()
        try:
            result[key] = float(val)
        except (ValueError, TypeError):
            result[key] = val
    return result


def _clean_csv_content(content: str) -> str:
    """Remove blank lines from CSV content (DuckDB error files have them)."""
    lines = content.split('\n')
    # Keep header (first line) always
    if not lines:
        return content

    header = lines[0]
    cleaned = [header]

    # For data lines, we need to handle multi-line error messages
    # Strategy: rejoin lines that don't start with the csv path pattern
    # A valid data row starts with a path (/) or the csv column value
    i = 1
    while i < len(lines):
        line = lines[i]
        if line.strip() == '':
            i += 1
            continue
        # Check if this looks like a new data row (starts with / for csv path)
        if line.startswith('/') or line.startswith('"ERROR') or line.startswith('ERROR'):
            cleaned.append(line)
        else:
            # Continuation of previous multi-line field — append to last line
            if cleaned:
                cleaned[-1] += ' ' + line
        i += 1

    return '\n'.join(cleaned)


def parse_csv_file(rf: ResultFile) -> list[QueryRow]:
    """Parse a result CSV file into QueryRow objects."""
    try:
        raw = rf.path.read_text(encoding='utf-8', errors='replace')
    except OSError as e:
        return []

    content = _clean_csv_content(raw)
    rows = []

    reader = csv.DictReader(io.StringIO(content))
    for record in reader:
        try:
            row = _parse_row(record, rf.method)
            if row is not None:
                rows.append(row)
        except Exception:
            # Unparseable row — create error marker
            rows.append(QueryRow(i=len(rows), time_sec=-1, is_error=True,
                                 error_msg='Parse error'))

    return rows


def _parse_row(record: dict, method: str) -> Optional[QueryRow]:
    """Parse one CSV dict record into a QueryRow."""
    try:
        i = int(record.get('i', -1))
    except (ValueError, TypeError):
        return None

    # Check for error rows (DuckDB writes -1 for time on errors)
    time_str = record.get('Time (sec)', '-1')
    try:
        time_sec = float(time_str)
    except (ValueError, TypeError):
        time_sec = -1.0

    is_error = False
    error_msg = ''

    # DuckDB error detection
    if method == 'duckdb_projected':
        qr = record.get('Query Result', '')
        if 'ERROR' in str(qr):
            is_error = True
            error_msg = str(qr)[:200]
        elif time_sec < 0:
            is_error = True
            error_msg = 'Negative time'

    # PilotDB error detection
    if method == 'pilotdb':
        qr = record.get('Query Result', '')
        if 'ERROR' in str(qr) or 'error' in str(qr):
            is_error = True
            error_msg = str(qr)[:200]
        elif time_sec < 0:
            is_error = True
            error_msg = 'Negative time'

    # Valinor fields
    ios = 0
    leaf_tiles = 0
    overlapped = 0
    contained_with = 0
    contained_without = 0
    sampling_tiles = 0
    sampling_rate = 0.0
    sampling_rounds = 0
    sampling_status = ''
    init_scan = 0.0
    init_gc = 0.0
    init_partition = 0.0
    init_wire = 0.0
    init_global_stats = 0.0
    init_total = 0.0
    heap_committed = 0.0
    heap_live = 0.0
    init_scan_path = ''
    init_partition_path = ''

    init_table_creation = 0.0
    init_index_creation = 0.0
    init_first_query = 0.0

    if method in ('valinor_a', 'valinor_s'):
        ios = _safe_int(record.get('I/Os', 0))
        leaf_tiles = _safe_int(record.get('Leaf tiles', 0))
        overlapped = _safe_int(record.get('Overlapped tiles', 0))
        contained_with = _safe_int(record.get('Fully Contained Tiles With Stats', 0))
        contained_without = _safe_int(record.get('Fully Contained Tiles Without Stats', 0))
        sampling_tiles = _safe_int(record.get('Sampling Tiles', 0))
        sampling_rate = _safe_float(record.get('Sampling Rate', 0))
        sampling_rounds = _safe_int(record.get('Sampling Rounds', 0))
        sampling_status = _normalize_sampling_status(
            record.get('Sampling Status', ''), record.get('Converged', 'true'),
        )

        # Parse init timing for i=0
        if i == 0:
            it = _parse_init_timing(record.get('Init Timing', ''))
            init_scan = it.get('scan', 0.0)
            init_gc = it.get('gc', 0.0)
            init_partition = it.get('partition', 0.0)
            init_wire = it.get('wire', 0.0)
            init_global_stats = it.get('globalStats', 0.0)
            init_total = it.get('total', 0.0)
            heap_committed = it.get('heapCommittedMB', 0.0)
            heap_live = it.get('heapLiveMB', 0.0)
            init_scan_path = it.get('scanPath', '')
            init_partition_path = it.get('partitionPath', '')

    # Parse DuckDB/PilotDB init timing for i=0
    if method in ('duckdb_projected', 'pilotdb') and i == 0:
        it = _parse_init_timing(record.get('Init Timing', ''))
        if it:
            init_table_creation = it.get('tableCreation', 0.0)
            init_index_creation = it.get('indexCreation', 0.0)
            init_first_query = it.get('q0', 0.0)

    return QueryRow(
        i=i, time_sec=time_sec, ios=ios,
        leaf_tiles=leaf_tiles, overlapped_tiles=overlapped,
        contained_with_stats=contained_with, contained_without_stats=contained_without,
        sampling_tiles=sampling_tiles, sampling_rate=sampling_rate,
        sampling_rounds=sampling_rounds, sampling_status=sampling_status,
        is_error=is_error, error_msg=error_msg,
        init_scan=init_scan, init_gc=init_gc,
        init_partition=init_partition,
        init_wire=init_wire, init_global_stats=init_global_stats,
        init_total=init_total,
        heap_committed_mb=heap_committed, heap_live_mb=heap_live,
        init_scan_path=init_scan_path, init_partition_path=init_partition_path,
        init_table_creation=init_table_creation,
        init_index_creation=init_index_creation,
        init_first_query=init_first_query,
    )


def _safe_int(v) -> int:
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


def _safe_float(v) -> float:
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _normalize_sampling_status(status_value, converged_value) -> str:
    status = str(status_value).strip().lower()
    if status and status not in ('nan', 'none'):
        return status
    converged = str(converged_value).strip().lower() != 'false'
    return 'legacy_converged' if converged else 'legacy_unconverged'


# =============================================================================
# Summarization
# =============================================================================

def compute_summary(rf: ResultFile, rows: list[QueryRow]) -> FileSummary:
    """Compute aggregated metrics for one result file."""
    s = FileSummary(result_file=rf)
    s.total_rows = len(rows)

    if not rows:
        s.status = 'EMPTY'
        s.status_detail = 'No data rows'
        return s

    error_rows = [r for r in rows if r.is_error]
    valid_rows = [r for r in rows if not r.is_error]
    s.error_rows = len(error_rows)
    s.valid_rows = len(valid_rows)

    if len(valid_rows) == 0:
        s.status = 'ERROR_ALL'
        s.status_detail = f'All {len(error_rows)} rows have errors'
        if error_rows:
            s.status_detail += f': {error_rows[0].error_msg[:100]}'
        return s

    if len(error_rows) > 0:
        s.status = 'ERROR_PARTIAL'
        s.status_detail = f'{len(error_rows)}/{s.total_rows} rows have errors'

    expected_rows = rf.query_count if rf.query_count is not None else FALLBACK_EXPECTED_QUERIES
    if expected_rows is not None and s.total_rows != expected_rows:
        if s.status == 'OK':
            s.status = 'ROW_COUNT_MISMATCH'
        if s.status_detail:
            s.status_detail += '; '
        s.status_detail += f'{s.total_rows}/{expected_rows} rows from filename'

    # Init (i=0)
    init_rows = [r for r in valid_rows if r.i == 0]
    query_rows = [r for r in valid_rows if r.i > 0]

    if rf.method in ('valinor_a', 'valinor_s'):
        sampling_statuses = Counter(r.sampling_status for r in query_rows if r.sampling_status)
        s.sampling_status_counts = dict(sampling_statuses)
        s.exactification_fallback_queries = (
            sampling_statuses.get('exactified_converged', 0)
            + sampling_statuses.get('full_sample_converged', 0)
        )
        s.unconverged_queries = (
            sampling_statuses.get('exhausted_unconverged', 0)
            + sampling_statuses.get('legacy_unconverged', 0)
        )
        if s.unconverged_queries:
            if s.status == 'OK':
                s.status = 'UNCONVERGED'
            if s.status_detail:
                s.status_detail += '; '
            s.status_detail += f'{s.unconverged_queries} Valinor rows did not converge'

    if init_rows:
        q0 = init_rows[0]
        s.init_time = q0.time_sec
        if rf.method in ('valinor_a', 'valinor_s'):
            s.init_scan = q0.init_scan
            s.init_gc = q0.init_gc
            s.init_partition = q0.init_partition
            s.heap_live_mb = q0.heap_live_mb
            s.init_scan_path = q0.init_scan_path
            s.init_partition_path = q0.init_partition_path
        if rf.method in ('duckdb_projected', 'pilotdb'):
            if q0.init_table_creation > 0:
                s.init_table_creation = q0.init_table_creation
                s.init_index_creation = q0.init_index_creation
                s.init_first_query = q0.init_first_query

    # Warm-query metrics (i > 0). Row i=0 is the cold query and carries init timing.
    s.query_count = len(query_rows)
    if query_rows:
        times = sorted([r.time_sec for r in query_rows])
        s.mean_time = sum(times) / len(times)
        s.median_time = _median(times)
        s.min_time = times[0]
        s.max_time = times[-1]
        s.p95_time = _percentile(times, 95)
        s.total_query_time = sum(times)

        if not _isnan(s.init_time):
            s.total_workload_time = s.init_time + s.total_query_time
        else:
            s.total_workload_time = s.total_query_time

        # I/O
        if rf.method in ('valinor_a', 'valinor_s'):
            io_vals = [r.ios for r in query_rows]
            s.mean_ios = sum(io_vals) / len(io_vals)
            s.total_ios = sum(io_vals)

            # Tile utilization: contained_with_stats / leaf_tiles (when leaf_tiles > 0)
            utils = []
            for r in query_rows:
                if r.leaf_tiles > 0:
                    utils.append(r.contained_with_stats / r.leaf_tiles)
            if utils:
                s.mean_tile_util = sum(utils) / len(utils)

            # Sampling rate
            srates = [r.sampling_rate for r in query_rows if r.sampling_rate > 0]
            if srates:
                s.mean_sampling_rate = sum(srates) / len(srates)

            # IO cost: µs per IO operation (query rows with IOs > 0)
            io_cost_rows = [(r.time_sec, r.ios) for r in query_rows if r.ios > 0]
            if io_cost_rows:
                total_t = sum(t for t, _ in io_cost_rows)
                total_i = sum(io for _, io in io_cost_rows)
                if total_i > 0:
                    s.mean_us_per_io = (total_t / total_i) * 1_000_000

        # Final index size
        if query_rows:
            last = query_rows[-1]
            s.final_leaf_tiles = last.leaf_tiles

    return s


def _median(sorted_vals: list[float]) -> float:
    n = len(sorted_vals)
    if n == 0:
        return float('nan')
    if n % 2 == 1:
        return sorted_vals[n // 2]
    return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2


def _percentile(sorted_vals: list[float], pct: int) -> float:
    n = len(sorted_vals)
    if n == 0:
        return float('nan')
    idx = int(n * pct / 100)
    return sorted_vals[min(idx, n - 1)]


def _isnan(v: float) -> bool:
    return v != v  # NaN != NaN


# =============================================================================
# Formatting utilities
# =============================================================================

def _fmt_time(v: float, precision: int = 3) -> str:
    """Format a time value as string, or '-' if NaN."""
    if _isnan(v):
        return '-'
    if v >= 100:
        return f'{v:.1f}'
    if v >= 10:
        return f'{v:.2f}'
    return f'{v:.{precision}f}'


def _fmt_int(v) -> str:
    if isinstance(v, float) and _isnan(v):
        return '-'
    return f'{int(v):,}'


def _fmt_pct(v: float) -> str:
    if _isnan(v):
        return '-'
    return f'{v * 100:.1f}%'


def _fmt_speedup(baseline: float, value: float) -> str:
    if _isnan(baseline) or _isnan(value) or value == 0:
        return '-'
    return f'{baseline / value:.1f}x'


def _print_table(headers: list[str], rows: list[list[str]], min_widths: Optional[list[int]] = None):
    """Print a formatted ASCII table."""
    if not rows:
        print("  (no data)")
        return

    widths = [len(h) for h in headers]
    for row in rows:
        for j, cell in enumerate(row):
            if j < len(widths):
                widths[j] = max(widths[j], len(str(cell)))
            else:
                widths.append(len(str(cell)))

    if min_widths:
        for j, mw in enumerate(min_widths):
            if j < len(widths):
                widths[j] = max(widths[j], mw)

    def _fmtrow(cells, sep='|'):
        parts = []
        for j, cell in enumerate(cells):
            w = widths[j] if j < len(widths) else len(str(cell))
            parts.append(f' {str(cell):<{w}} ')
        return sep.join(parts)

    print(_fmtrow(headers))
    print('+'.join('-' * (w + 2) for w in widths))
    for row in rows:
        # Pad row if shorter than headers
        padded = list(row) + [''] * (len(headers) - len(row))
        print(_fmtrow(padded))


# =============================================================================
# Report sections
# =============================================================================

def print_health_report(summaries: list[FileSummary], out=sys.stdout):
    """Print per-file health status."""
    _print = lambda *a, **k: print(*a, **k, file=out)

    _print("\n" + "=" * 80)
    _print("HEALTH REPORT")
    _print("=" * 80)

    # Group by status
    by_status = defaultdict(list)
    for s in summaries:
        by_status[s.status].append(s)

    _print(f"\n  Total files: {len(summaries)}")
    for status in ['OK', 'UNCONVERGED', 'ROW_COUNT_MISMATCH', 'ERROR_PARTIAL', 'ERROR_ALL', 'EMPTY', 'PARSE_ERROR']:
        count = len(by_status.get(status, []))
        if count > 0:
            _print(f"  {status}: {count}")

    sampling_status_totals = Counter()
    for s in summaries:
        sampling_status_totals.update(s.sampling_status_counts)
    visible_sampling_statuses = {
        status: count for status, count in sampling_status_totals.items()
        if not status.startswith('legacy_') and count > 0
    }
    if visible_sampling_statuses:
        _print("\n  Valinor sampling stop statuses:")
        for status, count in sorted(visible_sampling_statuses.items()):
            _print(f"    {status}: {count}")

    # Show problematic files
    problems = [s for s in summaries if s.status != 'OK']
    if problems:
        _print(f"\n  Problematic files ({len(problems)}):")
        headers = ['Scenario', 'Method', 'mcols', 'error', 'run', 'Status', 'Detail']
        rows = []
        for s in problems:
            rf = s.result_file
            rows.append([
                rf.scenario, METHOD_LABELS.get(rf.method, rf.method),
                str(rf.mcols), str(rf.error_bound), str(rf.run),
                s.status, s.status_detail[:60],
            ])
        _print("")
        _save_print = print
        __builtins_print = print

        # Temporarily redirect print to our out
        import builtins
        old_print = builtins.print
        builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
        _print_table(headers, rows)
        builtins.print = old_print
    else:
        _print("\n  All files OK!")


def _build_lookup(summaries: list[FileSummary]) -> dict:
    """Build a nested lookup: (scenario, method, mcols, error_bound) → FileSummary.
    Only includes OK/ERROR_PARTIAL files. For multi-run, averages across runs later."""
    lookup = {}
    for s in summaries:
        if s.status in ('ERROR_ALL', 'EMPTY', 'PARSE_ERROR'):
            continue
        rf = s.result_file
        key = (rf.scenario, rf.method, rf.mcols, rf.error_bound)
        if key not in lookup:
            lookup[key] = []
        lookup[key].append(s)
    return lookup


def _avg_summaries(slist: list[FileSummary]) -> FileSummary:
    """Average metrics across multiple runs of the same config."""
    if len(slist) == 1:
        return slist[0]

    # Use first as template
    avg = FileSummary(result_file=slist[0].result_file, status='OK')
    avg.total_rows = slist[0].total_rows

    def _avg_field(field_name):
        vals = [getattr(s, field_name) for s in slist if not _isnan(getattr(s, field_name))]
        if vals:
            return sum(vals) / len(vals)
        return float('nan')

    for f in ['init_time', 'init_scan', 'init_gc', 'init_partition',
              'heap_live_mb', 'mean_time', 'median_time', 'min_time', 'max_time',
              'p95_time', 'total_query_time', 'total_workload_time',
              'mean_ios', 'mean_tile_util', 'mean_sampling_rate', 'mean_us_per_io',
              'init_table_creation', 'init_index_creation', 'init_first_query']:
        setattr(avg, f, _avg_field(f))

    # Sum-based fields: average across runs too for consistency
    avg.total_ios = int(_avg_field('total_ios')) if not _isnan(_avg_field('total_ios')) else 0
    avg.final_leaf_tiles = slist[-1].final_leaf_tiles
    avg.query_count = slist[0].query_count

    return avg


def _get(lookup, scenario, method, mcols, error) -> Optional[FileSummary]:
    """Get averaged summary for a config, or None."""
    key = (scenario, method, mcols, error)
    if key in lookup:
        return _avg_summaries(lookup[key])
    return None


def print_scenario_overview(summaries: list[FileSummary], out=sys.stdout):
    """Print overview table per scenario × mcols, showing all methods × error bounds.

    Shows one sub-table per mcols value that has multi-method data (at least 2
    distinct method families), so speedup comparisons are fair.  mcols values
    with only a single method family are skipped here (covered by mcols sweep).
    """
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scenarios = []
    for s in summaries:
        if s.result_file.scenario not in scenarios:
            scenarios.append(s.result_file.scenario)
    order_map = {s: i for i, s in enumerate(SCENARIO_ORDER)}
    scenarios.sort(key=lambda s: (order_map.get(s, 999), s))

    _p("\n" + "=" * 80)
    _p("SCENARIO OVERVIEW — All Methods × Error Bounds (per mcols)")
    _p("=" * 80)
    _p(f"  One sub-table per mcols value with ≥2 method families.")
    _p(f"  Times in seconds. Speedup = DuckDB avg query / method avg query")

    for scenario in scenarios:
        # Discover all mcols values present for this scenario (from OK/partial files)
        all_mcols = sorted(set(
            s.result_file.mcols for s in summaries
            if s.result_file.scenario == scenario
            and s.status not in ('ERROR_ALL', 'EMPTY', 'PARSE_ERROR')
        ))

        if not all_mcols:
            continue

        for mcols in all_mcols:
            _p(f"\n{'─' * 80}")
            _p(f"  Scenario: {scenario}  |  mcols={mcols}")
            _p(f"{'─' * 80}")

            # Find DuckDB reference at this mcols
            ref_summary = None
            ref_summary = _get(lookup, scenario, 'duckdb_projected', mcols, 0.0)

            # Collect all (method, error_bound) rows at this mcols
            method_error_rows = []
            for method in METHOD_ORDER:
                if method == 'duckdb_projected':
                    s = _get(lookup, scenario, method, mcols, 0.0)
                    if s is not None:
                        method_error_rows.append((method, 0.0, s))
                else:
                    error_bounds = sorted(set(
                        rf.error_bound for rf in [sm.result_file for sm in summaries]
                        if rf.scenario == scenario and rf.method == method and rf.mcols == mcols
                    ))
                    for eb in error_bounds:
                        s = _get(lookup, scenario, method, mcols, eb)
                        if s is not None:
                            method_error_rows.append((method, eb, s))

            if not method_error_rows:
                continue

            headers = ['Method', 'Error', 'Init', 'Avg Query', 'Median', 'P95',
                       'Total Query', 'Total WL', 'Avg I/Os',
                       'Speedup (avg q)', 'Speedup (WL)']
            rows = []

            ref_mean = ref_summary.mean_time if ref_summary else float('nan')
            ref_wl = ref_summary.total_workload_time if ref_summary else float('nan')

            for method, eb, s in method_error_rows:
                err_label = 'exact' if eb == 0.0 else str(eb)
                rows.append([
                    METHOD_LABELS.get(method, method),
                    err_label,
                    _fmt_time(s.init_time),
                    _fmt_time(s.mean_time),
                    _fmt_time(s.median_time),
                    _fmt_time(s.p95_time),
                    _fmt_time(s.total_query_time),
                    _fmt_time(s.total_workload_time),
                    _fmt_int(s.mean_ios) if not _isnan(s.mean_ios) else '-',
                    _fmt_speedup(ref_mean, s.mean_time),
                    _fmt_speedup(ref_wl, s.total_workload_time),
                ])

            import builtins
            old_print = builtins.print
            builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
            _print_table(headers, rows)
            builtins.print = old_print


def print_error_sweep(summaries: list[FileSummary], out=sys.stdout):
    """Print error bound sweep tables per scenario."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scenarios = sorted(set(s.result_file.scenario for s in summaries))

    _p("\n" + "=" * 80)
    _p("ERROR BOUND SWEEP — Effect of relaxing error bound")
    _p("=" * 80)
    _p(f"  (fixed mcols={DEFAULT_MCOLS}, Valinor-A)")

    for scenario in scenarios:
        # Check if we have valinor_a data for this scenario
        errors = sorted(set(
            rf.error_bound for rf in [s.result_file for s in summaries]
            if rf.scenario == scenario and rf.method == 'valinor_a' and rf.mcols == DEFAULT_MCOLS
        ))
        if not errors:
            continue

        _p(f"\n  Scenario: {scenario}")
        headers = ['Error Bound', 'Avg Time', 'Median', 'Total Query', 'Total Workload',
                    'Avg I/Os', 'Speedup vs Exact']

        # Get exact baseline
        exact = _get(lookup, scenario, 'valinor_a', DEFAULT_MCOLS, 0.0)
        exact_mean = exact.mean_time if exact else float('nan')

        rows = []
        for err in errors:
            s = _get(lookup, scenario, 'valinor_a', DEFAULT_MCOLS, err)
            if s is None:
                continue
            rows.append([
                str(err) if err > 0 else '0 (exact)',
                _fmt_time(s.mean_time),
                _fmt_time(s.median_time),
                _fmt_time(s.total_query_time),
                _fmt_time(s.total_workload_time),
                _fmt_int(s.mean_ios) if not _isnan(s.mean_ios) else '-',
                _fmt_speedup(exact_mean, s.mean_time) if not _isnan(exact_mean) else '-',
            ])

        import builtins
        old_print = builtins.print
        builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
        _print_table(headers, rows)
        builtins.print = old_print


def print_mcols_sweep(summaries: list[FileSummary], out=sys.stdout):
    """Print measure columns sweep tables per scenario."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scenarios = sorted(set(s.result_file.scenario for s in summaries))

    _p("\n" + "=" * 80)
    _p("MEASURE COLUMNS SWEEP — Effect of adding measure columns")
    _p("=" * 80)
    _p(f"  (fixed error_bound={DEFAULT_ERROR}, Valinor-A)")

    for scenario in scenarios:
        mcols_list = sorted(set(
            rf.mcols for rf in [s.result_file for s in summaries]
            if rf.scenario == scenario and rf.method == 'valinor_a'
            and rf.error_bound == DEFAULT_ERROR
        ))
        if not mcols_list:
            continue

        _p(f"\n  Scenario: {scenario}")
        headers = ['Measure Cols', 'Avg Time', 'Median', 'Total Query', 'Total Workload',
                    'Init Time', 'Avg I/Os', 'Heap Live (MB)']
        rows = []
        for mc in mcols_list:
            s = _get(lookup, scenario, 'valinor_a', mc, DEFAULT_ERROR)
            if s is None:
                continue
            rows.append([
                str(mc),
                _fmt_time(s.mean_time),
                _fmt_time(s.median_time),
                _fmt_time(s.total_query_time),
                _fmt_time(s.total_workload_time),
                _fmt_time(s.init_time),
                _fmt_int(s.mean_ios) if not _isnan(s.mean_ios) else '-',
                _fmt_int(s.heap_live_mb) if not _isnan(s.heap_live_mb) else '-',
            ])

        import builtins
        old_print = builtins.print
        builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
        _print_table(headers, rows)
        builtins.print = old_print


def print_selectivity_sweep(summaries: list[FileSummary], out=sys.stdout):
    """Print selectivity sweep (synth10 scenarios)."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    # Check if any selectivity scenarios exist
    sel_scenarios = [s for s, _ in SELECTIVITY_SCENARIOS]
    available = [sc for sc in sel_scenarios
                 if any(s.result_file.scenario == sc for s in summaries)]
    if not available:
        return

    _p("\n" + "=" * 80)
    _p("SELECTIVITY SWEEP — Query response vs selectivity (synth10 300M)")
    _p("=" * 80)

    for eff_mcols in [1, 4]:
        _p(f"\n  mcols={eff_mcols}, error_bound={DEFAULT_ERROR}")
        headers = ['Selectivity', 'Valinor-A Avg', 'Valinor-A Total', 'Valinor-S Avg',
                    'DuckDB Avg', 'Speedup (V-A vs DuckDB)']
        rows = []
        for sc, sel in SELECTIVITY_SCENARIOS:
            va = _get(lookup, sc, 'valinor_a', eff_mcols, DEFAULT_ERROR)
            vs = _get(lookup, sc, 'valinor_s', eff_mcols, DEFAULT_ERROR)
            dk = _get(lookup, sc, 'duckdb_projected', eff_mcols, 0.0)

            if va is None and vs is None and dk is None:
                continue

            dk_mean = dk.mean_time if dk else float('nan')
            va_mean = va.mean_time if va else float('nan')

            rows.append([
                f'{sel}%',
                _fmt_time(va.mean_time) if va else '-',
                _fmt_time(va.total_workload_time) if va else '-',
                _fmt_time(vs.mean_time) if vs else '-',
                _fmt_time(dk.mean_time) if dk else '-',
                _fmt_speedup(dk_mean, va_mean),
            ])

        import builtins
        old_print = builtins.print
        builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
        _print_table(headers, rows)
        builtins.print = old_print


def print_scalability_sweep(summaries: list[FileSummary], out=sys.stdout):
    """Print scalability sweep (synth10 varying sizes)."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scale_scenarios = [s for s, _ in SCALABILITY_SCENARIOS]
    available = [sc for sc in scale_scenarios
                 if any(s.result_file.scenario == sc for s in summaries)]
    if not available:
        return

    _p("\n" + "=" * 80)
    _p("SCALABILITY SWEEP — Query response vs dataset size (synth10, 1% sel)")
    _p("=" * 80)

    for eff_mcols in [1, 4]:
        _p(f"\n  mcols={eff_mcols}, error_bound={DEFAULT_ERROR}")
        headers = ['Size', 'V-A Init', 'V-A Avg Query', 'V-A Total Workload',
                    'DuckDB Avg', 'DuckDB Total Workload', 'Speedup (avg)']
        rows = []
        for sc, label in SCALABILITY_SCENARIOS:
            va = _get(lookup, sc, 'valinor_a', eff_mcols, DEFAULT_ERROR)
            dk = _get(lookup, sc, 'duckdb_projected', eff_mcols, 0.0)

            if va is None and dk is None:
                continue

            dk_mean = dk.mean_time if dk else float('nan')
            va_mean = va.mean_time if va else float('nan')

            rows.append([
                label,
                _fmt_time(va.init_time) if va else '-',
                _fmt_time(va.mean_time) if va else '-',
                _fmt_time(va.total_workload_time) if va else '-',
                _fmt_time(dk.mean_time) if dk else '-',
                _fmt_time(dk.total_workload_time) if dk else '-',
                _fmt_speedup(dk_mean, va_mean),
            ])

        import builtins
        old_print = builtins.print
        builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
        _print_table(headers, rows)
        builtins.print = old_print


def print_init_breakdown(summaries: list[FileSummary], out=sys.stdout):
    """Print init cost breakdown per scenario."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scenarios = sorted(set(s.result_file.scenario for s in summaries))

    _p("\n" + "=" * 80)
    _p("INIT COST BREAKDOWN — First query (q0) timing decomposition")
    _p("=" * 80)
    _p(f"  (mcols={DEFAULT_MCOLS}, error_bound={DEFAULT_ERROR} for Valinor-A; error_bound=0 for DuckDB)")

    headers = ['Scenario', 'Method', 'Total Init', 'Scan/TblCreate', 'Partition/IdxCreate', 'GC/Q0',
               'Heap Live (MB)', 'Scan Path', 'Part Path']
    rows = []

    for scenario in scenarios:
        for method in ['valinor_a', 'valinor_s', 'duckdb_projected', 'pilotdb']:
            if method == 'duckdb_projected':
                s = _get(lookup, scenario, method, DEFAULT_MCOLS, 0.0)
            else:
                s = _get(lookup, scenario, method, DEFAULT_MCOLS, DEFAULT_ERROR)
            if s is None:
                # Try mcols=1
                if method == 'duckdb_projected':
                    s = _get(lookup, scenario, method, 1, 0.0)
                else:
                    s = _get(lookup, scenario, method, 1, DEFAULT_ERROR)
            if s is None:
                continue

            if method in ('valinor_a', 'valinor_s'):
                rows.append([
                    scenario,
                    METHOD_LABELS.get(method, method),
                    _fmt_time(s.init_time),
                    _fmt_time(s.init_scan),
                    _fmt_time(s.init_partition),
                    _fmt_time(s.init_gc),
                    _fmt_int(s.heap_live_mb) if not _isnan(s.heap_live_mb) else '-',
                    s.init_scan_path or '-',
                    s.init_partition_path or '-',
                ])
            else:
                rows.append([
                    scenario,
                    METHOD_LABELS.get(method, method),
                    _fmt_time(s.init_time),
                    _fmt_time(s.init_table_creation),
                    _fmt_time(s.init_index_creation),
                    _fmt_time(s.init_first_query),
                    '-',
                    '-',
                    '-',
                ])

    import builtins
    old_print = builtins.print
    builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
    _print_table(headers, rows)
    builtins.print = old_print


def print_valinor_comparison(summaries: list[FileSummary], out=sys.stdout):
    """Print Valinor-A vs Valinor-S comparison."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scenarios = sorted(set(s.result_file.scenario for s in summaries))

    _p("\n" + "=" * 80)
    _p("VALINOR-A vs VALINOR-S — Metadata reuse vs sampling-only")
    _p("=" * 80)
    _p(f"  (mcols={DEFAULT_MCOLS}, error_bound={DEFAULT_ERROR})")

    headers = ['Scenario', 'V-A Avg', 'V-S Avg', 'Time Speedup',
               'V-A I/Os', 'V-S I/Os', 'I/O Reduction']
    rows = []

    for scenario in scenarios:
        va = _get(lookup, scenario, 'valinor_a', DEFAULT_MCOLS, DEFAULT_ERROR)
        vs = _get(lookup, scenario, 'valinor_s', DEFAULT_MCOLS, DEFAULT_ERROR)
        if va is None and vs is None:
            continue

        va_mean = va.mean_time if va else float('nan')
        vs_mean = vs.mean_time if vs else float('nan')
        va_ios = va.mean_ios if va else float('nan')
        vs_ios = vs.mean_ios if vs else float('nan')

        io_reduction = '-'
        if not _isnan(va_ios) and not _isnan(vs_ios) and vs_ios > 0:
            io_reduction = f'{(1 - va_ios / vs_ios) * 100:.0f}%'

        rows.append([
            scenario,
            _fmt_time(va_mean),
            _fmt_time(vs_mean),
            _fmt_speedup(vs_mean, va_mean),
            _fmt_int(va_ios) if not _isnan(va_ios) else '-',
            _fmt_int(vs_ios) if not _isnan(vs_ios) else '-',
            io_reduction,
        ])

    import builtins
    old_print = builtins.print
    builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
    _print_table(headers, rows)
    builtins.print = old_print


def print_io_cost(summaries: list[FileSummary], out=sys.stdout):
    """Print average time cost per I/O operation (Valinor only)."""
    _p = lambda *a, **k: print(*a, **k, file=out)
    lookup = _build_lookup(summaries)

    scenarios = sorted(set(s.result_file.scenario for s in summaries))

    _p("\n" + "=" * 80)
    _p("IO COST — Average time per I/O operation (Valinor, q1+)")
    _p("=" * 80)
    _p(f"  (mcols={DEFAULT_MCOLS}, error_bound={DEFAULT_ERROR})")

    headers = ['Scenario', 'Method', 'Avg µs/IO', 'Avg IOs/Query', 'Avg Time/Query', 'Total IOs']
    rows = []

    for scenario in scenarios:
        for method in ['valinor_a', 'valinor_s']:
            s = _get(lookup, scenario, method, DEFAULT_MCOLS, DEFAULT_ERROR)
            if s is None:
                s = _get(lookup, scenario, method, 1, DEFAULT_ERROR)
            if s is None:
                continue

            avg_ios_per_q = s.mean_ios if not _isnan(s.mean_ios) else 0
            us_per_io = _fmt_time(s.mean_us_per_io, 1) if not _isnan(s.mean_us_per_io) else '-'

            rows.append([
                scenario,
                METHOD_LABELS.get(method, method),
                us_per_io,
                _fmt_int(s.mean_ios) if not _isnan(s.mean_ios) else '-',
                _fmt_time(s.mean_time),
                _fmt_int(s.total_ios),
            ])

    import builtins
    old_print = builtins.print
    builtins.print = lambda *a, **k: old_print(*a, **k, file=out)
    _print_table(headers, rows)
    builtins.print = old_print


def print_inventory(summaries: list[FileSummary], out=sys.stdout):
    """Print a compact inventory of what data exists."""
    _p = lambda *a, **k: print(*a, **k, file=out)

    _p("\n" + "=" * 80)
    _p("DATA INVENTORY — Available configs per scenario")
    _p("=" * 80)

    # Group by scenario
    by_scenario = defaultdict(lambda: defaultdict(set))
    for s in summaries:
        rf = s.result_file
        if s.status in ('ERROR_ALL', 'EMPTY'):
            continue
        by_scenario[rf.scenario][rf.method].add((rf.mcols, rf.error_bound))

    for scenario in sorted(by_scenario):
        _p(f"\n  {scenario}:")
        for method in METHOD_ORDER:
            if method in by_scenario[scenario]:
                configs = sorted(by_scenario[scenario][method])
                mcols_set = sorted(set(mc for mc, _ in configs))
                errors_set = sorted(set(e for _, e in configs))
                runs = set()
                for s in summaries:
                    rf = s.result_file
                    if rf.scenario == scenario and rf.method == method:
                        runs.add(rf.run)
                label = METHOD_LABELS.get(method, method)
                _p(f"    {label:15s}: mcols={mcols_set}, errors={errors_set}, runs={sorted(runs)}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Analyze experiment results')
    parser.add_argument('results_dir', help='Path to results base directory')
    parser.add_argument('--scenario', '-s', help='Filter to one scenario')
    parser.add_argument('--output', '-o', help='Write report to file instead of stdout')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show per-file details')
    args = parser.parse_args()

    base_dir = Path(args.results_dir)
    if not base_dir.is_dir():
        print(f"Error: {base_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    out = open(args.output, 'w') if args.output else sys.stdout

    try:
        # Discover files
        print(f"Scanning {base_dir} ...", file=sys.stderr)
        result_files = discover_results(base_dir)
        print(f"Found {len(result_files)} result files.", file=sys.stderr)

        if args.scenario:
            result_files = [rf for rf in result_files if rf.scenario == args.scenario]
            print(f"Filtered to {len(result_files)} files for scenario '{args.scenario}'.", file=sys.stderr)

        if not result_files:
            print("No result files found.", file=sys.stderr)
            sys.exit(1)

        # Parse and summarize
        print("Parsing CSV files ...", file=sys.stderr)
        summaries = []
        for rf in result_files:
            rows = parse_csv_file(rf)
            s = compute_summary(rf, rows)
            summaries.append(s)

        # Print report
        print(f"\n{'#' * 80}", file=out)
        print(f"# EXPERIMENT RESULTS ANALYSIS", file=out)
        print(f"# Base directory: {base_dir.resolve()}", file=out)
        if args.scenario:
            print(f"# Scenario filter: {args.scenario}", file=out)
        print(f"# Files analyzed: {len(summaries)}", file=out)
        print(f"{'#' * 80}", file=out)

        print_inventory(summaries, out)
        print_health_report(summaries, out)
        print_scenario_overview(summaries, out)
        print_error_sweep(summaries, out)
        print_mcols_sweep(summaries, out)
        print_selectivity_sweep(summaries, out)
        print_scalability_sweep(summaries, out)
        print_init_breakdown(summaries, out)
        print_valinor_comparison(summaries, out)
        print_io_cost(summaries, out)

        print(f"\n{'=' * 80}", file=out)
        print("END OF REPORT", file=out)
        print(f"{'=' * 80}", file=out)

    finally:
        if args.output:
            out.close()
            print(f"Report written to {args.output}", file=sys.stderr)


if __name__ == '__main__':
    main()
