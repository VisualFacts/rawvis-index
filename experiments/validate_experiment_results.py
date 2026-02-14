#!/usr/bin/env python3
"""
Validate experiment results across all methods.

Three validation modes:
  A. Bbox Consistency: Verify that all result files (Valinor exact/approx,
     DuckDB table, PilotDB) used the same bounding-box queries.
  B. CI / Error-Bound Validation:
     B.1 Valinor approximate: Check that DuckDB SUM values fall within
         confidence intervals (per scenario/mcols/error/run).
     B.2 PilotDB approximate: Check that PilotDB SUM relative error is
         within the claimed error bound vs DuckDB ground truth.
  C. Cross-validation: Compare Valinor exact results against DuckDB table results
     for all aggregates (count, sum, mean, min, max) to confirm both exact methods agree.

Uses scenarios defined in plotting/plot_config.py.
DuckDB table mode (deterministic SQL execution) is the independent ground truth.
"""

import re
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import pandas as pd

# Add plotting dir to path so we can import plot_config
sys.path.insert(0, str(Path(__file__).parent / "plotting"))
from plot_config import DATASETS, SELECTED_ERROR_BOUNDS, SELECTED_MEASURE_COLS


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class MeasureStats:
    """Aggregate statistics for a single measure column."""
    count: Optional[int] = None
    sum: Optional[float] = None
    mean: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None


AGGREGATE_NAMES = ['count', 'sum', 'mean', 'min', 'max']


# =============================================================================
# PARSING HELPERS
# =============================================================================

def parse_sum_dict(sum_str: str) -> Dict[int, float]:
    """
    Parse a Query Result Sum string like "{17=7.67E8, 3=9.70E7, ...}"
    into a dict mapping measure_col -> sum_value.
    """
    if pd.isna(sum_str) or not sum_str or sum_str == 'null':
        return {}
    try:
        result = {}
        pattern = r'(\d+)=([-\d.eE+]+)'
        for match in re.finditer(pattern, sum_str):
            col = int(match.group(1))
            val = float(match.group(2))
            result[col] = val
        return result
    except Exception:
        return {}


def parse_ci_dict(ci_str: str) -> Dict[int, Tuple[float, float]]:
    """
    Parse a Confidence Interval string like "{17=[lb, ub], 3=[lb, ub], ...}"
    into a dict mapping measure_col -> (lower_bound, upper_bound).
    """
    if pd.isna(ci_str) or not ci_str or ci_str == 'null':
        return {}
    try:
        result = {}
        pattern = r'(\d+)=\[([-\d.eE+]+),\s*([-\d.eE+]+)\]'
        for match in re.finditer(pattern, ci_str):
            col = int(match.group(1))
            lb = float(match.group(2))
            ub = float(match.group(3))
            result[col] = (lb, ub)
        return result
    except Exception:
        return {}


def parse_duckdb_query_result(qr_str: str) -> Dict[int, MeasureStats]:
    """
    Parse DuckDB Query Result string like:
      {12=StatsDuckDB{count=61050478, min=0.0000, max=400.0000, sum=691785375.3120, mean=11.3314, sumOfSquares=NaN}}
    Returns dict mapping measure_col -> MeasureStats.
    """
    if pd.isna(qr_str) or not qr_str or qr_str == 'null':
        return {}
    result = {}
    # Match each measure: col=StatsDuckDB{...}
    for m in re.finditer(r'(\d+)=StatsDuckDB\{([^}]+)\}', qr_str):
        col = int(m.group(1))
        body = m.group(2)
        vals = {}
        for kv in re.finditer(r'(\w+)=([-\d.eE+]+|NaN)', body):
            k, v = kv.group(1), kv.group(2)
            vals[k] = None if v == 'NaN' else float(v)
        result[col] = MeasureStats(
            count=int(vals['count']) if vals.get('count') is not None else None,
            sum=vals.get('sum'),
            mean=vals.get('mean'),
            min=vals.get('min'),
            max=vals.get('max'),
        )
    return result


def parse_valinor_query_result(qr_str: str) -> Dict[int, MeasureStats]:
    """
    Parse Valinor Query Result string like:
      {null={12=Stats{count=61050478, mean=11.331..., populationStandardDeviation=8.60..., min=0.0, max=400.0}}}
    Returns dict mapping measure_col -> MeasureStats.
    Note: Valinor Stats does not store sum directly; we compute sum = count * mean.
    """
    if pd.isna(qr_str) or not qr_str or qr_str == 'null':
        return {}
    result = {}
    for m in re.finditer(r'(\d+)=Stats\{([^}]+)\}', qr_str):
        col = int(m.group(1))
        body = m.group(2)
        vals = {}
        for kv in re.finditer(r'(\w+)=([-\d.eE+]+|NaN)', body):
            k, v = kv.group(1), kv.group(2)
            vals[k] = None if v == 'NaN' else float(v)
        count = int(vals['count']) if vals.get('count') is not None else None
        mean = vals.get('mean')
        computed_sum = (count * mean) if (count is not None and mean is not None) else None
        result[col] = MeasureStats(
            count=count,
            sum=computed_sum,
            mean=mean,
            min=vals.get('min'),
            max=vals.get('max'),
        )
    return result


# =============================================================================
# FILE DISCOVERY
# =============================================================================

def parse_filename(stem: str) -> Optional[Tuple[int, float, int]]:
    """Parse 'results_mcols{M}_error{E}_run{R}' -> (mcols, error, run) or None."""
    try:
        parts = stem.split('_')
        mcols = error = run = None
        for part in parts:
            if part.startswith('mcols'):
                mcols = int(part.replace('mcols', ''))
            elif part.startswith('error'):
                error = float(part.replace('error', ''))
            elif part.startswith('run'):
                run = int(part.replace('run', ''))
        if mcols is not None and error is not None and run is not None:
            return (mcols, error, run)
    except Exception:
        pass
    return None


def parse_duckdb_filename(stem: str) -> Optional[Tuple[int, int]]:
    """Parse 'results_mcols{M}_run{R}' -> (mcols, run) or None."""
    try:
        parts = stem.split('_')
        mcols = run = None
        for part in parts:
            if part.startswith('mcols'):
                mcols = int(part.replace('mcols', ''))
            elif part.startswith('run'):
                run = int(part.replace('run', ''))
        if mcols is not None and run is not None:
            return (mcols, run)
    except Exception:
        pass
    return None


def find_duckdb_table_file(results_dir: Path, mcols: int, run: int = 1) -> Optional[Path]:
    """Find DuckDB table result file for a given mcols. Uses run1 by default (deterministic)."""
    duckdb_dir = results_dir / "duckdb" / "table"
    candidate = duckdb_dir / f"results_mcols{mcols}_run{run}.csv"
    if candidate.exists():
        return candidate
    # Fallback: try any available run
    for f in sorted(duckdb_dir.glob(f"results_mcols{mcols}_run*.csv")):
        return f
    return None


def find_valinor_exact_file(results_dir: Path, mcols: int, run: int) -> Optional[Path]:
    """Find Valinor exact (error=0) file for a given mcols/run."""
    candidates = [
        results_dir / f"results_mcols{mcols}_error0_run{run}.csv",
        results_dir / f"results_mcols{mcols}_error0.0_run{run}.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


# =============================================================================
# PART A: BBOX CONSISTENCY CHECK
# =============================================================================

@dataclass
class BBox:
    """A 2D bounding box with (xmin, xmax, ymin, ymax)."""
    xmin: float
    xmax: float
    ymin: float
    ymax: float

    def approx_eq(self, other: 'BBox', tol: float = 1e-9) -> bool:
        return (abs(self.xmin - other.xmin) < tol and
                abs(self.xmax - other.xmax) < tol and
                abs(self.ymin - other.ymin) < tol and
                abs(self.ymax - other.ymax) < tol)

    def __repr__(self):
        return f"({self.xmin}..{self.xmax}),({self.ymin}..{self.ymax})"


def parse_bbox_valinor(query_str: str) -> Optional[BBox]:
    """
    Parse bbox from Valinor query column.
    Format: rect=(xmin..xmax),(ymin..ymax)
    Example: Query{op=P, rect=(-73.9965..-73.9465),(40.74825..40.77325), ...}
    """
    if not query_str:
        return None
    m = re.search(r'rect=\(([-\d.eE+]+)\.\.([-\d.eE+]+)\),\(([-\d.eE+]+)\.\.([-\d.eE+]+)\)', query_str)
    if m:
        return BBox(xmin=float(m.group(1)), xmax=float(m.group(2)),
                    ymin=float(m.group(3)), ymax=float(m.group(4)))
    return None


def parse_bbox_sql(query_str: str) -> Optional[BBox]:
    """
    Parse bbox from SQL WHERE clause (DuckDB or PilotDB).
    Handles both:
      - CAST(column05 AS FLOAT) > CAST(-73.99 AS FLOAT) AND ...
      - column05 > -73.99 AND ...
    """
    if not query_str:
        return None
    gt_pattern = r'(?:CAST\()?column(\d+)(?:\s+AS\s+FLOAT\))?\s*>\s*(?:CAST\()?([-\d.eE+]+)(?:\s+AS\s+FLOAT\))?'
    lt_pattern = r'(?:CAST\()?column(\d+)(?:\s+AS\s+FLOAT\))?\s*<\s*(?:CAST\()?([-\d.eE+]+)(?:\s+AS\s+FLOAT\))?'

    gt_matches = [(int(m.group(1)), float(m.group(2))) for m in re.finditer(gt_pattern, query_str)]
    lt_matches = [(int(m.group(1)), float(m.group(2))) for m in re.finditer(lt_pattern, query_str)]

    if len(gt_matches) < 2 or len(lt_matches) < 2:
        return None

    gt_by_col = {}
    for col_idx, val in gt_matches:
        gt_by_col.setdefault(col_idx, []).append(val)
    lt_by_col = {}
    for col_idx, val in lt_matches:
        lt_by_col.setdefault(col_idx, []).append(val)

    spatial_cols = sorted(set(gt_by_col.keys()) & set(lt_by_col.keys()))
    if len(spatial_cols) < 2:
        return None

    col_x, col_y = spatial_cols[0], spatial_cols[1]
    return BBox(
        xmin=gt_by_col[col_x][0], xmax=lt_by_col[col_x][0],
        ymin=gt_by_col[col_y][0], ymax=lt_by_col[col_y][0],
    )


def _extract_bboxes_from_csv(
    csv_path: Path,
    source_type: str,
) -> Dict[int, BBox]:
    """
    Extract bboxes from a result CSV for all query indices.

    source_type: 'valinor' (uses 'query' column) or 'sql' (uses 'Query' column)
    Returns dict mapping query_index -> BBox.
    """
    df = pd.read_csv(csv_path)
    result = {}

    query_col = 'query' if source_type == 'valinor' else 'Query'
    if query_col not in df.columns:
        return result

    parse_fn = parse_bbox_valinor if source_type == 'valinor' else parse_bbox_sql

    for _, row in df.iterrows():
        qi = int(row['i'])
        raw = str(row.get(query_col, ''))
        bbox = parse_fn(raw)
        if bbox is not None:
            result[qi] = bbox

    return result


def validate_bboxes_for_scenario(
    results_dir: str,
    scenario_label: str,
    selected_measure_cols: List[int] = None,
    verbose: bool = True,
) -> Tuple[int, int]:
    """
    Check bbox consistency across all result files in a scenario.

    For each mcols value, collects bboxes from all sources (Valinor exact/approx,
    DuckDB table, PilotDB) and verifies they all agree on the same bbox per query.

    Returns (total_queries_checked, total_mismatches).
    """
    if selected_measure_cols is None:
        selected_measure_cols = SELECTED_MEASURE_COLS

    dir_path = Path(results_dir)
    if not dir_path.exists():
        if verbose:
            print(f"  Directory not found: {dir_path}")
        return 0, 0

    total_checked = 0
    total_mismatches = 0

    for mcols in selected_measure_cols:
        # Collect all available sources for this mcols
        sources: Dict[str, Dict[int, BBox]] = {}

        # Valinor exact (use run1)
        vex_path = find_valinor_exact_file(dir_path, mcols, run=1)
        if vex_path:
            sources['valinor_exact'] = _extract_bboxes_from_csv(vex_path, 'valinor')

        # Valinor approx (pick first available)
        for f in sorted(dir_path.glob(f'results_mcols{mcols}_error*_run1.csv')):
            parsed = parse_filename(f.stem)
            if parsed and parsed[1] > 0:  # non-zero error
                label = f'valinor_e{parsed[1]}'
                sources[label] = _extract_bboxes_from_csv(f, 'valinor')
                break

        # DuckDB table
        ddb_path = find_duckdb_table_file(dir_path, mcols)
        if ddb_path:
            sources['duckdb_table'] = _extract_bboxes_from_csv(ddb_path, 'sql')

        # PilotDB
        pdb_dir = dir_path / 'pilotdb'
        if pdb_dir.exists():
            for f in sorted(pdb_dir.glob(f'results_mcols{mcols}_error*_run1.csv')):
                sources['pilotdb'] = _extract_bboxes_from_csv(f, 'sql')
                break

        if len(sources) < 2:
            if verbose:
                print(f"  mcols={mcols}: only {len(sources)} source(s) found — skipping")
            continue

        # Use the first source as reference, compare all others
        ref_label = list(sources.keys())[0]
        ref_bboxes = sources[ref_label]

        all_qi = set()
        for bboxes in sources.values():
            all_qi |= set(bboxes.keys())

        mismatches = []
        checked = 0
        for qi in sorted(all_qi):
            ref_bb = ref_bboxes.get(qi)
            if ref_bb is None:
                continue
            checked += 1
            for src_label, src_bboxes in sources.items():
                if src_label == ref_label:
                    continue
                other_bb = src_bboxes.get(qi)
                if other_bb is None:
                    mismatches.append((qi, src_label, 'MISSING'))
                elif not ref_bb.approx_eq(other_bb):
                    mismatches.append((qi, src_label, f'{ref_bb} vs {other_bb}'))

        total_checked += checked
        total_mismatches += len(mismatches)

        if verbose:
            src_list = ', '.join(sources.keys())
            if not mismatches:
                print(f"  ✓ mcols={mcols}: {checked} queries consistent across [{src_list}]")
            else:
                print(f"  ✗ mcols={mcols}: {len(mismatches)} mismatches (ref: {ref_label})")
                for qi, src, detail in mismatches[:10]:
                    print(f"      query {qi}: {src} — {detail}")
                if len(mismatches) > 10:
                    print(f"      ... and {len(mismatches) - 10} more")

    return total_checked, total_mismatches


# =============================================================================
# PART B: CI VALIDATION (Valinor approximate CIs vs DuckDB table SUM)
# =============================================================================

def validate_ci_against_duckdb(
    duckdb_path: Path,
    approx_path: Path,
) -> Tuple[List[dict], Dict[int, int], Dict[int, List[float]]]:
    """
    Check that DuckDB ground-truth SUM values fall within Valinor approximate CIs.

    Returns:
      violations: list of dicts with query_idx, measure_col, duckdb_sum, ci_lower, ci_upper, rel_error
      per_measure_total: dict mapping measure_col -> total number of CIs checked
      all_rel_errors: dict mapping measure_col -> list of ALL relative errors
                      (|CI_midpoint - true| / |true| for every comparison)
    """
    duckdb_df = pd.read_csv(duckdb_path)
    approx_df = pd.read_csv(approx_path)

    violations = []
    per_measure_total: Dict[int, int] = {}
    all_rel_errors: Dict[int, List[float]] = {}

    for _, approx_row in approx_df.iterrows():
        qi = approx_row['i']
        ci_dict = parse_ci_dict(str(approx_row.get('Confidence Interval', '')))
        if not ci_dict:
            continue  # init query or missing CI

        # Find matching DuckDB row
        duckdb_match = duckdb_df[duckdb_df['i'] == qi]
        if duckdb_match.empty:
            continue
        duckdb_row = duckdb_match.iloc[0]
        sum_dict = parse_sum_dict(str(duckdb_row.get('Query Result Sum', '')))
        if not sum_dict:
            continue

        for col, (lb, ub) in ci_dict.items():
            duckdb_sum = sum_dict.get(col)
            if duckdb_sum is None:
                continue

            per_measure_total[col] = per_measure_total.get(col, 0) + 1

            # Compute relative error of CI midpoint vs ground truth (for ALL queries)
            midpoint = (lb + ub) / 2.0
            if duckdb_sum != 0:
                rel_err = abs(midpoint - duckdb_sum) / abs(duckdb_sum)
            else:
                rel_err = float('inf') if midpoint != 0 else 0.0
            all_rel_errors.setdefault(col, []).append(rel_err)

            if duckdb_sum < lb or duckdb_sum > ub:
                violations.append({
                    'query_idx': qi,
                    'measure_col': col,
                    'duckdb_sum': duckdb_sum,
                    'ci_lower': lb,
                    'ci_upper': ub,
                    'rel_error': rel_err,
                })

    return violations, per_measure_total, all_rel_errors


# Threshold for per-measure violation rate to be considered "expected statistical noise"
# A 95% CI should miss ~5% of the time; we allow some slack for finite samples.
EXPECTED_VIOLATION_RATE = 0.10  # 10% — generous threshold to separate noise from bugs


def classify_violations(
    violations: List[dict],
    per_measure_total: Dict[int, int],
) -> str:
    """
    Classify a file pair result as 'pass', 'statistical', or 'systematic'.

    - 'pass': zero violations
    - 'statistical': all per-measure violation rates <= EXPECTED_VIOLATION_RATE
    - 'systematic': at least one measure exceeds EXPECTED_VIOLATION_RATE
    """
    if not violations:
        return 'pass'

    by_col: Dict[int, int] = {}
    for v in violations:
        by_col[v['measure_col']] = by_col.get(v['measure_col'], 0) + 1

    for col, n_fail in by_col.items():
        n_total = per_measure_total.get(col, 1)
        if n_fail / n_total > EXPECTED_VIOLATION_RATE:
            return 'systematic'

    return 'statistical'


def validate_cis_for_scenario(
    results_dir: str,
    scenario_label: str,
    selected_error_bounds: List[float] = None,
    selected_measure_cols: List[int] = None,
    verbose: bool = True,
) -> Dict[str, Tuple[List[dict], Dict[int, int]]]:
    """
    Validate all Valinor approximate CIs against DuckDB table ground truth.

    Returns dict keyed by "mcols{M}_error{E}_run{R}" -> (violations, per_measure_total).
    """
    if selected_error_bounds is None:
        selected_error_bounds = [e for e in SELECTED_ERROR_BOUNDS if e > 0]
    else:
        selected_error_bounds = [e for e in selected_error_bounds if e > 0]

    if selected_measure_cols is None:
        selected_measure_cols = SELECTED_MEASURE_COLS

    dir_path = Path(results_dir)
    if not dir_path.exists():
        if verbose:
            print(f"  Directory not found: {dir_path}")
        return {}

    all_violations = {}

    for csv_file in sorted(dir_path.glob("results_mcols*_error*_run*.csv")):
        parsed = parse_filename(csv_file.stem)
        if parsed is None:
            continue
        mcols, error, run = parsed

        if error == 0 or error not in selected_error_bounds:
            continue
        if mcols not in selected_measure_cols:
            continue

        # Find DuckDB table ground truth (deterministic — use run1)
        duckdb_path = find_duckdb_table_file(dir_path, mcols)
        if duckdb_path is None:
            if verbose:
                print(f"  WARNING: No DuckDB table file for mcols={mcols}")
            continue

        violations, per_measure_total, all_rel_errors = validate_ci_against_duckdb(duckdb_path, csv_file)
        key = f"mcols{mcols}_error{error}_run{run}"
        all_violations[key] = (violations, per_measure_total, all_rel_errors)

        total_comparisons = sum(per_measure_total.values())
        n_violations = len(violations)
        classification = classify_violations(violations, per_measure_total)

        if verbose:
            _print_violation_details(key, violations, per_measure_total, classification,
                                    all_rel_errors=all_rel_errors)

    return all_violations


def validate_valinor_s_for_scenario(
    results_dir: str,
    scenario_label: str,
    selected_error_bounds: List[float] = None,
    selected_measure_cols: List[int] = None,
    verbose: bool = True,
) -> Dict[str, Tuple[List[dict], Dict[int, int], Dict[int, List[float]]]]:
    """
    Validate all VALINOR-S (sampling-only) CIs against DuckDB table ground truth.

    VALINOR-S results live in {results_dir}/valinor_s/ but use the same CI format
    as the full Valinor approximate. DuckDB ground truth is in {results_dir}/duckdb/table/.
    """
    if selected_error_bounds is None:
        selected_error_bounds = [e for e in SELECTED_ERROR_BOUNDS if e > 0]
    else:
        selected_error_bounds = [e for e in selected_error_bounds if e > 0]

    if selected_measure_cols is None:
        selected_measure_cols = SELECTED_MEASURE_COLS

    dir_path = Path(results_dir)
    vs_dir = dir_path / "valinor_s"
    if not vs_dir.exists():
        if verbose:
            print(f"  No valinor_s directory found: {vs_dir}")
        return {}

    all_violations = {}

    for csv_file in sorted(vs_dir.glob("results_mcols*_error*_run*.csv")):
        parsed = parse_filename(csv_file.stem)
        if parsed is None:
            continue
        mcols, error, run = parsed

        if error == 0 or error not in selected_error_bounds:
            continue
        if mcols not in selected_measure_cols:
            continue

        # DuckDB ground truth is in the *parent* (scenario root) dir
        duckdb_path = find_duckdb_table_file(dir_path, mcols)
        if duckdb_path is None:
            if verbose:
                print(f"  WARNING: No DuckDB table file for mcols={mcols}")
            continue

        violations, per_measure_total, all_rel_errors = validate_ci_against_duckdb(duckdb_path, csv_file)
        key = f"mcols{mcols}_error{error}_run{run}"
        all_violations[key] = (violations, per_measure_total, all_rel_errors)

        if verbose:
            _print_violation_details(key, violations, per_measure_total,
                                    classify_violations(violations, per_measure_total),
                                    all_rel_errors=all_rel_errors)

    return all_violations


# -----------------------------------------------------------------------------
# PART B.2: PilotDB error-bound validation (approximate SUM vs DuckDB table)
# -----------------------------------------------------------------------------

def validate_pilotdb_against_duckdb(
    duckdb_path: Path,
    pilotdb_path: Path,
) -> Tuple[List[dict], Dict[int, int], Dict[int, List[float]]]:
    """
    Check that PilotDB SUM values are within their claimed error bound of
    the DuckDB ground truth.

    For each query/measure, computes:
        rel_error = |pilotdb_sum - duckdb_sum| / |duckdb_sum|
    and flags if rel_error > errorBound.

    Returns:
      violations: list of dicts with query_idx, measure_col, duckdb_sum,
                  pilotdb_sum, rel_error, error_bound
      per_measure_total: dict mapping measure_col -> total comparisons
      all_rel_errors: dict mapping measure_col -> list of ALL relative errors
    """
    duckdb_df = pd.read_csv(duckdb_path)
    pilotdb_df = pd.read_csv(pilotdb_path)

    violations = []
    per_measure_total: Dict[int, int] = {}
    all_rel_errors: Dict[int, List[float]] = {}

    for _, pdb_row in pilotdb_df.iterrows():
        qi = pdb_row['i']
        eb = float(pdb_row.get('errorBound', 0))
        pdb_sum = parse_sum_dict(str(pdb_row.get('Query Result Sum', '')))
        if not pdb_sum:
            continue

        # Find matching DuckDB row
        duckdb_match = duckdb_df[duckdb_df['i'] == qi]
        if duckdb_match.empty:
            continue
        duckdb_row = duckdb_match.iloc[0]
        ddb_sum = parse_sum_dict(str(duckdb_row.get('Query Result Sum', '')))
        if not ddb_sum:
            continue

        for col, pdb_val in pdb_sum.items():
            ddb_val = ddb_sum.get(col)
            if ddb_val is None:
                continue

            per_measure_total[col] = per_measure_total.get(col, 0) + 1

            if ddb_val == 0:
                rel_err = float('inf') if pdb_val != 0 else 0.0
            else:
                rel_err = abs(pdb_val - ddb_val) / abs(ddb_val)
            all_rel_errors.setdefault(col, []).append(rel_err)

            if rel_err > eb:
                violations.append({
                    'query_idx': qi,
                    'measure_col': col,
                    'duckdb_sum': ddb_val,
                    'pilotdb_sum': pdb_val,
                    'rel_error': rel_err,
                    'error_bound': eb,
                })

    return violations, per_measure_total, all_rel_errors


def validate_pilotdb_for_scenario(
    results_dir: str,
    scenario_label: str,
    selected_error_bounds: List[float] = None,
    selected_measure_cols: List[int] = None,
    verbose: bool = True,
) -> Dict[str, Tuple[List[dict], Dict[int, int]]]:
    """
    Validate all PilotDB approximate results against DuckDB table ground truth.

    Returns dict keyed by "mcols{M}_error{E}_run{R}" -> (violations, per_measure_total).
    """
    if selected_error_bounds is None:
        selected_error_bounds = [e for e in SELECTED_ERROR_BOUNDS if e > 0]
    else:
        selected_error_bounds = [e for e in selected_error_bounds if e > 0]

    if selected_measure_cols is None:
        selected_measure_cols = SELECTED_MEASURE_COLS

    dir_path = Path(results_dir)
    pdb_dir = dir_path / 'pilotdb'
    if not pdb_dir.exists():
        if verbose:
            print(f"  No pilotdb/ subdirectory found in {dir_path}")
        return {}

    all_violations = {}

    for csv_file in sorted(pdb_dir.glob("results_mcols*_error*_run*.csv")):
        parsed = parse_filename(csv_file.stem)
        if parsed is None:
            continue
        mcols, error, run = parsed

        if error == 0 or error not in selected_error_bounds:
            continue
        if mcols not in selected_measure_cols:
            continue

        duckdb_path = find_duckdb_table_file(dir_path, mcols)
        if duckdb_path is None:
            if verbose:
                print(f"  WARNING: No DuckDB table file for mcols={mcols}")
            continue

        violations, per_measure_total, all_rel_errors = validate_pilotdb_against_duckdb(
            duckdb_path, csv_file,
        )
        key = f"mcols{mcols}_error{error}_run{run}"
        all_violations[key] = (violations, per_measure_total, all_rel_errors)

        classification = classify_violations(violations, per_measure_total)

        if verbose:
            _print_violation_details(key, violations, per_measure_total, classification,
                                    all_rel_errors=all_rel_errors)

    return all_violations


# =============================================================================
# PART C: CROSS-VALIDATION (Valinor exact vs DuckDB table — all aggregates)
# =============================================================================

# Threshold for flagging relative differences between exact methods
EXACT_REL_DIFF_THRESHOLD = 1e-6  # flag anything > 0.0001%


def cross_validate_exact_pair(
    duckdb_path: Path,
    valinor_path: Path,
) -> List[dict]:
    """
    Compare all aggregates between Valinor exact and DuckDB table for one file pair.

    Returns list of diffs with: query_idx, measure_col, aggregate, valinor_val, duckdb_val, rel_diff
    """
    duckdb_df = pd.read_csv(duckdb_path)
    valinor_df = pd.read_csv(valinor_path)

    diffs = []

    for _, valinor_row in valinor_df.iterrows():
        qi = valinor_row['i']

        # Parse Valinor Query Result
        valinor_stats = parse_valinor_query_result(
            str(valinor_row.get('Query Result', '')))
        # Also get Valinor Query Result Sum (more precise sum than count*mean)
        valinor_sums = parse_sum_dict(
            str(valinor_row.get('Query Result Sum', '')))

        if not valinor_stats and not valinor_sums:
            continue  # init query

        # Find matching DuckDB row
        duckdb_match = duckdb_df[duckdb_df['i'] == qi]
        if duckdb_match.empty:
            continue
        duckdb_row = duckdb_match.iloc[0]

        duckdb_stats = parse_duckdb_query_result(
            str(duckdb_row.get('Query Result', '')))
        duckdb_sums = parse_sum_dict(
            str(duckdb_row.get('Query Result Sum', '')))

        if not duckdb_stats and not duckdb_sums:
            continue

        # Compare all shared measure columns
        all_cols = set(valinor_stats.keys()) | set(duckdb_stats.keys())
        for col in sorted(all_cols):
            v_stats = valinor_stats.get(col)
            d_stats = duckdb_stats.get(col)

            # Use the explicit Query Result Sum for sum comparison (more precise)
            v_sum = valinor_sums.get(col)
            d_sum = duckdb_sums.get(col)

            # Build pairs to compare: (aggregate_name, valinor_value, duckdb_value)
            pairs = []
            if v_stats and d_stats:
                pairs.append(('count', v_stats.count, d_stats.count))
                pairs.append(('min', v_stats.min, d_stats.min))
                pairs.append(('max', v_stats.max, d_stats.max))
            if v_sum is not None and d_sum is not None:
                pairs.append(('sum', v_sum, d_sum))
            elif v_stats and d_stats:
                pairs.append(('sum', v_stats.sum, d_stats.sum))
            # Derive mean from full-precision sum/count instead of comparing
            # the truncated string representation (DuckDB rounds to 4 decimals).
            if v_stats and d_stats and v_stats.count and d_stats.count:
                v_mean = (v_sum if v_sum is not None else v_stats.sum) / v_stats.count
                d_mean = (d_sum if d_sum is not None else d_stats.sum) / d_stats.count
                pairs.append(('mean', v_mean, d_mean))

            for agg_name, v_val, d_val in pairs:
                if v_val is None or d_val is None:
                    continue
                if d_val != 0:
                    rel_diff = abs(v_val - d_val) / abs(d_val)
                elif v_val != 0:
                    rel_diff = float('inf')
                else:
                    rel_diff = 0.0

                if rel_diff > EXACT_REL_DIFF_THRESHOLD:
                    diffs.append({
                        'query_idx': qi,
                        'measure_col': col,
                        'aggregate': agg_name,
                        'valinor_val': v_val,
                        'duckdb_val': d_val,
                        'rel_diff': rel_diff,
                    })

    return diffs


def cross_validate_scenario(
    results_dir: str,
    scenario_label: str,
    selected_measure_cols: List[int] = None,
    verbose: bool = True,
) -> Dict[str, List[dict]]:
    """
    Cross-validate Valinor exact vs DuckDB table for all aggregates.

    Returns dict keyed by "mcols{M}_run{R}" -> list of diffs.
    """
    if selected_measure_cols is None:
        selected_measure_cols = SELECTED_MEASURE_COLS

    dir_path = Path(results_dir)
    if not dir_path.exists():
        if verbose:
            print(f"  Directory not found: {dir_path}")
        return {}

    all_diffs = {}

    # Discover all Valinor exact files (error=0)
    for csv_file in sorted(dir_path.glob("results_mcols*_error0_run*.csv")):
        parsed = parse_filename(csv_file.stem)
        if parsed is None:
            continue
        mcols, error, run = parsed
        if error != 0:
            continue
        if mcols not in selected_measure_cols:
            continue

        duckdb_path = find_duckdb_table_file(dir_path, mcols)
        if duckdb_path is None:
            if verbose:
                print(f"  WARNING: No DuckDB table file for mcols={mcols}")
            continue

        diffs = cross_validate_exact_pair(duckdb_path, csv_file)
        key = f"mcols{mcols}_run{run}"
        all_diffs[key] = diffs

        n_queries = sum(1 for _ in pd.read_csv(csv_file).iterrows()) - 1  # exclude init
        if verbose:
            if not diffs:
                print(f"  ✓ {key}: all aggregates match DuckDB (threshold={EXACT_REL_DIFF_THRESHOLD:.0e})")
            else:
                # Group by aggregate type
                by_agg: Dict[str, List[dict]] = {}
                for d in diffs:
                    by_agg.setdefault(d['aggregate'], []).append(d)

                max_diff = max(d['rel_diff'] for d in diffs)
                print(f"  ⚠ {key}: {len(diffs)} differences > {EXACT_REL_DIFF_THRESHOLD:.0e} "
                      f"(max_rel_diff={max_diff:.2e})")
                for agg_name in AGGREGATE_NAMES:
                    agg_diffs = by_agg.get(agg_name, [])
                    if not agg_diffs:
                        continue
                    max_d = max(d['rel_diff'] for d in agg_diffs)
                    n_cols = len(set(d['measure_col'] for d in agg_diffs))
                    n_qs = len(set(d['query_idx'] for d in agg_diffs))
                    print(f"      {agg_name}: {len(agg_diffs)} diffs across "
                          f"{n_cols} col(s), {n_qs} query(ies), max_rel_diff={max_d:.2e}")

    return all_diffs


# =============================================================================
# SHARED PRINTING
# =============================================================================

def _print_violation_details(
    key: str,
    violations: List[dict],
    per_measure_total: Dict[int, int],
    classification: str,
    all_rel_errors: Dict[int, List[float]] = None,
):
    """Print detailed per-measure violation breakdown for one file pair."""
    total_comparisons = sum(per_measure_total.values())
    n_violations = len(violations)

    # Group violations by measure col
    by_col: Dict[int, List[dict]] = {}
    for v in violations:
        by_col.setdefault(v['measure_col'], []).append(v)

    col_summaries = []
    for col in sorted(per_measure_total.keys()):
        n_total = per_measure_total[col]
        col_violations = by_col.get(col, [])
        n_fail = len(col_violations)
        pct = 100.0 * n_fail / n_total if n_total > 0 else 0.0
        exceeds = (n_fail / n_total > EXPECTED_VIOLATION_RATE) if n_total > 0 else False
        col_summaries.append((col, n_fail, n_total, pct, exceeds, col_violations))

    if classification == 'pass':
        marker, status = "✓", "PASS"
    elif classification == 'statistical':
        marker, status = "~", "OK (within expected noise)"
    else:
        marker, status = "✗", f"FAIL >{EXPECTED_VIOLATION_RATE*100:.0f}% violation rate"

    # Compute overall avg relative error across all measures
    avg_rel_str = ""
    if all_rel_errors:
        flat_errors = [e for errs in all_rel_errors.values() for e in errs if e != float('inf')]
        if flat_errors:
            mean_err = sum(flat_errors) / len(flat_errors)
            max_err = max(flat_errors)
            avg_rel_str = f"  avg_rel_err={mean_err:.6f}, max={max_err:.6f}"

    print(f"  {marker} {key}: {n_violations}/{total_comparisons} violations  [{status}]{avg_rel_str}")

    for col, n_fail, n_total, pct, exceeds, col_violations in col_summaries:
        flag = f" ⚠️  EXCEEDS {EXPECTED_VIOLATION_RATE*100:.0f}%" if exceeds else ""
        # Per-column avg relative error
        col_avg_str = ""
        if all_rel_errors and col in all_rel_errors:
            col_errs = [e for e in all_rel_errors[col] if e != float('inf')]
            if col_errs:
                col_avg_str = f", avg_rel_err={sum(col_errs)/len(col_errs):.6f}"
        if n_fail == 0:
            print(f"      col {col}: 0/{n_total} (0.0%) violations{col_avg_str}")
            continue
        max_err = max(v['rel_error'] for v in col_violations)
        query_idxs = sorted(v['query_idx'] for v in col_violations)
        if len(query_idxs) <= 10:
            idx_str = str(query_idxs)
        else:
            idx_str = f"{query_idxs[:5]}...{query_idxs[-3:]} ({len(query_idxs)} total)"
        print(f"      col {col}: {n_fail}/{n_total} ({pct:.1f}%) violations, "
              f"max_rel_error={max_err:.4f}{col_avg_str}, queries={idx_str}{flag}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Validate experiment results: bbox consistency, approximate CIs, "
                    "and cross-validate Valinor exact vs DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s                          # Run all validations\n"
            "  %(prog)s --scenario taxi/zoom      # One scenario only\n"
            "  %(prog)s --bbox-only               # Only check bbox consistency\n"
            "  %(prog)s --ci-only                 # Only CI validation\n"
            "  %(prog)s --cross-only              # Only cross-validation\n"
            "  %(prog)s --skip-bbox               # Skip bbox check\n"
        ),
    )
    parser.add_argument('--scenario', type=str, default=None,
                        help="Run only this dataset/scenario (e.g. 'taxi/zoom'). Default: all.")
    parser.add_argument('--quiet', action='store_true',
                        help="Only print failures.")
    parser.add_argument('--error-bounds', type=float, nargs='+', default=None,
                        help="Error bounds to check (default: all non-zero from config)")
    parser.add_argument('--mcols', type=int, nargs='+', default=None,
                        help="Measure column counts to check (default: all from config)")
    parser.add_argument('--bbox-only', action='store_true',
                        help="Only run bbox consistency check")
    parser.add_argument('--ci-only', action='store_true',
                        help="Only run CI validation (skip bbox and cross-validation)")
    parser.add_argument('--cross-only', action='store_true',
                        help="Only run cross-validation (skip bbox and CI)")
    parser.add_argument('--skip-bbox', action='store_true',
                        help="Skip bbox consistency check")
    args = parser.parse_args()

    script_dir = Path(__file__).parent

    # Determine which parts to run
    if args.bbox_only:
        run_bbox, run_ci, run_cross = True, False, False
    elif args.ci_only:
        run_bbox, run_ci, run_cross = False, True, False
    elif args.cross_only:
        run_bbox, run_ci, run_cross = False, False, True
    else:
        run_bbox = not args.skip_bbox
        run_ci = True
        run_cross = True

    # =========================================================================
    # PART A: BBOX CONSISTENCY
    # =========================================================================
    total_bbox_checked = 0
    total_bbox_mismatches = 0
    bbox_ok = True

    if run_bbox:
        print("\n" + "=" * 70)
        print("PART A: BBOX CONSISTENCY — Check bounding boxes match across all methods")
        print("=" * 70)

        for ds_key, ds_info in DATASETS.items():
            for sc_key, sc_info in ds_info['scenarios'].items():
                scenario_id = f"{ds_key}/{sc_key}"
                if args.scenario and args.scenario != scenario_id:
                    continue

                results_dir = (script_dir / "plotting" / sc_info['dir']).resolve()

                print(f"\n{'-'*60}")
                print(f"Scenario: {ds_info['name']} / {sc_info['description']}")
                print(f"Directory: {results_dir}")
                print(f"{'-'*60}")

                checked, mismatches = validate_bboxes_for_scenario(
                    str(results_dir),
                    scenario_label=f"{ds_info['label']}-{sc_key}",
                    selected_measure_cols=args.mcols,
                    verbose=not args.quiet,
                )
                total_bbox_checked += checked
                total_bbox_mismatches += mismatches

        if total_bbox_mismatches > 0:
            bbox_ok = False

    # =========================================================================
    # PART B: CI VALIDATION
    # =========================================================================
    total_pass = total_statistical = total_systematic = 0
    by_mcols_error: Dict[Tuple[int, float], List[Tuple[str, int, int, float]]] = {}
    pdb_pass = pdb_statistical = pdb_systematic = 0
    pdb_by_mcols_error: Dict[Tuple[int, float], List[Tuple[str, int, int, float]]] = {}
    vs_pass = vs_statistical = vs_systematic = 0
    vs_by_mcols_error: Dict[Tuple[int, float], List[Tuple[str, int, int, float]]] = {}

    if run_ci:
        print("\n" + "=" * 70)
        print("PART B: CI VALIDATION — Valinor approximate CIs vs DuckDB table SUM")
        print("=" * 70)

        for ds_key, ds_info in DATASETS.items():
            for sc_key, sc_info in ds_info['scenarios'].items():
                scenario_id = f"{ds_key}/{sc_key}"
                if args.scenario and args.scenario != scenario_id:
                    continue

                results_dir = (script_dir / "plotting" / sc_info['dir']).resolve()

                print(f"\n{'-'*60}")
                print(f"Scenario: {ds_info['name']} / {sc_info['description']}")
                print(f"Directory: {results_dir}")
                print(f"{'-'*60}")

                violations = validate_cis_for_scenario(
                    str(results_dir),
                    scenario_label=f"{ds_info['label']}-{sc_key}",
                    selected_error_bounds=args.error_bounds,
                    selected_measure_cols=args.mcols,
                    verbose=not args.quiet,
                )

                for key, (v_list, per_measure_total, all_rel_errors) in violations.items():
                    classification = classify_violations(v_list, per_measure_total)
                    if classification == 'pass':
                        total_pass += 1
                    elif classification == 'statistical':
                        total_statistical += 1
                    else:
                        total_systematic += 1
                        if args.quiet:
                            print(f"  ✗ {scenario_id} / {key}: {len(v_list)} violations")

                    # Compute overall mean relative error for this file pair
                    flat_errors = [e for errs in all_rel_errors.values()
                                   for e in errs if e != float('inf')]
                    mean_rel = sum(flat_errors) / len(flat_errors) if flat_errors else 0.0

                    m = re.match(r'mcols(\d+)_error([\d.]+)_run\d+', key)
                    if m:
                        mcols_val = int(m.group(1))
                        error_val = float(m.group(2))
                        n_total = sum(per_measure_total.values())
                        by_col_count: Dict[int, int] = {}
                        for v in v_list:
                            by_col_count[v['measure_col']] = by_col_count.get(v['measure_col'], 0) + 1
                        max_rate = 0.0
                        for col, col_total in per_measure_total.items():
                            if col_total > 0:
                                rate = by_col_count.get(col, 0) / col_total
                                max_rate = max(max_rate, rate)
                        by_mcols_error.setdefault((mcols_val, error_val), []).append(
                            (classification, len(v_list), n_total, max_rate, mean_rel)
                        )

        # ----- Part B.1.5: VALINOR-S (sampling-only) CI validation -----

        print("\n" + "-" * 70)
        print("PART B.1.5: VALINOR-S (sampling-only) CIs vs DuckDB table SUM")
        print("-" * 70)

        for ds_key, ds_info in DATASETS.items():
            for sc_key, sc_info in ds_info['scenarios'].items():
                scenario_id = f"{ds_key}/{sc_key}"
                if args.scenario and args.scenario != scenario_id:
                    continue

                results_dir = (script_dir / "plotting" / sc_info['dir']).resolve()

                print(f"\n{'-'*60}")
                print(f"Scenario: {ds_info['name']} / {sc_info['description']}")
                print(f"Directory: {results_dir}/valinor_s")
                print(f"{'-'*60}")

                vs_violations = validate_valinor_s_for_scenario(
                    str(results_dir),
                    scenario_label=f"{ds_info['label']}-{sc_key}",
                    selected_error_bounds=args.error_bounds,
                    selected_measure_cols=args.mcols,
                    verbose=not args.quiet,
                )

                for key, (v_list, per_measure_total, all_rel_errors) in vs_violations.items():
                    classification = classify_violations(v_list, per_measure_total)
                    if classification == 'pass':
                        vs_pass += 1
                    elif classification == 'statistical':
                        vs_statistical += 1
                    else:
                        vs_systematic += 1
                        if args.quiet:
                            print(f"  ✗ {scenario_id} / valinor_s / {key}: {len(v_list)} violations")

                    flat_errors = [e for errs in all_rel_errors.values()
                                   for e in errs if e != float('inf')]
                    mean_rel = sum(flat_errors) / len(flat_errors) if flat_errors else 0.0

                    m_vs = re.match(r'mcols(\d+)_error([\d.]+)_run\d+', key)
                    if m_vs:
                        mcols_val = int(m_vs.group(1))
                        error_val = float(m_vs.group(2))
                        n_total = sum(per_measure_total.values())
                        by_col_count: Dict[int, int] = {}
                        for v in v_list:
                            by_col_count[v['measure_col']] = by_col_count.get(v['measure_col'], 0) + 1
                        max_rate = 0.0
                        for col, col_total in per_measure_total.items():
                            if col_total > 0:
                                rate = by_col_count.get(col, 0) / col_total
                                max_rate = max(max_rate, rate)
                        vs_by_mcols_error.setdefault((mcols_val, error_val), []).append(
                            (classification, len(v_list), n_total, max_rate, mean_rel)
                        )

        total_systematic += vs_systematic

        # ----- Part B.2: PilotDB error-bound validation -----

        print("\n" + "-" * 70)
        print("PART B.2: PilotDB approximate SUM vs DuckDB table (error-bound check)")
        print("-" * 70)

        for ds_key, ds_info in DATASETS.items():
            for sc_key, sc_info in ds_info['scenarios'].items():
                scenario_id = f"{ds_key}/{sc_key}"
                if args.scenario and args.scenario != scenario_id:
                    continue

                results_dir = (script_dir / "plotting" / sc_info['dir']).resolve()

                print(f"\n{'-'*60}")
                print(f"Scenario: {ds_info['name']} / {sc_info['description']}")
                print(f"Directory: {results_dir}")
                print(f"{'-'*60}")

                pdb_violations = validate_pilotdb_for_scenario(
                    str(results_dir),
                    scenario_label=f"{ds_info['label']}-{sc_key}",
                    selected_error_bounds=args.error_bounds,
                    selected_measure_cols=args.mcols,
                    verbose=not args.quiet,
                )

                for key, (v_list, per_measure_total, all_rel_errors) in pdb_violations.items():
                    classification = classify_violations(v_list, per_measure_total)
                    if classification == 'pass':
                        pdb_pass += 1
                    elif classification == 'statistical':
                        pdb_statistical += 1
                    else:
                        pdb_systematic += 1
                        if args.quiet:
                            print(f"  ✗ {scenario_id} / {key}: {len(v_list)} violations")

                    flat_errors = [e for errs in all_rel_errors.values()
                                   for e in errs if e != float('inf')]
                    mean_rel = sum(flat_errors) / len(flat_errors) if flat_errors else 0.0

                    m_key = re.match(r'mcols(\d+)_error([\d.]+)_run\d+', key)
                    if m_key:
                        mcols_val = int(m_key.group(1))
                        error_val = float(m_key.group(2))
                        n_total = sum(per_measure_total.values())
                        by_col_count: Dict[int, int] = {}
                        for v in v_list:
                            by_col_count[v['measure_col']] = by_col_count.get(v['measure_col'], 0) + 1
                        max_rate = 0.0
                        for col, col_total in per_measure_total.items():
                            if col_total > 0:
                                rate = by_col_count.get(col, 0) / col_total
                                max_rate = max(max_rate, rate)
                        pdb_by_mcols_error.setdefault((mcols_val, error_val), []).append(
                            (classification, len(v_list), n_total, max_rate, mean_rel)
                        )

        total_systematic += pdb_systematic

    # =========================================================================
    # PART C: CROSS-VALIDATION
    # =========================================================================
    total_cross_checked = 0
    total_cross_diffs = 0

    if run_cross:
        print(f"\n\n{'='*70}")
        print("PART C: CROSS-VALIDATION — Valinor exact vs DuckDB table (all aggregates)")
        print(f"  Threshold: flag relative differences > {EXACT_REL_DIFF_THRESHOLD:.0e}")
        print(f"{'='*70}")

        for ds_key, ds_info in DATASETS.items():
            for sc_key, sc_info in ds_info['scenarios'].items():
                scenario_id = f"{ds_key}/{sc_key}"
                if args.scenario and args.scenario != scenario_id:
                    continue

                results_dir = (script_dir / "plotting" / sc_info['dir']).resolve()

                print(f"\n{'-'*60}")
                print(f"Scenario: {ds_info['name']} / {sc_info['description']}")
                print(f"Directory: {results_dir}")
                print(f"{'-'*60}")

                all_diffs = cross_validate_scenario(
                    str(results_dir),
                    scenario_label=f"{ds_info['label']}-{sc_key}",
                    selected_measure_cols=args.mcols,
                    verbose=not args.quiet,
                )

                for key, diff_list in all_diffs.items():
                    total_cross_checked += 1
                    if diff_list:
                        total_cross_diffs += 1

    # =========================================================================
    # CONSOLIDATED SUMMARIES
    # =========================================================================
    print(f"\n\n{'#'*70}")
    print(f"{'#'*70}")
    print(f"##  CONSOLIDATED SUMMARIES")
    print(f"{'#'*70}")
    print(f"{'#'*70}")

    # --- Bbox summary ---
    if run_bbox:
        print(f"\n{'='*70}")
        print(f"BBOX CONSISTENCY SUMMARY  ({total_bbox_checked} queries checked)")
        if total_bbox_mismatches == 0:
            print(f"  ✓ All bounding boxes are consistent across methods")
        else:
            print(f"  ✗ {total_bbox_mismatches} mismatches found")
        print(f"{'='*70}")

    # --- Valinor CI summary ---
    if run_ci:
        grand_total = total_pass + total_statistical + total_systematic - pdb_systematic - vs_systematic
        valinor_systematic = total_systematic - pdb_systematic - vs_systematic
        print(f"\n{'='*70}")
        print(f"VALINOR CI VALIDATION SUMMARY  ({grand_total} file pairs, ground truth: DuckDB table)")
        print(f"  ✓ {total_pass} fully passed (0 violations)")
        print(f"  ~ {total_statistical} within expected statistical noise "
              f"(violations ≤{EXPECTED_VIOLATION_RATE*100:.0f}% per measure)")
        print(f"  ✗ {valinor_systematic} EXCEED {EXPECTED_VIOLATION_RATE*100:.0f}% "
              f"— likely systematic bug")
        print(f"{'='*70}")

        if by_mcols_error:
            print(f"\nVALINOR BREAKDOWN BY (mcols, error_bound):")
            print(f"  {'mcols':>5}  {'error':>6}  {'runs':>4}  {'pass':>4}  "
                  f"{'noise':>5}  {'bug':>4}  {'avg_viol%':>9}  {'max_measure%':>12}  {'avg_rel_err':>12}")
            print(f"  {'-'*5}  {'-'*6}  {'-'*4}  {'-'*4}  {'-'*5}  {'-'*4}  "
                  f"{'-'*9}  {'-'*12}  {'-'*12}")

            for (mcols_val, error_val) in sorted(by_mcols_error.keys()):
                entries = by_mcols_error[(mcols_val, error_val)]
                n_runs = len(entries)
                n_pass = sum(1 for c, _, _, _, _ in entries if c == 'pass')
                n_stat = sum(1 for c, _, _, _, _ in entries if c == 'statistical')
                n_sys = sum(1 for c, _, _, _, _ in entries if c == 'systematic')
                total_v = sum(nv for _, nv, _, _, _ in entries)
                total_c = sum(nt for _, _, nt, _, _ in entries)
                avg_rate = 100.0 * total_v / total_c if total_c > 0 else 0.0
                worst_rate = 100.0 * max(mr for _, _, _, mr, _ in entries)
                mean_rel_err = sum(mre for _, _, _, _, mre in entries) / n_runs
                flag = " ⚠️" if n_sys > 0 else ""
                print(f"  {mcols_val:>5}  {error_val:>6.2f}  {n_runs:>4}  {n_pass:>4}  "
                      f"{n_stat:>5}  {n_sys:>4}  {avg_rate:>8.1f}%  {worst_rate:>11.1f}%"
                      f"  {mean_rel_err:>11.6f}{flag}")

            print(f"\n  A 95% CI should produce ~5% violations per measure.")
            print(f"  Rates up to ~{EXPECTED_VIOLATION_RATE*100:.0f}% are within "
                  f"normal sampling variance. ⚠️ flags rates above that.")

        # --- VALINOR-S summary ---
        vs_grand = vs_pass + vs_statistical + vs_systematic
        print(f"\n{'='*70}")
        print(f"VALINOR-S CI VALIDATION SUMMARY  ({vs_grand} file pairs, ground truth: DuckDB table)")
        print(f"  ✓ {vs_pass} fully passed (0 violations)")
        print(f"  ~ {vs_statistical} within expected statistical noise "
              f"(violations ≤{EXPECTED_VIOLATION_RATE*100:.0f}% per measure)")
        print(f"  ✗ {vs_systematic} EXCEED {EXPECTED_VIOLATION_RATE*100:.0f}% "
              f"— likely systematic bug")
        print(f"{'='*70}")

        if vs_by_mcols_error:
            print(f"\nVALINOR-S BREAKDOWN BY (mcols, error_bound):")
            print(f"  {'mcols':>5}  {'error':>6}  {'runs':>4}  {'pass':>4}  "
                  f"{'noise':>5}  {'bug':>4}  {'avg_viol%':>9}  {'max_measure%':>12}  {'avg_rel_err':>12}")
            print(f"  {'-'*5}  {'-'*6}  {'-'*4}  {'-'*4}  {'-'*5}  {'-'*4}  "
                  f"{'-'*9}  {'-'*12}  {'-'*12}")

            for (mcols_val, error_val) in sorted(vs_by_mcols_error.keys()):
                entries = vs_by_mcols_error[(mcols_val, error_val)]
                n_runs = len(entries)
                n_pass = sum(1 for c, _, _, _, _ in entries if c == 'pass')
                n_stat = sum(1 for c, _, _, _, _ in entries if c == 'statistical')
                n_sys = sum(1 for c, _, _, _, _ in entries if c == 'systematic')
                total_v = sum(nv for _, nv, _, _, _ in entries)
                total_c = sum(nt for _, _, nt, _, _ in entries)
                avg_rate = 100.0 * total_v / total_c if total_c > 0 else 0.0
                worst_rate = 100.0 * max(mr for _, _, _, mr, _ in entries)
                mean_rel_err = sum(mre for _, _, _, _, mre in entries) / n_runs
                flag = " ⚠️" if n_sys > 0 else ""
                print(f"  {mcols_val:>5}  {error_val:>6.2f}  {n_runs:>4}  {n_pass:>4}  "
                      f"{n_stat:>5}  {n_sys:>4}  {avg_rate:>8.1f}%  {worst_rate:>11.1f}%"
                      f"  {mean_rel_err:>11.6f}{flag}")

            print(f"\n  A 95% CI should produce ~5% violations per measure.")
            print(f"  Rates up to ~{EXPECTED_VIOLATION_RATE*100:.0f}% are within "
                  f"normal sampling variance. ⚠️ flags rates above that.")

        # --- PilotDB summary ---
        pdb_grand = pdb_pass + pdb_statistical + pdb_systematic
        print(f"\n{'='*70}")
        print(f"PILOTDB VALIDATION SUMMARY  ({pdb_grand} file pairs, ground truth: DuckDB table)")
        print(f"  ✓ {pdb_pass} fully passed (0 violations)")
        print(f"  ~ {pdb_statistical} within expected statistical noise "
              f"(violations ≤{EXPECTED_VIOLATION_RATE*100:.0f}% per measure)")
        print(f"  ✗ {pdb_systematic} EXCEED {EXPECTED_VIOLATION_RATE*100:.0f}% "
              f"— error exceeds claimed bound")
        print(f"{'='*70}")

        if pdb_by_mcols_error:
            print(f"\nPILOTDB BREAKDOWN BY (mcols, error_bound):")
            print(f"  {'mcols':>5}  {'error':>6}  {'runs':>4}  {'pass':>4}  "
                  f"{'noise':>5}  {'bug':>4}  {'avg_viol%':>9}  {'max_measure%':>12}  {'avg_rel_err':>12}")
            print(f"  {'-'*5}  {'-'*6}  {'-'*4}  {'-'*4}  {'-'*5}  {'-'*4}  "
                  f"{'-'*9}  {'-'*12}  {'-'*12}")

            for (mcols_val, error_val) in sorted(pdb_by_mcols_error.keys()):
                entries = pdb_by_mcols_error[(mcols_val, error_val)]
                n_runs = len(entries)
                n_pass = sum(1 for c, _, _, _, _ in entries if c == 'pass')
                n_stat = sum(1 for c, _, _, _, _ in entries if c == 'statistical')
                n_sys = sum(1 for c, _, _, _, _ in entries if c == 'systematic')
                total_v = sum(nv for _, nv, _, _, _ in entries)
                total_c = sum(nt for _, _, nt, _, _ in entries)
                avg_rate = 100.0 * total_v / total_c if total_c > 0 else 0.0
                worst_rate = 100.0 * max(mr for _, _, _, mr, _ in entries)
                mean_rel_err = sum(mre for _, _, _, _, mre in entries) / n_runs
                flag = " ⚠️" if n_sys > 0 else ""
                print(f"  {mcols_val:>5}  {error_val:>6.2f}  {n_runs:>4}  {n_pass:>4}  "
                      f"{n_stat:>5}  {n_sys:>4}  {avg_rate:>8.1f}%  {worst_rate:>11.1f}%"
                      f"  {mean_rel_err:>11.6f}{flag}")

    # --- Cross-validation summary ---
    if run_cross:
        print(f"\n{'='*70}")
        print(f"CROSS-VALIDATION SUMMARY  ({total_cross_checked} file pairs)")
        if total_cross_diffs == 0:
            print(f"  ✓ All Valinor exact results match DuckDB table "
                  f"(within {EXACT_REL_DIFF_THRESHOLD:.0e} relative tolerance)")
        else:
            print(f"  ⚠ {total_cross_diffs}/{total_cross_checked} file pairs "
                  f"have differences > {EXACT_REL_DIFF_THRESHOLD:.0e}")
        print(f"{'='*70}")

    has_failures = total_systematic > 0 or not bbox_ok
    return 1 if has_failures else 0


if __name__ == '__main__':
    sys.exit(main())
