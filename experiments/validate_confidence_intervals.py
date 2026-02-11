#!/usr/bin/env python3
"""
Validate approximate query results against exact baselines.

For each scenario/mcols/error/run combination, checks which queries have
exact SUM values falling outside the reported confidence intervals.

Uses scenarios defined in plotting/plot_config.py.
"""

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd

# Add plotting dir to path so we can import plot_config
sys.path.insert(0, str(Path(__file__).parent / "plotting"))
from plot_config import DATASETS, SELECTED_ERROR_BOUNDS, SELECTED_MEASURE_COLS


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


def find_exact_file(results_dir: Path, mcols: int, run: int) -> Optional[Path]:
    """Find the exact (error=0) file for a given mcols/run."""
    candidates = [
        results_dir / f"results_mcols{mcols}_error0_run{run}.csv",
        results_dir / f"results_mcols{mcols}_error0.0_run{run}.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


# =============================================================================
# VALIDATION LOGIC
# =============================================================================

def validate_one_pair(
    exact_path: Path,
    approx_path: Path,
) -> Tuple[List[dict], Dict[int, int]]:
    """
    Compare exact sums vs approximate CIs for one file pair.
    
    Returns:
      violations: list of dicts with query_idx, measure_col, exact_sum, ci_lower, ci_upper, rel_error
      per_measure_total: dict mapping measure_col -> total number of CIs computed for that column
    """
    exact_df = pd.read_csv(exact_path)
    approx_df = pd.read_csv(approx_path)

    violations = []
    per_measure_total: Dict[int, int] = {}  # measure_col -> count of queries with a CI

    for _, approx_row in approx_df.iterrows():
        qi = approx_row['i']
        ci_dict = parse_ci_dict(str(approx_row.get('Confidence Interval', '')))
        if not ci_dict:
            continue  # init query or missing CI

        # Find matching exact row
        exact_match = exact_df[exact_df['i'] == qi]
        if exact_match.empty:
            continue
        exact_row = exact_match.iloc[0]
        sum_dict = parse_sum_dict(str(exact_row.get('Query Result Sum', '')))
        if not sum_dict:
            continue

        for col, (lb, ub) in ci_dict.items():
            exact_sum = sum_dict.get(col)
            if exact_sum is None:
                continue

            per_measure_total[col] = per_measure_total.get(col, 0) + 1

            if exact_sum < lb or exact_sum > ub:
                # Compute relative error of midpoint vs exact
                midpoint = (lb + ub) / 2.0
                if exact_sum != 0:
                    rel_err = abs(midpoint - exact_sum) / abs(exact_sum)
                else:
                    rel_err = float('inf') if midpoint != 0 else 0.0

                violations.append({
                    'query_idx': qi,
                    'measure_col': col,
                    'exact_sum': exact_sum,
                    'ci_lower': lb,
                    'ci_upper': ub,
                    'rel_error': rel_err,
                })

    return violations, per_measure_total


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


def validate_scenario(
    results_dir: str,
    scenario_label: str,
    selected_error_bounds: List[float] = None,
    selected_measure_cols: List[int] = None,
    verbose: bool = True,
) -> Dict[str, Tuple[List[dict], Dict[int, int]]]:
    """
    Validate all approximate files in a scenario directory.
    
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

    # Discover all approximate files
    for csv_file in sorted(dir_path.glob("results_mcols*_error*_run*.csv")):
        parsed = parse_filename(csv_file.stem)
        if parsed is None:
            continue
        mcols, error, run = parsed

        # Skip exact files and files not in our selection
        if error == 0 or error not in selected_error_bounds:
            continue
        if mcols not in selected_measure_cols:
            continue

        # Find corresponding exact file
        exact_path = find_exact_file(dir_path, mcols, run)
        if exact_path is None:
            if verbose:
                print(f"  WARNING: No exact file for mcols={mcols} run={run}")
            continue

        # Validate
        violations, per_measure_total = validate_one_pair(exact_path, csv_file)
        key = f"mcols{mcols}_error{error}_run{run}"
        all_violations[key] = (violations, per_measure_total)

        total_comparisons = sum(per_measure_total.values())
        n_violations = len(violations)
        classification = classify_violations(violations, per_measure_total)

        if verbose:
            # Group violations by measure col
            by_col: Dict[int, List[dict]] = {}
            for v in violations:
                by_col.setdefault(v['measure_col'], []).append(v)

            # Determine per-measure violation rates
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

            print(f"  {marker} {key}: {n_violations}/{total_comparisons} violations  [{status}]")

            for col, n_fail, n_total, pct, exceeds, col_violations in col_summaries:
                flag = f" ⚠️  EXCEEDS {EXPECTED_VIOLATION_RATE*100:.0f}%" if exceeds else ""
                if n_fail == 0:
                    print(f"      col {col}: 0/{n_total} (0.0%) violations")
                    continue
                max_err = max(v['rel_error'] for v in col_violations)
                query_idxs = sorted(v['query_idx'] for v in col_violations)
                if len(query_idxs) <= 10:
                    idx_str = str(query_idxs)
                else:
                    idx_str = f"{query_idxs[:5]}...{query_idxs[-3:]} ({len(query_idxs)} total)"
                print(f"      col {col}: {n_fail}/{n_total} ({pct:.1f}%) violations, "
                      f"max_rel_error={max_err:.4f}, queries={idx_str}{flag}")

    return all_violations


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Validate approximate CIs against exact results")
    parser.add_argument('--scenario', type=str, default=None,
                        help="Run only this dataset/scenario (e.g. 'taxi/zoom'). Default: all.")
    parser.add_argument('--quiet', action='store_true',
                        help="Only print failures.")
    parser.add_argument('--error-bounds', type=float, nargs='+', default=None,
                        help="Error bounds to check (default: all non-zero from config)")
    parser.add_argument('--mcols', type=int, nargs='+', default=None,
                        help="Measure column counts to check (default: all from config)")
    args = parser.parse_args()

    # Resolve results base dir (script is in experiments/)
    script_dir = Path(__file__).parent
    results_base = script_dir / "results"

    total_pass = 0
    total_statistical = 0
    total_systematic = 0

    # Track per (mcols, error_bound) across all scenarios
    # key: (mcols, error_bound) -> list of (classification, n_violations, n_total, max_violation_rate)
    by_mcols_error: Dict[Tuple[int, float], List[Tuple[str, int, int, float]]] = {}

    for ds_key, ds_info in DATASETS.items():
        for sc_key, sc_info in ds_info['scenarios'].items():
            scenario_id = f"{ds_key}/{sc_key}"

            # Filter if --scenario given
            if args.scenario and args.scenario != scenario_id:
                continue

            # Resolve directory (sc_info['dir'] is relative to plotting/)
            results_dir = (script_dir / "plotting" / sc_info['dir']).resolve()

            print(f"\n{'='*60}")
            print(f"Scenario: {ds_info['name']} / {sc_info['description']}")
            print(f"Directory: {results_dir}")
            print(f"{'='*60}")

            violations = validate_scenario(
                str(results_dir),
                scenario_label=f"{ds_info['label']}-{sc_key}",
                selected_error_bounds=args.error_bounds,
                selected_measure_cols=args.mcols,
                verbose=not args.quiet,
            )

            for key, (v_list, per_measure_total) in violations.items():
                classification = classify_violations(v_list, per_measure_total)
                if classification == 'pass':
                    total_pass += 1
                elif classification == 'statistical':
                    total_statistical += 1
                else:
                    total_systematic += 1
                    if args.quiet:
                        print(f"  ✗ {scenario_id} / {key}: {len(v_list)} violations")

                # Parse mcols and error from key like "mcols4_error0.05_run1"
                import re
                m = re.match(r'mcols(\d+)_error([\d.]+)_run\d+', key)
                if m:
                    mcols_val = int(m.group(1))
                    error_val = float(m.group(2))
                    n_total = sum(per_measure_total.values())
                    # Max per-measure violation rate
                    by_col: Dict[int, int] = {}
                    for v in v_list:
                        by_col[v['measure_col']] = by_col.get(v['measure_col'], 0) + 1
                    max_rate = 0.0
                    for col, col_total in per_measure_total.items():
                        if col_total > 0:
                            rate = by_col.get(col, 0) / col_total
                            max_rate = max(max_rate, rate)
                    by_mcols_error.setdefault((mcols_val, error_val), []).append(
                        (classification, len(v_list), n_total, max_rate)
                    )

    # =========================================================================
    # OVERALL SUMMARY
    # =========================================================================
    grand_total = total_pass + total_statistical + total_systematic
    print(f"\n{'='*70}")
    print(f"OVERALL SUMMARY  ({grand_total} file pairs total)")
    print(f"  ✓ {total_pass} fully passed (0 violations)")
    print(f"  ~ {total_statistical} within expected statistical noise (violations ≤{EXPECTED_VIOLATION_RATE*100:.0f}% per measure)")
    print(f"  ✗ {total_systematic} EXCEED {EXPECTED_VIOLATION_RATE*100:.0f}% — likely systematic bug")
    print(f"{'='*70}")

    # =========================================================================
    # BREAKDOWN BY (mcols, error_bound)
    # =========================================================================
    if by_mcols_error:
        print(f"\nBREAKDOWN BY (mcols, error_bound):")
        print(f"  {'mcols':>5}  {'error':>6}  {'runs':>4}  {'pass':>4}  {'noise':>5}  {'bug':>4}  {'avg_viol%':>9}  {'max_measure%':>12}")
        print(f"  {'-'*5}  {'-'*6}  {'-'*4}  {'-'*4}  {'-'*5}  {'-'*4}  {'-'*9}  {'-'*12}")

        # Sort by mcols then error
        for (mcols_val, error_val) in sorted(by_mcols_error.keys()):
            entries = by_mcols_error[(mcols_val, error_val)]
            n_runs = len(entries)
            n_pass = sum(1 for c, _, _, _ in entries if c == 'pass')
            n_stat = sum(1 for c, _, _, _ in entries if c == 'statistical')
            n_sys = sum(1 for c, _, _, _ in entries if c == 'systematic')

            # Average violation rate across runs
            total_v = sum(nv for _, nv, _, _ in entries)
            total_c = sum(nt for _, _, nt, _ in entries)
            avg_rate = 100.0 * total_v / total_c if total_c > 0 else 0.0

            # Worst per-measure violation rate across all runs
            worst_rate = 100.0 * max(mr for _, _, _, mr in entries)

            flag = " ⚠️" if n_sys > 0 else ""
            print(f"  {mcols_val:>5}  {error_val:>6.2f}  {n_runs:>4}  {n_pass:>4}  {n_stat:>5}  {n_sys:>4}  {avg_rate:>8.1f}%  {worst_rate:>11.1f}%{flag}")

        print(f"\n  Column definitions:")
        print(f"    mcols        — number of measure columns used")
        print(f"    error        — sampling error bound (ε)")
        print(f"    runs         — number of repeat runs (each compared against the exact baseline)")
        print(f"    pass         — runs with 0 CI violations")
        print(f"    noise        — runs with violations within expected statistical noise (≤{EXPECTED_VIOLATION_RATE*100:.0f}% per measure)")
        print(f"    bug          — runs where ≥1 measure exceeds {EXPECTED_VIOLATION_RATE*100:.0f}% violation rate (likely systematic bug)")
        print(f"    avg_viol%    — overall violation rate across all runs (total violations / total comparisons)")
        print(f"    max_measure% — worst single-measure violation rate observed in any run")
        print(f"  A 95% CI should produce ~5% violations per measure. With {99} queries per measure,")
        print(f"  rates up to ~{EXPECTED_VIOLATION_RATE*100:.0f}% are within normal sampling variance (~2σ). ⚠️ flags rates above that.")

    return 1 if total_systematic > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
