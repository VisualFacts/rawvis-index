#!/usr/bin/env python3
"""
Validate experiment results: assess approximation accuracy across all methods.

Expected directory layout::

    <results_dir>/
        <scenario>/
            valinor_a/  results_mcols{M}_error{E}_res{N}_str{S}_n{Q}[_outK{K}]_run{R}.csv
            valinor_s/  results_mcols{M}_error{E}_res{N}_str{S}_n{Q}[_outK{K}]_run{R}.csv
            duckdb/tableProjected/  results_mcols{M}_n{Q}_run{R}.csv
            pilotdb/    results_mcols{M}_error{E}_n{Q}_run{R}.csv

Validation tasks
----------------
A. Bbox consistency
   For each (scenario, mcols, query_idx), every method must have queried the
   same bounding box. Valinor records the bbox directly in the ``bbox`` column;
   DuckDB and PilotDB carry it inside the SQL ``Query`` column.

B. Approximation accuracy (the primary objective)
    Ground truth: DuckDB tableProjected when present, otherwise Valinor-A
    exact (error=0). DuckDB may cover a shorter prefix; this is valid.
   - Valinor-A / Valinor-S (CIs ``{col={sum=[lb,ub], count=[lb,ub], mean=[lb,ub]}}``):
       * coverage  - fraction of true values inside CI (target ~= 95%)
       * tightness - half-width / |true value|, expected to satisfy errorBound when converged
       * mid-point relative error
   - PilotDB (point estimates ``{col=Stats{sum=v, avg=v}}``):
       * within-bound rate - fraction of queries with rel_error <= errorBound
       * mean / max relative error

C. Exact equivalence
    When DuckDB is present, Valinor-A with error=0 must match DuckDB on every
    aggregate (count, sum, mean) up to floating-point tolerance.

Usage
-----
    python3 analysis/validate_experiment_results.py <results_dir>
    python3 analysis/validate_experiment_results.py <results_dir> --scenario synth10_100M_pan_sel1
    python3 analysis/validate_experiment_results.py <results_dir> --task accuracy
    python3 analysis/validate_experiment_results.py <results_dir> --quiet

Exit code: 0 = all checks passed, 1 = systematic failures detected.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


# =============================================================================
# Constants
# =============================================================================

AGG_NAMES = ('sum', 'count', 'mean')

# Floating-point tolerance for "exact" comparisons
EXACT_REL_TOL = 1e-6

# Valinor's `errorBound` parameter is the *relative half-width budget* of the
# CI (i.e. (ub - lb) / (2 * |estimate|) <= errorBound), evaluated at a fixed
# confidence level (95% by default in the engine). It is NOT the CI's alpha.
# Therefore the expected coverage is ~95% regardless of the requested
# error-bound value.  We allow some finite-sample slack before flagging.
CI_CONFIDENCE = 0.95
MIN_COVERAGE = 0.90  # never accept lower than this, even for small samples

# PilotDB within-bound rate threshold below which we flag the configuration
PILOTDB_MIN_WITHIN_RATE = 0.90


# =============================================================================
# Filename parsing
# =============================================================================

_PARAM_RE = re.compile(r'(mcols|error|run)([\d.]+)')
_QUERY_COUNT_RE = re.compile(r'_n(\d+)(?:_|$)')


def _parse_params(stem: str) -> Optional[Tuple[int, float, int]]:
    """Extract (mcols, error, run) from a result filename stem.

    Tolerates extra parts like ``res500_str0.2`` between recognised keys.
    """
    found: Dict[str, float] = {}
    for key, val in _PARAM_RE.findall(stem):
        found[key] = int(val) if key in ('mcols', 'run') else float(val)
    if 'mcols' in found and 'error' in found and 'run' in found:
        return int(found['mcols']), found['error'], int(found['run'])
    return None


def _parse_duckdb_params(stem: str) -> Optional[Tuple[int, int]]:
    """Extract (mcols, run) from a DuckDB filename stem."""
    found: Dict[str, int] = {}
    for key, val in _PARAM_RE.findall(stem):
        if key in ('mcols', 'run'):
            found[key] = int(val)
    if 'mcols' in found and 'run' in found:
        return found['mcols'], found['run']
    return None


def _expected_query_count(stem: str) -> Optional[int]:
    match = _QUERY_COUNT_RE.search(stem)
    return int(match.group(1)) if match else None


def _is_current_result_file(path: Path) -> bool:
    return '_legacyApprox' not in path.name


# =============================================================================
# Bbox + result-string parsers
# =============================================================================

@dataclass(frozen=True)
class BBox:
    xmin: float
    xmax: float
    ymin: float
    ymax: float

    def approx_eq(self, other: 'BBox', tol: float = 1e-9) -> bool:
        # Two range-pairs are the same regardless of which range we call x and
        # which y — so accept both axis orderings.  This avoids false positives
        # when SQL columns are emitted in (lat, lon) by one method and
        # (lon, lat) by another.
        same = (abs(self.xmin - other.xmin) < tol
                and abs(self.xmax - other.xmax) < tol
                and abs(self.ymin - other.ymin) < tol
                and abs(self.ymax - other.ymax) < tol)
        if same:
            return True
        swapped = (abs(self.xmin - other.ymin) < tol
                   and abs(self.xmax - other.ymax) < tol
                   and abs(self.ymin - other.xmin) < tol
                   and abs(self.ymax - other.xmax) < tol)
        return swapped

    def __str__(self) -> str:
        return f"({self.xmin}..{self.xmax}, {self.ymin}..{self.ymax})"


_BBOX_VALINOR_RE = re.compile(
    r'\s*([-\d.eE+]+),([-\d.eE+]+),([-\d.eE+]+),([-\d.eE+]+)\s*'
)
_SQL_GT_RE = re.compile(
    r'(?:CAST\()?column(\d+)(?:\s+AS\s+FLOAT\))?\s*>\s*'
    r'(?:CAST\()?([-\d.eE+]+)(?:\s+AS\s+FLOAT\))?'
)
_SQL_LT_RE = re.compile(
    r'(?:CAST\()?column(\d+)(?:\s+AS\s+FLOAT\))?\s*<\s*'
    r'(?:CAST\()?([-\d.eE+]+)(?:\s+AS\s+FLOAT\))?'
)


def parse_bbox_valinor(s: str) -> Optional[BBox]:
    if not s or s == 'nan':
        return None
    m = _BBOX_VALINOR_RE.fullmatch(s)
    if not m:
        return None
    return BBox(float(m.group(1)), float(m.group(2)),
                float(m.group(3)), float(m.group(4)))


def parse_bbox_sql(s: str) -> Optional[BBox]:
    if not s or s == 'nan':
        return None
    gts: Dict[int, float] = {}
    lts: Dict[int, float] = {}
    for col, val in _SQL_GT_RE.findall(s):
        gts.setdefault(int(col), float(val))
    for col, val in _SQL_LT_RE.findall(s):
        lts.setdefault(int(col), float(val))
    cols = sorted(set(gts) & set(lts))
    if len(cols) < 2:
        return None
    cx, cy = cols[0], cols[1]
    return BBox(gts[cx], lts[cx], gts[cy], lts[cy])


@dataclass
class Stats:
    count: Optional[int] = None
    sum: Optional[float] = None
    mean: Optional[float] = None


def _parse_kv_block(body: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for k, v in re.findall(r'(\w+)=([-\d.eE+]+|NaN)', body):
        out[k] = None if v == 'NaN' else float(v)
    return out


def parse_duckdb_result(s: str) -> Dict[int, Stats]:
    """Parse ``{col=StatsDuckDB{count=, min=, max=, sum=, mean=, sumOfSquares=}}``."""
    if not s or s == 'nan':
        return {}
    out: Dict[int, Stats] = {}
    for m in re.finditer(r'(\d+)=StatsDuckDB\{([^}]+)\}', s):
        kv = _parse_kv_block(m.group(2))
        out[int(m.group(1))] = Stats(
            count=int(kv['count']) if kv.get('count') is not None else None,
            sum=kv.get('sum'),
            mean=kv.get('mean'),
        )
    return out


def parse_valinor_exact_result(s: str) -> Dict[int, Stats]:
    """Parse ``{col=Stats{count=, mean=, populationStandardDeviation=, min=, max=}}``.

    Sum is computed as count * mean (Valinor exact does not store it directly).
    """
    if not s or s == 'nan':
        return {}
    out: Dict[int, Stats] = {}
    for m in re.finditer(r'(\d+)=Stats\{([^}]+)\}', s):
        kv = _parse_kv_block(m.group(2))
        cnt = int(kv['count']) if kv.get('count') is not None else None
        mean = kv.get('mean')
        out[int(m.group(1))] = Stats(
            count=cnt, mean=mean,
            sum=(cnt * mean) if (cnt is not None and mean is not None) else None,
        )
    return out


def parse_pilotdb_result(s: str) -> Dict[int, Dict[str, float]]:
    """Parse ``{col=Stats{sum=, avg=}}`` -> ``{col: {'sum': v, 'mean': v}}``."""
    if not s or s == 'nan':
        return {}
    out: Dict[int, Dict[str, float]] = {}
    for m in re.finditer(r'(\d+)=Stats\{([^}]+)\}', s):
        kv = _parse_kv_block(m.group(2))
        col = int(m.group(1))
        ests: Dict[str, float] = {}
        if kv.get('sum') is not None:
            ests['sum'] = kv['sum']
        if kv.get('avg') is not None:
            ests['mean'] = kv['avg']
        out[col] = ests
    return out


def parse_valinor_ci(s: str) -> Dict[int, Dict[str, Tuple[float, float]]]:
    """Parse ``{col={sum=[lb,ub], count=[lb,ub], mean=[lb,ub]}}``."""
    if not s or s == 'nan' or '={' not in s:
        return {}
    out: Dict[int, Dict[str, Tuple[float, float]]] = {}
    for col_match in re.finditer(r'(\d+)=\{([^}]+)\}', s):
        col = int(col_match.group(1))
        body = col_match.group(2)
        cis: Dict[str, Tuple[float, float]] = {}
        for ag in re.finditer(r'(\w+)=\[([-\d.eE+]+),\s*([-\d.eE+]+)\]', body):
            cis[ag.group(1)] = (float(ag.group(2)), float(ag.group(3)))
        if cis:
            out[col] = cis
    return out


def parse_valinor_point_estimates(s: str) -> Dict[int, Dict[str, float]]:
    """Parse ``{col={sum=v, count=v, mean=v}}`` point estimates."""
    if not s or s == 'nan' or '={' not in s:
        return {}
    out: Dict[int, Dict[str, float]] = {}
    for col_match in re.finditer(r'(\d+)=\{([^}]+)\}', s):
        col = int(col_match.group(1))
        values: Dict[str, float] = {}
        for key, value in re.findall(r'(sum|count|mean)=([-\d.eE+]+|NaN)', col_match.group(2)):
            if value != 'NaN':
                values[key] = float(value)
        if values:
            out[col] = values
    return out


def parse_valinor_error_bounds(s: str) -> Dict[int, Dict[str, float]]:
    """Parse ``{col={sum=e, count=e, mean=e}}`` aggregate error bounds."""
    return parse_valinor_point_estimates(s)


# =============================================================================
# Discovery
# =============================================================================

@dataclass
class MethodFiles:
    valinor_a: List[Path] = field(default_factory=list)
    valinor_s: List[Path] = field(default_factory=list)
    duckdb: List[Path] = field(default_factory=list)
    pilotdb: List[Path] = field(default_factory=list)


def discover_scenarios(results_dir: Path) -> Dict[str, MethodFiles]:
    scenarios: Dict[str, MethodFiles] = {}
    for sc_dir in sorted(p for p in results_dir.iterdir() if p.is_dir()):
        mf = MethodFiles()
        if (sc_dir / 'valinor_a').is_dir():
            mf.valinor_a = sorted(f for f in (sc_dir / 'valinor_a').glob('results_*.csv') if _is_current_result_file(f))
        if (sc_dir / 'valinor_s').is_dir():
            mf.valinor_s = sorted(f for f in (sc_dir / 'valinor_s').glob('results_*.csv') if _is_current_result_file(f))
        ddb = sc_dir / 'duckdb' / 'tableProjected'
        if ddb.is_dir():
            mf.duckdb = sorted(f for f in ddb.glob('results_*.csv') if _is_current_result_file(f))
        if (sc_dir / 'pilotdb').is_dir():
            mf.pilotdb = sorted(f for f in (sc_dir / 'pilotdb').glob('results_*.csv') if _is_current_result_file(f))
        if any((mf.valinor_a, mf.valinor_s, mf.duckdb, mf.pilotdb)):
            scenarios[sc_dir.name] = mf
    return scenarios


def _safe_read(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        return None


# =============================================================================
# Ground-truth index
# =============================================================================

@dataclass
class TruthRow:
    bbox: Optional[BBox]
    stats: Dict[int, Stats]


def _load_duckdb_truth(duckdb_files: List[Path]) -> Dict[int, Dict[int, TruthRow]]:
    """Build ``{mcols: {query_idx: TruthRow}}`` from DuckDB tableProjected files."""
    by_mcols: Dict[int, Path] = {}
    for f in duckdb_files:
        params = _parse_duckdb_params(f.stem)
        if not params:
            continue
        mcols, run = params
        if mcols not in by_mcols or run == 1:
            by_mcols[mcols] = f

    truth: Dict[int, Dict[int, TruthRow]] = {}
    for mcols, path in by_mcols.items():
        df = _safe_read(path)
        if df is None:
            continue
        rows: Dict[int, TruthRow] = {}
        for _, r in df.iterrows():
            try:
                qi = int(r['i'])
            except (KeyError, ValueError):
                continue
            if qi == 0:
                continue  # init row
            bbox = parse_bbox_sql(str(r.get('Query', '')))
            stats = parse_duckdb_result(str(r.get('Query Result', '')))
            if stats:
                rows[qi] = TruthRow(bbox=bbox, stats=stats)
        if rows:
            truth[mcols] = rows
    return truth


def _load_valinor_exact_truth(valinor_files: List[Path]) -> Dict[int, Dict[int, TruthRow]]:
    """Build truth from Valinor-A exact files when DuckDB is absent or shorter."""
    by_mcols: Dict[int, Path] = {}
    for f in valinor_files:
        params = _parse_params(f.stem)
        if not params:
            continue
        mcols, error_bound, run = params
        if error_bound != 0:
            continue
        current = by_mcols.get(mcols)
        if current is None:
            by_mcols[mcols] = f
            continue
        current_n = _expected_query_count(current.stem) or -1
        candidate_n = _expected_query_count(f.stem) or -1
        current_run = _parse_params(current.stem)[2] if _parse_params(current.stem) else math.inf
        if run == 1 and current_run != 1:
            by_mcols[mcols] = f
        elif run == current_run and candidate_n > current_n:
            by_mcols[mcols] = f

    truth: Dict[int, Dict[int, TruthRow]] = {}
    for mcols, path in by_mcols.items():
        df = _safe_read(path)
        if df is None:
            continue
        rows: Dict[int, TruthRow] = {}
        for _, r in df.iterrows():
            try:
                qi = int(r['i'])
            except (KeyError, ValueError):
                continue
            bbox = parse_bbox_valinor(str(r.get('bbox', '')))
            stats = parse_valinor_exact_result(str(r.get('Query Result', '')))
            if stats:
                rows[qi] = TruthRow(bbox=bbox, stats=stats)
        if rows:
            truth[mcols] = rows
    return truth


def load_truth(mf: MethodFiles) -> Tuple[Dict[int, Dict[int, TruthRow]], str]:
    """Prefer DuckDB truth and fill missing query prefixes from Valinor exact."""
    valinor_truth = _load_valinor_exact_truth(mf.valinor_a)
    duckdb_truth = _load_duckdb_truth(mf.duckdb)
    truth: Dict[int, Dict[int, TruthRow]] = {m: dict(rows) for m, rows in valinor_truth.items()}
    for mcols, rows in duckdb_truth.items():
        truth.setdefault(mcols, {}).update(rows)
    if duckdb_truth and valinor_truth:
        source = 'DuckDB tableProjected with Valinor-A exact fallback'
    elif duckdb_truth:
        source = 'DuckDB tableProjected'
    elif valinor_truth:
        source = 'Valinor-A exact'
    else:
        source = ''
    return truth, source


# =============================================================================
# Validation primitives
# =============================================================================

@dataclass
class AccuracyReport:
    """Per-file CI accuracy metrics, broken down by aggregate."""
    n_queries: int = 0
    nonconverged_rows: int = 0
    n_per_agg: Dict[str, int] = field(default_factory=dict)
    n_inside: Dict[str, int] = field(default_factory=dict)
    rel_errors: Dict[str, List[float]] = field(default_factory=dict)
    half_widths_pct: Dict[str, List[float]] = field(default_factory=dict)
    reported_bounds: Dict[str, List[float]] = field(default_factory=dict)
    bound_violations: Dict[str, int] = field(default_factory=dict)
    misses: List[Tuple[int, int, str, float, float, float]] = field(default_factory=list)


def _rel_err(approx: float, true: float) -> float:
    if true == 0:
        return 0.0 if approx == 0 else math.inf
    return abs(approx - true) / abs(true)


def _truth_value(stats: Stats, agg: str) -> Optional[float]:
    if agg == 'sum':
        return stats.sum
    if agg == 'count':
        return float(stats.count) if stats.count is not None else None
    if agg == 'mean':
        return stats.mean
    return None


def _is_false(value: object) -> bool:
    return str(value).strip().lower() == 'false'


def evaluate_ci_file(csv_path: Path, truth: Dict[int, TruthRow], error_bound: float) -> AccuracyReport:
    """Compute CI coverage + tightness for a Valinor approximate file."""
    df = _safe_read(csv_path)
    rep = AccuracyReport()
    if df is None:
        return rep

    for _, r in df.iterrows():
        try:
            qi = int(r['i'])
        except (KeyError, ValueError):
            continue
        cis = parse_valinor_ci(str(r.get('Query Result', '')))
        if not cis:
            continue
        point_estimates = parse_valinor_point_estimates(str(r.get('Point Estimate', '')))
        reported_bounds = parse_valinor_error_bounds(str(r.get('Error Bound By Aggregate', '')))
        converged = not _is_false(r.get('Converged', 'true'))
        if not converged:
            rep.nonconverged_rows += 1
        truth_row = truth.get(qi)
        if truth_row is None:
            continue
        rep.n_queries += 1
        for col, agg_cis in cis.items():
            tstats = truth_row.stats.get(col)
            if tstats is None:
                continue
            for agg, (lb, ub) in agg_cis.items():
                tval = _truth_value(tstats, agg)
                if tval is None:
                    continue
                rep.n_per_agg[agg] = rep.n_per_agg.get(agg, 0) + 1

                mid = point_estimates.get(col, {}).get(agg, 0.5 * (lb + ub))
                rep.rel_errors.setdefault(agg, []).append(_rel_err(mid, tval))
                if abs(tval) > 0:
                    rep.half_widths_pct.setdefault(agg, []).append(
                        0.5 * (ub - lb) / abs(tval)
                    )
                reported = reported_bounds.get(col, {}).get(agg)
                if reported is not None and math.isfinite(reported):
                    rep.reported_bounds.setdefault(agg, []).append(reported)
                    if converged and reported > error_bound + 1e-9:
                        rep.bound_violations[agg] = rep.bound_violations.get(agg, 0) + 1

                # CI coverage with FP slack on point-estimate (zero-width) CIs
                width = ub - lb
                inside = lb <= tval <= ub
                if not inside and width == 0:
                    ref = max(abs(tval), abs(mid), 1e-15)
                    if abs(tval - mid) <= EXACT_REL_TOL * ref:
                        inside = True
                if inside:
                    rep.n_inside[agg] = rep.n_inside.get(agg, 0) + 1
                else:
                    rep.misses.append((qi, col, agg, tval, lb, ub))
    return rep


@dataclass
class PilotDBReport:
    n_queries: int = 0
    n_per_agg: Dict[str, int] = field(default_factory=dict)
    n_within: Dict[str, int] = field(default_factory=dict)
    rel_errors: Dict[str, List[float]] = field(default_factory=dict)
    over_bound: List[Tuple[int, int, str, float, float, float]] = field(default_factory=list)


def evaluate_pilotdb_file(
    csv_path: Path,
    truth: Dict[int, TruthRow],
    error_bound: float,
) -> PilotDBReport:
    df = _safe_read(csv_path)
    rep = PilotDBReport()
    if df is None:
        return rep
    for _, r in df.iterrows():
        try:
            qi = int(r['i'])
        except (KeyError, ValueError):
            continue
        if qi == 0:
            continue
        ests = parse_pilotdb_result(str(r.get('Query Result', '')))
        if not ests:
            continue
        truth_row = truth.get(qi)
        if truth_row is None:
            continue
        rep.n_queries += 1
        for col, col_ests in ests.items():
            tstats = truth_row.stats.get(col)
            if tstats is None:
                continue
            for agg, est in col_ests.items():
                tval = _truth_value(tstats, agg)
                if tval is None:
                    continue
                rep.n_per_agg[agg] = rep.n_per_agg.get(agg, 0) + 1
                rerr = _rel_err(est, tval)
                rep.rel_errors.setdefault(agg, []).append(rerr)
                if rerr <= error_bound:
                    rep.n_within[agg] = rep.n_within.get(agg, 0) + 1
                else:
                    rep.over_bound.append((qi, col, agg, tval, est, rerr))
    return rep


@dataclass
class ExactReport:
    n_compared: int = 0
    diffs: List[Tuple[int, int, str, float, float, float]] = field(default_factory=list)


def evaluate_exact_file(csv_path: Path, truth: Dict[int, TruthRow]) -> ExactReport:
    df = _safe_read(csv_path)
    rep = ExactReport()
    if df is None:
        return rep
    for _, r in df.iterrows():
        try:
            qi = int(r['i'])
        except (KeyError, ValueError):
            continue
        if qi == 0:
            continue
        v_stats = parse_valinor_exact_result(str(r.get('Query Result', '')))
        if not v_stats:
            continue
        truth_row = truth.get(qi)
        if truth_row is None:
            continue
        for col, vs in v_stats.items():
            ds = truth_row.stats.get(col)
            if ds is None:
                continue
            for agg in AGG_NAMES:
                vval = _truth_value(vs, agg)
                dval = _truth_value(ds, agg)
                if vval is None or dval is None:
                    continue
                rep.n_compared += 1
                rdiff = _rel_err(vval, dval)
                if rdiff > EXACT_REL_TOL:
                    rep.diffs.append((qi, col, agg, vval, dval, rdiff))
    return rep


# =============================================================================
# Bbox consistency
# =============================================================================

def _collect_bboxes(path: Path, source: str) -> Dict[int, BBox]:
    df = _safe_read(path)
    if df is None or 'i' not in df.columns:
        return {}
    if source == 'valinor':
        if 'bbox' not in df.columns:
            return {}
        col, parser = 'bbox', parse_bbox_valinor
    else:
        if 'Query' not in df.columns:
            return {}
        col, parser = 'Query', parse_bbox_sql
    out: Dict[int, BBox] = {}
    for _, r in df.iterrows():
        try:
            qi = int(r['i'])
        except (KeyError, ValueError):
            continue
        if qi == 0:
            continue
        bb = parser(str(r.get(col, '')))
        if bb is not None:
            out[qi] = bb
    return out


def check_bbox_consistency(mf: MethodFiles) -> Tuple[int, List[str]]:
    """Verify every method used the same bboxes per query_idx, grouped by mcols.

    Returns (queries_checked, mismatch_messages).
    """
    by_mcols: Dict[int, List[Tuple[str, Path, str]]] = {}

    def _add(file_list: List[Path], label: str, parse_fn, source: str) -> None:
        for f in file_list:
            params = parse_fn(f.stem)
            if not params:
                continue
            mcols = params[0]
            by_mcols.setdefault(mcols, []).append((f"{label}:{f.name}", f, source))

    _add(mf.valinor_a, 'valinor_a', _parse_params, 'valinor')
    _add(mf.valinor_s, 'valinor_s', _parse_params, 'valinor')
    _add(mf.pilotdb, 'pilotdb', _parse_params, 'sql')
    _add(mf.duckdb, 'duckdb', _parse_duckdb_params, 'sql')

    total_checked = 0
    msgs: List[str] = []
    for mcols, files in sorted(by_mcols.items()):
        if len(files) < 2:
            continue
        bboxes = {label: _collect_bboxes(path, source) for label, path, source in files}
        ref_label, ref_bb = max(bboxes.items(), key=lambda item: len(item[1]))
        for lbl, bbs in bboxes.items():
            if lbl == ref_label:
                continue
            common_qi = sorted(set(ref_bb) & set(bbs))
            total_checked += len(common_qi)
            for qi in common_qi:
                ref = ref_bb[qi]
                other = bbs[qi]
                if not ref.approx_eq(other):
                    msgs.append(f"  mcols={mcols} q={qi}: {lbl}={other} vs {ref_label}={ref}")
    return total_checked, msgs


def _all_files(mf: MethodFiles) -> List[Path]:
    return [*mf.valinor_a, *mf.valinor_s, *mf.duckdb, *mf.pilotdb]


def check_query_counts(mf: MethodFiles) -> Tuple[int, List[str]]:
    checked = 0
    msgs: List[str] = []
    for path in _all_files(mf):
        expected = _expected_query_count(path.stem)
        if expected is None:
            continue
        checked += 1
        df = _safe_read(path)
        actual = 0 if df is None else len(df.index)
        if actual != expected:
            msgs.append(f"  {path.name}: expected {expected} rows from filename, found {actual}")
    return checked, msgs


# =============================================================================
# Reporting helpers
# =============================================================================

def _fmt_pct(num: int, den: int) -> str:
    return f"{(100.0 * num / den):.1f}%" if den else "n/a"


def _stats_summary(values: List[float]) -> Tuple[float, float]:
    finite = [v for v in values if math.isfinite(v)]
    if not finite:
        return (math.nan, math.nan)
    return sum(finite) / len(finite), max(finite)


def _coverage_floor(n: int) -> float:
    if n <= 0:
        return MIN_COVERAGE
    se = math.sqrt(CI_CONFIDENCE * (1.0 - CI_CONFIDENCE) / n)
    return max(MIN_COVERAGE, CI_CONFIDENCE - 3.0 * se)


def _print_ci_report(label: str, rep: AccuracyReport, error_bound: float, *, verbose: bool) -> str:
    if rep.n_queries == 0:
        if verbose:
            print(f"    {label}: no comparable queries")
        return 'pass'
    # Coverage is governed by the CI confidence level (fixed, ~95%), NOT by
    # the requested error_bound (which is the half-width budget). The observed
    # coverage floor allows finite-sample variation but tightens as n grows.
    worst = 'pass'
    coverage_failed = False
    lines: List[str] = []
    if rep.nonconverged_rows:
        worst = 'fail'
        lines.append(f"      [x] {rep.nonconverged_rows} row(s) reported Converged=false")
    for agg in AGG_NAMES:
        n = rep.n_per_agg.get(agg, 0)
        if n == 0:
            continue
        inside = rep.n_inside.get(agg, 0)
        cov = inside / n
        mean_re, max_re = _stats_summary(rep.rel_errors.get(agg, []))
        mean_hw, max_hw = _stats_summary(rep.half_widths_pct.get(agg, []))
        mean_bound, max_bound = _stats_summary(rep.reported_bounds.get(agg, []))
        target_cov = _coverage_floor(n)
        cov_ok = cov >= target_cov
        bound_violations = rep.bound_violations.get(agg, 0)
        bound_ok = bound_violations == 0
        flags = []
        if not cov_ok:
            flags.append(f"cov<{target_cov:.0%}")
            coverage_failed = True
        if not bound_ok:
            flags.append(f"reported_bound>{error_bound:.0%}")
        if flags:
            marker, status = 'x', f"FAIL ({','.join(flags)})"
            worst = 'fail'
        else:
            marker, status = 'v', 'PASS'
        lines.append(
            f"      [{marker}] {agg:<5}  cov={_fmt_pct(inside, n):>6} ({inside}/{n})  "
            f"rel_err mean/max = {mean_re:.2%}/{max_re:.2%}  "
            f"half_width/true mean/max = {mean_hw:.2%}/{max_hw:.2%}  "
            f"reported_bound mean/max = {mean_bound:.2%}/{max_bound:.2%}  [{status}]"
        )
    if verbose or worst == 'fail':
        print(f"    {label}  (target coverage ~= {CI_CONFIDENCE:.0%}, reported bound <= {error_bound:.0%})")
        for ln in lines:
            print(ln)
        if coverage_failed and rep.misses:
            for qi, col, agg, tval, lb, ub in rep.misses[:3]:
                print(f"        miss q={qi} col={col} {agg}: true={tval:.6g}  CI=[{lb:.6g}, {ub:.6g}]")
            if len(rep.misses) > 3:
                print(f"        ... and {len(rep.misses) - 3} more misses")
    return worst


def _print_pilotdb_report(label: str, rep: PilotDBReport, error_bound: float, *, verbose: bool) -> str:
    if rep.n_queries == 0:
        if verbose:
            print(f"    {label}: no comparable queries")
        return 'pass'
    worst = 'pass'
    lines: List[str] = []
    for agg in ('sum', 'mean'):
        n = rep.n_per_agg.get(agg, 0)
        if n == 0:
            continue
        within = rep.n_within.get(agg, 0)
        rate = within / n
        mean_re, max_re = _stats_summary(rep.rel_errors.get(agg, []))
        if rate < PILOTDB_MIN_WITHIN_RATE:
            marker, status = 'x', 'FAIL'
            worst = 'fail'
        elif within < n:
            marker, status = '~', 'noise'
            if worst == 'pass':
                worst = 'noise'
        else:
            marker, status = 'v', 'PASS'
        lines.append(
            f"      [{marker}] {agg:<5}  within_bound={_fmt_pct(within, n):>6} ({within}/{n})  "
            f"rel_err mean/max = {mean_re:.2%}/{max_re:.2%}  [{status}]"
        )
    if verbose or worst == 'fail':
        print(f"    {label}  (bound={error_bound}, target within-rate >= {PILOTDB_MIN_WITHIN_RATE:.0%})")
        for ln in lines:
            print(ln)
        if worst == 'fail' and rep.over_bound:
            for qi, col, agg, tval, est, rerr in rep.over_bound[:3]:
                print(f"        over q={qi} col={col} {agg}: true={tval:.6g} est={est:.6g} rel_err={rerr:.2%}")
            if len(rep.over_bound) > 3:
                print(f"        ... and {len(rep.over_bound) - 3} more over-bound queries")
    return worst


def _print_exact_report(label: str, rep: ExactReport, *, verbose: bool) -> str:
    if rep.n_compared == 0:
        if verbose:
            print(f"    {label}: no comparable values")
        return 'pass'
    if not rep.diffs:
        if verbose:
            print(f"    [v] {label}  {rep.n_compared} aggregate values exactly match DuckDB")
        return 'pass'
    if verbose or rep.diffs:
        print(f"    [x] {label}  {len(rep.diffs)}/{rep.n_compared} aggregates differ > {EXACT_REL_TOL:.0e}")
        for qi, col, agg, vval, dval, rdiff in rep.diffs[:5]:
            print(f"        q={qi} col={col} {agg}: valinor={vval:.6g} duckdb={dval:.6g} rel_diff={rdiff:.2e}")
        if len(rep.diffs) > 5:
            print(f"        ... and {len(rep.diffs) - 5} more diffs")
    return 'fail'


# =============================================================================
# Per-scenario driver
# =============================================================================

@dataclass
class ScenarioOutcome:
    query_count_checked: int = 0
    query_count_mismatches: int = 0
    bbox_mismatches: int = 0
    bbox_checked: int = 0
    pass_count: int = 0
    noise_count: int = 0
    fail_count: int = 0
    skipped: int = 0


def validate_scenario(
    name: str,
    mf: MethodFiles,
    *,
    tasks: Iterable[str],
    verbose: bool,
) -> ScenarioOutcome:
    out = ScenarioOutcome()
    print(f"\n{'-' * 70}\nScenario: {name}\n{'-' * 70}")

    print("  [File query counts]")
    count_checked, count_msgs = check_query_counts(mf)
    out.query_count_checked = count_checked
    out.query_count_mismatches = len(count_msgs)
    if not count_msgs:
        print(f"    [v] {count_checked} files match their _n query-count suffix")
    else:
        print(f"    [x] {len(count_msgs)} row-count mismatches across {count_checked} files")
        for m in count_msgs[:10]:
            print(m)
        if len(count_msgs) > 10:
            print(f"      ... and {len(count_msgs) - 10} more")

    if 'bbox' in tasks:
        print("  [Bbox consistency]")
        checked, msgs = check_bbox_consistency(mf)
        out.bbox_checked = checked
        out.bbox_mismatches = len(msgs)
        if not msgs:
            print(f"    [v] {checked} queries consistent across methods")
        else:
            print(f"    [x] {len(msgs)} mismatches across {checked} compared queries")
            for m in msgs[:10]:
                print(m)
            if len(msgs) > 10:
                print(f"      ... and {len(msgs) - 10} more")

    needs_truth = bool(set(tasks) & {'accuracy', 'exact'})
    truth: Dict[int, Dict[int, TruthRow]] = {}
    truth_source = ''
    if needs_truth:
        truth, truth_source = load_truth(mf)
        if not truth:
            print("  [!] no exact ground truth available - accuracy/exact checks skipped")
            out.skipped += 1
            return out
        print(f"  [Ground truth] {truth_source}")

    def _bump(status: str) -> None:
        if status == 'pass':
            out.pass_count += 1
        elif status == 'noise':
            out.noise_count += 1
        else:
            out.fail_count += 1

    if 'accuracy' in tasks:
        print("  [Accuracy: Valinor-A]")
        for f in mf.valinor_a:
            params = _parse_params(f.stem)
            if not params or params[1] == 0:
                continue
            mcols, eb, _ = params
            t = truth.get(mcols)
            if t is None:
                if verbose:
                    print(f"    {f.name}: no DuckDB truth for mcols={mcols}")
                continue
            _bump(_print_ci_report(f.name, evaluate_ci_file(f, t, eb), eb, verbose=verbose))

        print("  [Accuracy: Valinor-S]")
        for f in mf.valinor_s:
            params = _parse_params(f.stem)
            if not params or params[1] == 0:
                continue
            mcols, eb, _ = params
            t = truth.get(mcols)
            if t is None:
                continue
            _bump(_print_ci_report(f.name, evaluate_ci_file(f, t, eb), eb, verbose=verbose))

        print("  [Accuracy: PilotDB]")
        for f in mf.pilotdb:
            params = _parse_params(f.stem)
            if not params or params[1] == 0:
                continue
            mcols, eb, _ = params
            t = truth.get(mcols)
            if t is None:
                continue
            _bump(_print_pilotdb_report(
                f.name, evaluate_pilotdb_file(f, t, eb), eb, verbose=verbose,
            ))

    if 'exact' in tasks:
        print("  [Exact equivalence: Valinor-A (error=0) vs DuckDB]")
        if not mf.duckdb:
            print("    (skipped: no DuckDB exact files; Valinor-A exact is being used as ground truth)")
            return out
        duckdb_truth = _load_duckdb_truth(mf.duckdb)
        any_exact = False
        for f in mf.valinor_a:
            params = _parse_params(f.stem)
            if not params or params[1] != 0:
                continue
            any_exact = True
            mcols = params[0]
            t = duckdb_truth.get(mcols)
            if t is None:
                if verbose:
                    print(f"    {f.name}: no DuckDB truth for mcols={mcols}")
                continue
            _bump(_print_exact_report(f.name, evaluate_exact_file(f, t), verbose=verbose))
        if not any_exact:
            print("    (no error=0 Valinor-A files found)")

    return out


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate experiment results. Assesses bbox consistency, "
            "approximation accuracy (CI coverage and PilotDB error-bound "
            "compliance), and exact-method equivalence."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s experiments/results/\n"
            "  %(prog)s experiments/results/ --scenario synth10_100M_pan_sel1\n"
            "  %(prog)s experiments/results/ --task accuracy\n"
            "  %(prog)s experiments/results/ --quiet\n"
        ),
    )
    _default_results = str(Path(__file__).parent.parent / "results")
    parser.add_argument('results_dir', type=str, nargs='?', default=_default_results,
                        help=f"Path to results/ root (default: {_default_results})")
    parser.add_argument('--scenario', type=str, default=None,
                        help="Run only this scenario (subdirectory name)")
    parser.add_argument('--task', choices=['all', 'bbox', 'accuracy', 'exact'],
                        default='all', help="Which validation task(s) to run")
    parser.add_argument('--quiet', action='store_true',
                        help="Print only failures and the final summary")
    args = parser.parse_args()

    results_dir = Path(args.results_dir).resolve()
    if not results_dir.is_dir():
        print(f"ERROR: results_dir not found: {results_dir}", file=sys.stderr)
        return 2

    tasks = {'bbox', 'accuracy', 'exact'} if args.task == 'all' else {args.task}

    scenarios = discover_scenarios(results_dir)
    if not scenarios:
        print(f"ERROR: no scenarios found under {results_dir}", file=sys.stderr)
        return 2
    if args.scenario:
        if args.scenario not in scenarios:
            print(
                f"ERROR: scenario '{args.scenario}' not found. "
                f"Available: {', '.join(sorted(scenarios))}",
                file=sys.stderr,
            )
            return 2
        scenarios = {args.scenario: scenarios[args.scenario]}

    print(f"Validating {len(scenarios)} scenario(s) under {results_dir}")
    print(f"Tasks: {', '.join(sorted(tasks))}")

    totals = ScenarioOutcome()
    for name, mf in scenarios.items():
        outcome = validate_scenario(name, mf, tasks=tasks, verbose=not args.quiet)
        totals.query_count_checked += outcome.query_count_checked
        totals.query_count_mismatches += outcome.query_count_mismatches
        totals.bbox_checked += outcome.bbox_checked
        totals.bbox_mismatches += outcome.bbox_mismatches
        totals.pass_count += outcome.pass_count
        totals.noise_count += outcome.noise_count
        totals.fail_count += outcome.fail_count
        totals.skipped += outcome.skipped

    print(f"\n{'=' * 70}\nSUMMARY\n{'=' * 70}")
    if totals.query_count_mismatches == 0:
        print(f"  [v] File query counts: {totals.query_count_checked} files match _n")
    else:
        print(f"  [x] File query counts: {totals.query_count_mismatches} mismatches "
              f"in {totals.query_count_checked} files")
    if 'bbox' in tasks:
        if totals.bbox_mismatches == 0:
            print(f"  [v] Bbox consistency: {totals.bbox_checked} queries, all consistent")
        else:
            print(f"  [x] Bbox consistency: {totals.bbox_mismatches} mismatches "
                  f"in {totals.bbox_checked} queries")
    if tasks & {'accuracy', 'exact'}:
        total_files = totals.pass_count + totals.noise_count + totals.fail_count
        print(f"  Files validated: {total_files}")
        print(f"    [v] pass:  {totals.pass_count}")
        print(f"    [~] noise: {totals.noise_count}  (within expected statistical variance)")
        print(f"    [x] fail:  {totals.fail_count}")
        if totals.skipped:
            print(f"    skipped: {totals.skipped} scenario(s) without exact truth")

    has_failure = totals.fail_count > 0 or totals.bbox_mismatches > 0 or totals.query_count_mismatches > 0
    return 1 if has_failure else 0


if __name__ == '__main__':
    sys.exit(main())
