"""Load experiment result CSVs into tidy DataFrames.

Two output frames per call to :func:`load`:

* ``queries``  — one row per (scenario, method, mcols, error_bound, run, q)
* ``measures`` — one row per (scenario, method, mcols, error_bound, run, q, measure_id)

`q` follows the CSV row index ``i``. Row ``q=0`` is the cold query and includes
initialization work; rows ``q>=1`` are warm queries. The effective query count is
read from the filename suffix ``_n<N>`` when present.

`query_time` (in the ``queries`` frame) excludes init for every method.

Results are cached to a Parquet file under ``experiments/.plot_cache/`` keyed by the
mtimes of every CSV that contributes; the cache is invalidated automatically.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from . import config

# ---------------------------------------------------------------------------
# Filename patterns
# ---------------------------------------------------------------------------
_RE_VALINOR = re.compile(
    r"^results_mcols(\d+)_error([\d.]+)_res\d+_str[\d.]+"
    r"(?:_n(\d+))?(?:_outK\d+)?_run(\d+)\.csv$"
)
_RE_PILOTDB = re.compile(r"^results_mcols(\d+)_error([\d.]+)(?:_n(\d+))?_run(\d+)\.csv$")
_RE_DUCKDB  = re.compile(r"^results_mcols(\d+)(?:_n(\d+))?_run(\d+)\.csv$")


@dataclass(frozen=True)
class _FileMeta:
    path: Path
    scenario: str
    method: str
    mcols: int
    error_bound: float   # 0.0 for duckdb
    run: int
    query_count: Optional[int]


def _discover(scenarios: Optional[Iterable[str]] = None) -> list[_FileMeta]:
    out: list[_FileMeta] = []
    scen_filter = set(scenarios) if scenarios else None
    for sc_dir in sorted(config.RESULTS_DIR.iterdir()):
        if not sc_dir.is_dir() or sc_dir.name.startswith("."):
            continue
        if scen_filter and sc_dir.name not in scen_filter:
            continue
        for mkey, m in config.METHODS.items():
            if mkey == "valinor_exact":
                continue  # synthesized later from valinor_a error=0
            mdir = sc_dir / m.subdir
            if not mdir.is_dir():
                continue
            for f in sorted(mdir.glob("*.csv")):
                meta = _parse_filename(f, sc_dir.name, mkey)
                if meta is not None:
                    out.append(meta)
    return out


def _parse_filename(path: Path, scenario: str, method: str) -> Optional[_FileMeta]:
    name = path.name
    if method in ("valinor_a", "valinor_s"):
        m = _RE_VALINOR.match(name)
        if not m:
            return None
        return _FileMeta(path, scenario, method, int(m.group(1)), float(m.group(2)), int(m.group(4)), _opt_int(m.group(3)))
    if method == "pilotdb":
        m = _RE_PILOTDB.match(name)
        if not m:
            return None
        return _FileMeta(path, scenario, method, int(m.group(1)), float(m.group(2)), int(m.group(4)), _opt_int(m.group(3)))
    if method == "duckdb_projected":
        m = _RE_DUCKDB.match(name)
        if not m:
            return None
        return _FileMeta(path, scenario, method, int(m.group(1)), 0.0, int(m.group(3)), _opt_int(m.group(2)))
    return None


def _opt_int(value: str | None) -> Optional[int]:
    return int(value) if value is not None else None


# ---------------------------------------------------------------------------
# CSV cleaning (multi-line error rows)
# ---------------------------------------------------------------------------
def _clean(content: str) -> str:
    lines = content.split("\n")
    if not lines:
        return content
    out = [lines[0]]
    for ln in lines[1:]:
        if ln.strip() == "":
            continue
        if ln.startswith("/") or ln.startswith("ERROR") or ln.startswith('"ERROR'):
            out.append(ln)
        elif out:
            out[-1] += " " + ln
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------
_RE_INIT_KV = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^,}]+)")

def _parse_init_timing(s: str) -> dict[str, float]:
    if not s or pd.isna(s):
        return {}
    s = str(s).strip().strip('"').strip("{}")
    out: dict[str, float] = {}
    for k, v in _RE_INIT_KV.findall(s):
        try:
            out[k] = float(v.strip())
        except ValueError:
            pass
    return out


# Valinor CI:  {2={sum=[lb, ub], count=[..], mean=[lb, ub]}, 3=...}
_RE_VALINOR_CI_MEASURE = re.compile(
    r"(\d+)=\{[^}]*sum=\[([^,\]]+),\s*([^\]]+)\][^}]*mean=\[([^,\]]+),\s*([^\]]+)\][^}]*\}"
)
# Valinor point estimate: {2={sum=..., count=..., mean=...}, 3=...}
_RE_VALINOR_POINT_MEASURE = re.compile(
    r"(\d+)=\{[^}]*sum=([^,}]+)[^}]*mean=([^,}]+)[^}]*\}"
)
# Valinor exact: {2=Stats{count=..., mean=..., populationStandardDeviation=..., ...}}
_RE_VALINOR_EXACT_MEASURE = re.compile(
    r"(\d+)=Stats\{[^}]*count=([^,}]+)[^}]*mean=([^,}]+)[^}]*\}"
)
# DuckDB: {2=StatsDuckDB{count=3001251, min=.., max=.., sum=1.498e9, mean=499.44, sumOfSquares=NaN}}
_RE_DUCKDB_MEASURE = re.compile(
    r"(\d+)=StatsDuckDB\{[^}]*sum=([^,}]+)[^}]*mean=([^,}]+)[^}]*\}"
)
# PilotDB: {2=Stats{sum=1498948911.0, avg=499.44}}
_RE_PILOTDB_MEASURE = re.compile(
    r"(\d+)=Stats\{[^}]*sum=([^,}]+)[^}]*avg=([^,}]+)[^}]*\}"
)


def _parse_query_result(s: str, method: str) -> dict[int, dict[str, float]]:
    """Return {measure_id: {'sum': float, 'avg': float}} (point estimates)."""
    if not s or pd.isna(s) or s == "{}":
        return {}
    s = str(s)
    out: dict[int, dict[str, float]] = {}
    if method in ("valinor_a", "valinor_s"):
        for mid, sval, mval in _RE_VALINOR_POINT_MEASURE.findall(s):
            try:
                out[int(mid)] = {"sum": float(sval), "avg": float(mval)}
            except ValueError:
                pass
        if out:
            return out
        for mid, cnt, mval in _RE_VALINOR_EXACT_MEASURE.findall(s):
            try:
                count = float(cnt)
                mean = float(mval)
                out[int(mid)] = {"sum": count * mean, "avg": mean}
            except ValueError:
                pass
        if out:
            return out
        for mid, slo, shi, mlo, mhi in _RE_VALINOR_CI_MEASURE.findall(s):
            try:
                out[int(mid)] = {
                    "sum": (float(slo) + float(shi)) / 2.0,
                    "avg": (float(mlo) + float(mhi)) / 2.0,
                }
            except ValueError:
                pass
        return out
    if method == "duckdb_projected":
        for mid, sval, mval in _RE_DUCKDB_MEASURE.findall(s):
            try:
                out[int(mid)] = {"sum": float(sval), "avg": float(mval)}
            except ValueError:
                pass
        return out
    if method == "pilotdb":
        for mid, sval, mval in _RE_PILOTDB_MEASURE.findall(s):
            try:
                out[int(mid)] = {"sum": float(sval), "avg": float(mval)}
            except ValueError:
                pass
        return out
    return {}


# ---------------------------------------------------------------------------
# Per-file parsing
# ---------------------------------------------------------------------------
def _parse_file(meta: _FileMeta) -> tuple[list[dict], list[dict]]:
    try:
        raw = meta.path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return [], []
    reader = csv.DictReader(io.StringIO(_clean(raw)))

    qrows: list[dict] = []
    mrows: list[dict] = []

    init_time = float("nan")
    init_table_create_time = float("nan")
    q0_eval_time = float("nan")

    for rec in reader:
        try:
            i = int(rec.get("i", "-1"))
        except (TypeError, ValueError):
            continue

        try:
            time_sec = float(rec.get("Time (sec)", "nan"))
        except ValueError:
            time_sec = float("nan")

        init_breakdown = _parse_init_timing(rec.get("Init Timing", ""))

        # Index convention (verified by bbox match across all methods):
        #   i=0 = first cold query (q0). Time(sec)[i=0] = init + q0 evaluation
        #   for every system. The Init Timing breakdown column is populated
        #   only on the i=0 row.
        #
        # We emit q=0 as a real query row whose `query_time` is the full
        # cold-query cost (init + q0 eval, as actually measured).  The
        # `init_time` column carries the *cold-start* time of the system,
        # which is the same number for *all* systems: the wall-clock spent
        # before any warm queries can run, including evaluating q0.  In
        # practice that equals time_sec[i=0] for every method:
        #   - Valinor: q0 stats are computed during the biased scan, so
        #     init=cold-start naturally bundles q0.
        #   - DuckDB / PilotDB: cold-start = tableCreation + indexCreation
        #     + q0 evaluation = time_sec[i=0]; the breakdown additionally
        #     reports the pure index-build component (`tableCreation +
        #     indexCreation`) which we expose separately as
        #     `init_table_create_time` for systems that build an index up
        #     front, and the q0 evaluation cost as `q0_eval_time`.
        if i == 0:
            init_time = time_sec  # full cold-start (always includes q0)
            if meta.method in ("valinor_a", "valinor_s"):
                init_table_create_time = init_breakdown.get("total", time_sec)
                q0_eval_time = max(0.0, time_sec - init_table_create_time)
            else:  # duckdb / pilotdb
                init_table_create_time = (
                    init_breakdown.get("tableCreation", 0.0)
                    + init_breakdown.get("indexCreation", 0.0)
                )
                q0_eval_time = max(0.0, time_sec - init_table_create_time)
        q = i
        query_time = time_sec

        # Optional valinor-specific query metrics
        def _f(col: str) -> float:
            v = rec.get(col, "")
            try:
                return float(v)
            except (TypeError, ValueError):
                return float("nan")

        qrow = {
            "scenario": meta.scenario,
            "method": meta.method,
            "mcols": meta.mcols,
            "error_bound": meta.error_bound,
            "run": meta.run,
            "expected_queries": meta.query_count,
            "q": q,
            "query_time": query_time,
            "init_time": init_time,                # cold-start incl. q0
            "init_table_create_time": init_table_create_time,  # pure index build
            "q0_eval_time": q0_eval_time,          # q0 work, excl. table/index build
            "ios": _f("I/Os"),
            "sampling_rate": _f("Sampling Rate"),
            "sampling_rounds": _f("Sampling Rounds"),
            "leaf_tiles": _f("Leaf tiles"),
            "overlapped_tiles": _f("Overlapped tiles"),
        }
        qrows.append(qrow)

        result_text = rec.get("Point Estimate", "") if meta.method in ("valinor_a", "valinor_s") else ""
        if not result_text:
            result_text = rec.get("Query Result", "")
        for mid, vals in _parse_query_result(result_text, meta.method).items():
            mrows.append({
                "scenario": meta.scenario, "method": meta.method,
                "mcols": meta.mcols, "error_bound": meta.error_bound,
                "run": meta.run, "expected_queries": meta.query_count,
                "q": q, "measure_id": mid,
                "sum": vals["sum"], "avg": vals["avg"],
            })

    # back-fill init_time / init_table_create_time / q0_eval_time onto every
    # row of this file so warm-query rows can also be sliced by cold-start cost.
    for r in qrows:
        r["init_time"] = init_time
        r["init_table_create_time"] = init_table_create_time
        r["q0_eval_time"] = q0_eval_time

    return qrows, mrows


# ---------------------------------------------------------------------------
# Cache key
# ---------------------------------------------------------------------------
# Bump this whenever the loader's row schema or per-row semantics change so
# stale parquet caches are automatically invalidated.
LOADER_VERSION = 4


def _cache_key(files: list[_FileMeta]) -> str:
    h = hashlib.sha1()
    h.update(f"loader_v{LOADER_VERSION}\n".encode())
    for fm in sorted(files, key=lambda f: str(f.path)):
        try:
            mt = fm.path.stat().st_mtime_ns
        except OSError:
            mt = 0
        h.update(f"{fm.path}|{mt}\n".encode())
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
@dataclass
class Dataset:
    queries: pd.DataFrame
    measures: pd.DataFrame
    scenarios: dict[str, "config.Scenario"]


def load(scenarios: Optional[Iterable[str]] = None, use_cache: bool = True) -> Dataset:
    files = _discover(scenarios)
    if not files:
        raise FileNotFoundError(f"No result files under {config.RESULTS_DIR}")

    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = _cache_key(files)
    qcache = config.CACHE_DIR / f"queries_{key}.parquet"
    mcache = config.CACHE_DIR / f"measures_{key}.parquet"

    if use_cache and qcache.exists() and mcache.exists():
        queries = pd.read_parquet(qcache)
        measures = pd.read_parquet(mcache)
    else:
        all_q, all_m = [], []
        for fm in files:
            q, m = _parse_file(fm)
            all_q.extend(q)
            all_m.extend(m)
        queries = pd.DataFrame(all_q)
        measures = pd.DataFrame(all_m)
        if use_cache:
            # purge old cache files
            for old in config.CACHE_DIR.glob("queries_*.parquet"):
                if old.name != qcache.name:
                    old.unlink(missing_ok=True)
            for old in config.CACHE_DIR.glob("measures_*.parquet"):
                if old.name != mcache.name:
                    old.unlink(missing_ok=True)
            queries.to_parquet(qcache, index=False)
            measures.to_parquet(mcache, index=False)

    scen_meta = {sc.key: sc for sc in (config.parse_scenario(s) for s in queries["scenario"].unique()) if sc}
    return Dataset(queries=queries, measures=measures, scenarios=scen_meta)
