"""Derived metrics: ground-truth-relative error, per-config aggregates, etc."""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import config


# ---------------------------------------------------------------------------
# Ground truth join (DuckDB exact when present, otherwise Valinor exact)
# ---------------------------------------------------------------------------
def attach_relative_error(measures: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *measures* with columns:
        gt_avg, gt_sum, rel_err_avg, rel_err_sum
    by joining each row to the exact row for the same
    (scenario, mcols, q, measure_id). DuckDB is preferred when present;
    otherwise Valinor-A error=0 is used. Rows with no GT remain NaN.
    """
    exact_duckdb = measures[measures["method"] == config.GROUND_TRUTH_METHOD].copy()
    exact_duckdb["_gt_priority"] = 0
    exact_valinor = measures[(measures["method"] == "valinor_a") & (measures["error_bound"] == 0.0)].copy()
    exact_valinor["_gt_priority"] = 1
    gt_rows = pd.concat([exact_duckdb, exact_valinor], ignore_index=True)
    if gt_rows.empty:
        out = measures.copy()
        out["gt_avg"] = np.nan
        out["gt_sum"] = np.nan
    else:
        gt_rows = gt_rows.sort_values("_gt_priority")
        gt = (gt_rows.groupby(["scenario", "mcols", "q", "measure_id"], as_index=False)
              .agg(gt_avg=("avg", "first"), gt_sum=("sum", "first")))
        out = measures.merge(gt, on=["scenario", "mcols", "q", "measure_id"], how="left")

    eps = 1e-30
    out["rel_err_avg"] = (out["avg"] - out["gt_avg"]).abs() / (out["gt_avg"].abs() + eps)
    out["rel_err_sum"] = (out["sum"] - out["gt_sum"]).abs() / (out["gt_sum"].abs() + eps)
    return out


def query_level_error(measures_with_gt: pd.DataFrame, kind: str = "avg") -> pd.DataFrame:
    """Collapse per-measure errors to one error per query (mean across measures).

    Returns columns: scenario, method, mcols, error_bound, run, q, rel_err
    """
    col = f"rel_err_{kind}"
    return (measures_with_gt
            .groupby(["scenario", "method", "mcols", "error_bound", "run", "q"], as_index=False)
            [col].mean()
            .rename(columns={col: "rel_err"}))


# ---------------------------------------------------------------------------
# Synthesize a `valinor_exact` view
# ---------------------------------------------------------------------------
def add_valinor_exact_view(queries: pd.DataFrame) -> pd.DataFrame:
    """Add rows where method='valinor_exact' = subset of valinor_a with error_bound==0.

    The synthetic method has error_bound NaN so it isn't double-counted in
    error-bound sweeps, but is selectable independently.
    """
    sub = queries[(queries["method"] == "valinor_a") & (queries["error_bound"] == 0.0)].copy()
    if sub.empty:
        return queries
    sub["method"] = "valinor_exact"
    sub["error_bound"] = np.nan
    return pd.concat([queries, sub], ignore_index=True)


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------
def per_query_aggregate(queries: pd.DataFrame, include_init: bool = False) -> pd.DataFrame:
    """Aggregate over runs to get mean ± std of per-query response time
    for every (scenario, method, mcols, error_bound, q).
    """
    df = queries.copy()
    if not include_init:
        # query_time already excludes init in the loader; nothing else to drop
        df = df.dropna(subset=["query_time"])

    agg = (df.groupby(["scenario", "method", "mcols", "error_bound", "q"], dropna=False)
             ["query_time"].agg(mean="mean", std="std", count="count")
             .reset_index())
    return agg


def workload_aggregate(queries: pd.DataFrame) -> pd.DataFrame:
    """Per-run workload time, split into cold-start vs warm queries.

    Convention (uniform across systems): the *cold-start* time is the full
    cost of the first query (q=0), which already includes table/index
    creation **and** q0 evaluation. Warm-query time is the sum over q>=1.
    The total is therefore sum(query_time over all q).

    Returns columns: scenario, method, mcols, error_bound,
        init_mean, init_std, queries_mean, queries_std,
        total_mean, total_std, n_runs

    where ``init_*`` is the cold-start (q=0) time and ``queries_*`` is the
    warm-query (q>=1) sum.
    """
    cold = (queries[queries["q"] == 0]
            .groupby(["scenario", "method", "mcols", "error_bound", "run"], dropna=False)
            ["query_time"].first()
            .rename("cold")
            .reset_index())
    warm = (queries[queries["q"] >= 1]
            .groupby(["scenario", "method", "mcols", "error_bound", "run"], dropna=False)
            ["query_time"].sum()
            .rename("warm")
            .reset_index())
    per_run = cold.merge(warm, on=["scenario", "method", "mcols", "error_bound", "run"],
                         how="outer")
    per_run["cold"] = per_run["cold"].fillna(0)
    per_run["warm"] = per_run["warm"].fillna(0)
    per_run["total"] = per_run["cold"] + per_run["warm"]
    out = (per_run.groupby(["scenario", "method", "mcols", "error_bound"], dropna=False)
                  .agg(init_mean=("cold", "mean"), init_std=("cold", "std"),
                       queries_mean=("warm", "mean"), queries_std=("warm", "std"),
                       total_mean=("total", "mean"), total_std=("total", "std"),
                       n_runs=("run", "nunique"))
                  .reset_index())
    return out


def init_breakdown_aggregate(queries: pd.DataFrame) -> pd.DataFrame:
    """Per-run cold-start breakdown: index/table build vs q0 evaluation.

    For Valinor, q0 evaluation is folded into the biased scan and is reported
    as ~0; the entire cold-start is "init". For DuckDB / PilotDB, the init
    segment is ``tableCreation + indexCreation`` and the q0 segment is the
    rest of the cold-query wall time.
    """
    cold = (queries[queries["q"] == 0]
            .groupby(["scenario", "method", "mcols", "error_bound", "run"], dropna=False)
            .agg(table_create=("init_table_create_time", "first"),
                 q0_eval=("q0_eval_time", "first"),
                 cold_total=("query_time", "first"))
            .reset_index())
    out = (cold.groupby(["scenario", "method", "mcols", "error_bound"], dropna=False)
               .agg(table_create_mean=("table_create", "mean"),
                    table_create_std=("table_create", "std"),
                    q0_eval_mean=("q0_eval", "mean"),
                    q0_eval_std=("q0_eval", "std"),
                    cold_total_mean=("cold_total", "mean"),
                    cold_total_std=("cold_total", "std"),
                    n_runs=("run", "nunique"))
               .reset_index())
    return out


def error_aggregate(query_err: pd.DataFrame) -> pd.DataFrame:
    """Mean / p95 of relative error across queries and runs, per config."""
    return (query_err.groupby(["scenario", "method", "mcols", "error_bound"], dropna=False)
                     ["rel_err"].agg(mean="mean", p95=lambda x: np.percentile(x.dropna(), 95) if x.dropna().size else np.nan)
                     .reset_index())


# ---------------------------------------------------------------------------
# Selection helpers
# ---------------------------------------------------------------------------
def select(df: pd.DataFrame, **fixed) -> pd.DataFrame:
    out = df
    for k, v in fixed.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple, set)):
            out = out[out[k].isin(list(v))]
        else:
            out = out[out[k] == v]
    return out
