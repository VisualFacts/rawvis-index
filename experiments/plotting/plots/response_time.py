"""Response-time plots: per-query line, CDF, sweep."""
from __future__ import annotations

from typing import Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .. import config, metrics, style


def _filter(queries: pd.DataFrame, scenario: str, mcols: int,
            error_bound: float, methods: Sequence[str],
            include_init: bool) -> pd.DataFrame:
    df = metrics.add_valinor_exact_view(queries)
    df = metrics.select(df, scenario=scenario, mcols=mcols)
    df = df[df["method"].isin(methods)]
    df = df[(df["error_bound"] == error_bound) | (df["method"].isin(["valinor_exact", "duckdb_projected"]))]
    if not include_init:
        df = df[df["q"] >= 1]
    return df


def _resolve_mcols(scenario: str, mcols: Optional[int]) -> int:
    if mcols is not None:
        return mcols
    sc = config.parse_scenario(scenario)
    return sc.default_mcols if sc else config.DEFAULT_MCOLS


# ---------------------------------------------------------------------------
def per_query(queries: pd.DataFrame, scenario: str, *,
              mcols: Optional[int] = None,
              error_bound: float = config.DEFAULT_ERROR_BOUND,
              methods: Optional[Sequence[str]] = None,
              include_init: bool = False,
              figsize: tuple[float, float] = style.FIG_DOUBLE):
    """Mean query_time vs query index q, with std band across runs."""
    methods = methods or config.METHODS_NO_ERROR_SWEEP
    mcols = _resolve_mcols(scenario, mcols)
    df = _filter(queries, scenario, mcols, error_bound, methods, include_init)

    agg = (df.groupby(["method", "q"], dropna=False)["query_time"]
             .agg(mean="mean", std="std", n="count").reset_index())

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("q")
        if sub.empty:
            continue
        s = style.style_for(mkey, marker=False)  # no markers — too noisy at 100 queries
        ax.plot(sub["q"], sub["mean"], linewidth=1.0, alpha=0.9, **s)
        if sub["n"].max() > 1:
            ax.fill_between(sub["q"], sub["mean"] - sub["std"].fillna(0),
                            sub["mean"] + sub["std"].fillna(0),
                            color=s["color"], alpha=0.15, linewidth=0)
    ax.set_xlabel("Query index")
    ax.set_ylabel("Response time (s, log)")
    ax.set_yscale("log")
    # Left edge = first query actually plotted (q=0 includes the cold-start
    # measurement, q=1 is the first warm query). Force a tick at that left
    # edge so the first query is explicitly labelled, then add evenly-spaced
    # integer ticks afterwards.
    left = 0 if include_init else 1
    q_max = int(agg["q"].max()) if not agg.empty else left
    ax.set_xlim(left=left, right=q_max)
    from matplotlib.ticker import MaxNLocator, FixedLocator
    auto_ticks = MaxNLocator(nbins=6, integer=True, prune=None).tick_values(left, q_max)
    ticks = sorted({left, *[int(t) for t in auto_ticks if left <= t <= q_max]})
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    # Legend outside the axes — keeps the lines uncluttered.
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0,
              ncol=1, handlelength=2.0)
    style.annotate_fixed(ax, mcols=mcols, error_bound=error_bound)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
def cdf(queries: pd.DataFrame, scenario: str, *,
        mcols: Optional[int] = None,
        error_bound: float = config.DEFAULT_ERROR_BOUND,
        methods: Optional[Sequence[str]] = None,
        include_init: bool = False,
        figsize: tuple[float, float] = style.FIG_SINGLE):
    methods = methods or config.METHODS_NO_ERROR_SWEEP
    mcols = _resolve_mcols(scenario, mcols)
    df = _filter(queries, scenario, mcols, error_bound, methods, include_init)

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        v = df[df["method"] == mkey]["query_time"].dropna().to_numpy()
        if v.size == 0:
            continue
        v = np.sort(v)
        y = np.arange(1, v.size + 1) / v.size
        s = style.style_for(mkey, marker=False)
        ax.plot(v, y, drawstyle="steps-post", linewidth=1.2, **s)
    ax.set_xlabel("Response time (s, log)")
    ax.set_ylabel("CDF")
    ax.set_xscale("log")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", ncol=1, handlelength=2.0)
    style.annotate_fixed(ax, mcols=mcols, error_bound=error_bound)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
def sweep_error_bound(queries: pd.DataFrame, scenario: str, *,
                      mcols: Optional[int] = None,
                      methods: Optional[Sequence[str]] = None,
                      figsize: tuple[float, float] = style.FIG_SINGLE):
    """Mean query_time (excluding init) vs error_bound."""
    methods = methods or config.METHODS_ERROR_SWEEP
    mcols = _resolve_mcols(scenario, mcols)

    df = metrics.select(queries, scenario=scenario, mcols=mcols)
    df = df[df["method"].isin(methods) & (df["q"] >= 1)]
    df = df[df["error_bound"] > 0]

    agg = (df.groupby(["method", "error_bound", "run"])["query_time"].mean().reset_index()
             .groupby(["method", "error_bound"])["query_time"]
             .agg(mean="mean", std="std").reset_index())

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("error_bound")
        if sub.empty:
            continue
        s = style.style_for(mkey)
        ax.errorbar(sub["error_bound"] * 100, sub["mean"], yerr=sub["std"].fillna(0),
                    capsize=2, **s)
    ax.set_xlabel("Error bound (log)")
    ax.set_ylabel("Mean response time (s, log)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    eb_ticks = sorted({e * 100 for e in df["error_bound"].unique() if e > 0})
    if eb_ticks:
        ax.set_xticks(eb_ticks)
        ax.xaxis.set_major_formatter(style.percent_formatter)
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
    ax.legend(loc="best")
    style.annotate_fixed(ax, mcols=mcols)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
def sweep_mcols(queries: pd.DataFrame, scenario: str, *,
                error_bound: float = config.DEFAULT_ERROR_BOUND,
                methods: Optional[Sequence[str]] = None,
                figsize: tuple[float, float] = style.FIG_SINGLE):
    methods = methods or config.METHODS_NO_ERROR_SWEEP
    df = metrics.add_valinor_exact_view(queries)
    df = metrics.select(df, scenario=scenario)
    df = df[df["method"].isin(methods) & (df["q"] >= 1)]
    df = df[(df["error_bound"] == error_bound) | (df["method"].isin(["valinor_exact", "duckdb_projected"]))]

    agg = (df.groupby(["method", "mcols", "run"])["query_time"].mean().reset_index()
             .groupby(["method", "mcols"])["query_time"]
             .agg(mean="mean", std="std").reset_index())

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("mcols")
        if sub.empty:
            continue
        s = style.style_for(mkey)
        ax.errorbar(sub["mcols"], sub["mean"], yerr=sub["std"].fillna(0), capsize=2, **s)
    ax.set_xlabel("# Measure columns")
    ax.set_ylabel("Mean response time (s, log)")
    ax.set_yscale("log")
    ax.legend(loc="best")
    style.annotate_fixed(ax, error_bound=error_bound)
    fig.tight_layout()
    return fig
