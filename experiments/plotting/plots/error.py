"""Observed-error plots (relative error vs DuckDB ground truth)."""
from __future__ import annotations

from typing import Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .. import config, metrics, style


def _resolve_mcols(scenario: str, mcols: Optional[int]) -> int:
    if mcols is not None:
        return mcols
    sc = config.parse_scenario(scenario)
    return sc.default_mcols if sc else config.DEFAULT_MCOLS


def _query_err(measures: pd.DataFrame, scenario: str, mcols: int) -> pd.DataFrame:
    sub = measures[(measures["scenario"] == scenario) & (measures["mcols"] == mcols)]
    sub = metrics.attach_relative_error(sub)
    return metrics.query_level_error(sub, kind="avg")


# ---------------------------------------------------------------------------
def cdf(measures: pd.DataFrame, scenario: str, *,
        mcols: Optional[int] = None,
        error_bound: float = config.DEFAULT_ERROR_BOUND,
        methods: Optional[Sequence[str]] = None,
        figsize: tuple[float, float] = style.FIG_SINGLE):
    """CDF of per-query observed relative error (averaged across measures)."""
    methods = methods or [m for m in config.METHODS_ERROR_SWEEP if m != config.GROUND_TRUTH_METHOD]
    mcols = _resolve_mcols(scenario, mcols)

    qe = _query_err(measures, scenario, mcols)
    qe = qe[(qe["q"] >= 1) & (qe["error_bound"] == error_bound) & qe["method"].isin(methods)]

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        v = qe[qe["method"] == mkey]["rel_err"].dropna().to_numpy()
        if v.size == 0:
            continue
        v = np.sort(v)
        y = np.arange(1, v.size + 1) / v.size
        s = style.style_for(mkey, marker=False)
        ax.plot(v, y, drawstyle="steps-post", linewidth=1.2, **s)
    # vertical reference line at requested bound, with a label
    ax.axvline(error_bound, color="black", linestyle=":", linewidth=0.8)
    ax.text(error_bound, 0.05, "  requested", fontsize=7, color="#444",
            ha="left", va="bottom", rotation=90)
    ax.set_xlabel("Relative error (log)")
    ax.set_ylabel("CDF")
    ax.set_xscale("log")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", handlelength=2.0)
    style.annotate_fixed(ax, mcols=mcols, error_bound=error_bound)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
def sweep_error_bound(measures: pd.DataFrame, scenario: str, *,
                      mcols: Optional[int] = None,
                      methods: Optional[Sequence[str]] = None,
                      figsize: tuple[float, float] = style.FIG_SINGLE):
    """Observed mean relative error vs requested error bound.

    The dotted ``y = x`` reference is the *budget line*: a method whose curve
    sits on it consumes exactly its allowed error; below = more accurate than
    requested, above = budget violated.
    """
    methods = methods or [m for m in config.METHODS_ERROR_SWEEP if m != config.GROUND_TRUTH_METHOD]
    mcols = _resolve_mcols(scenario, mcols)

    qe = _query_err(measures, scenario, mcols)
    qe = qe[(qe["q"] >= 1) & (qe["error_bound"] > 0) & qe["method"].isin(methods)]

    agg = (qe.groupby(["method", "error_bound", "run"])["rel_err"].mean().reset_index()
             .groupby(["method", "error_bound"])["rel_err"].agg(mean="mean", std="std").reset_index())

    fig, ax = plt.subplots(figsize=figsize)
    eb_pct = sorted({e * 100 for e in qe["error_bound"].unique()})
    if eb_pct:
        ax.plot(eb_pct, eb_pct, color="black", linestyle=":", linewidth=0.8,
                label="$y = x$ (budget)")
    # Linear-near-zero threshold for the y-axis (in percent). PilotDB sometimes
    # returns the exact answer (mean rel-err ~ 0) which a pure log axis cannot
    # render; symlog is log above ``linthresh`` and linear below, so an exact
    # run drops visibly to (and reaches) zero.
    linthresh_pct = (min(eb_pct) / 10.0) if eb_pct else 1e-4

    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("error_bound").copy()
        if sub.empty:
            continue
        s = style.style_for(mkey)
        x_pct = (sub["error_bound"] * 100).to_numpy()
        y_pct = (sub["mean"] * 100).to_numpy()
        yerr = (sub["std"].fillna(0) * 100).to_numpy()
        ax.errorbar(x_pct, y_pct, yerr=yerr, capsize=2, **s)
    ax.set_xlabel("Requested error bound (log)")
    ax.set_ylabel("Observed mean error (symlog)")
    ax.set_xscale("log")
    ax.set_yscale("symlog", linthresh=linthresh_pct, linscale=0.5)
    if eb_pct:
        ax.set_xticks(eb_pct)
        ax.xaxis.set_major_formatter(style.percent_formatter)
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
        ax.yaxis.set_major_formatter(style.percent_formatter)
        ax.yaxis.set_minor_formatter(plt.NullFormatter())
        # Y range: include 0 (so PilotDB-exact is visible) up to a cap above
        # the largest requested bound.
        ax.set_ylim(0, max(eb_pct) * 5.0)
    ax.legend(loc="best", handlelength=2.0)
    style.annotate_fixed(ax, mcols=mcols)
    fig.tight_layout()
    return fig
