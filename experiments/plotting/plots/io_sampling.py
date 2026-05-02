"""I/O and sampling-rate plots (Valinor-only metrics)."""
from __future__ import annotations

from typing import Optional, Sequence

import matplotlib.pyplot as plt
import pandas as pd

from .. import config, metrics, style


_VALINOR_METHODS = ("valinor_a", "valinor_s")


def io_per_query(queries: pd.DataFrame, scenario: str, *,
                 mcols: Optional[int] = None,
                 error_bound: float = config.DEFAULT_ERROR_BOUND,
                 methods: Sequence[str] = _VALINOR_METHODS,
                 figsize: tuple[float, float] = style.FIG_SINGLE):
    if mcols is None:
        sc = config.parse_scenario(scenario)
        mcols = sc.default_mcols if sc else config.DEFAULT_MCOLS

    df = metrics.select(queries, scenario=scenario, mcols=mcols, error_bound=error_bound)
    df = df[df["method"].isin(methods) & (df["q"] >= 1)]
    agg = (df.groupby(["method", "q"])["ios"].mean().reset_index())

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("q")
        if sub.empty:
            continue
        s = style.style_for(mkey, marker=False)
        ax.plot(sub["q"], sub["ios"], linewidth=1.0, **s)
    ax.set_xlabel("Query index")
    ax.set_ylabel("I/Os per query")
    ax.legend(loc="best")
    style.annotate_fixed(ax, mcols=mcols, error_bound=error_bound)
    fig.tight_layout()
    return fig


def sampling_rate(queries: pd.DataFrame, scenario: str, *,
                  mcols: Optional[int] = None,
                  methods: Sequence[str] = _VALINOR_METHODS,
                  figsize: tuple[float, float] = style.FIG_SINGLE):
    """Mean sampling rate vs error bound."""
    if mcols is None:
        sc = config.parse_scenario(scenario)
        mcols = sc.default_mcols if sc else config.DEFAULT_MCOLS

    df = metrics.select(queries, scenario=scenario, mcols=mcols)
    df = df[df["method"].isin(methods) & (df["q"] >= 1) & (df["error_bound"] > 0)]
    agg = (df.groupby(["method", "error_bound"])["sampling_rate"]
             .agg(mean="mean", std="std").reset_index())

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("error_bound")
        if sub.empty:
            continue
        s = style.style_for(mkey)
        ax.errorbar(sub["error_bound"] * 100, sub["mean"] * 100,
                    yerr=(sub["std"].fillna(0) * 100), capsize=2, **s)
    ax.set_xlabel("Error bound (%)")
    ax.set_ylabel("Mean sampling rate (%)")
    ax.set_xscale("log")
    ax.legend(loc="best")
    style.annotate_fixed(ax, mcols=mcols)
    fig.tight_layout()
    return fig
