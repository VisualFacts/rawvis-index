"""Workload-cost bar charts: init vs query time, stacked."""
from __future__ import annotations

from typing import Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .. import config, metrics, style


def stacked_workload(queries: pd.DataFrame, scenario: str, *,
                     mcols: Optional[int] = None,
                     error_bound: float = config.DEFAULT_ERROR_BOUND,
                     methods: Optional[Sequence[str]] = None,
                     figsize: tuple[float, float] = style.FIG_SINGLE):
    """Per-method stacked bar: bottom = cold-start (q0, includes index build
    and q0 evaluation), top = sum of warm queries (q>=1).

    Includes valinor_exact (synth) so the cost of "no approximation" is visible.
    """
    methods = methods or config.METHODS_NO_ERROR_SWEEP
    if mcols is None:
        sc = config.parse_scenario(scenario)
        mcols = sc.default_mcols if sc else config.DEFAULT_MCOLS

    df = metrics.add_valinor_exact_view(queries)
    df = metrics.select(df, scenario=scenario, mcols=mcols)
    df = df[df["method"].isin(methods)]
    df = df[(df["error_bound"] == error_bound) | (df["method"].isin(["valinor_exact", "duckdb_projected"]))]

    wl = metrics.workload_aggregate(df)
    # Keep one row per method (the right one per method's error_bound rule already filtered)
    wl = wl.drop_duplicates(subset=["method"], keep="first").set_index("method")
    wl = wl.reindex([m for m in methods if m in wl.index])

    fig, ax = plt.subplots(figsize=figsize)
    x = np.arange(len(wl))
    init = wl["init_mean"].fillna(0).to_numpy()
    qsum = wl["queries_mean"].fillna(0).to_numpy()
    qstd = wl["queries_std"].fillna(0).to_numpy()

    # No hatches: each bar already carries the method's color and the x-tick
    # labels each method, so hatches add only visual noise. The two segments
    # are distinguished by alpha alone (light = cold-start, solid = warm).
    for i, mkey in enumerate(wl.index):
        s = style.bar_style_for(mkey)
        ax.bar(x[i], init[i], color=s["color"], alpha=0.45,
               edgecolor="black", linewidth=0.5)
        ax.bar(x[i], qsum[i], bottom=init[i], color=s["color"],
               edgecolor="black", linewidth=0.5)
        ax.errorbar(x[i], init[i] + qsum[i], yerr=qstd[i], fmt="none",
                    ecolor="black", capsize=2, linewidth=0.6)

    ax.set_xticks(x)
    ax.set_xticklabels([config.METHODS[m].label for m in wl.index], rotation=20, ha="right")
    ax.set_ylabel("Workload time (s)")

    from matplotlib.patches import Patch
    legend_handles = [
        Patch(facecolor="lightgray", edgecolor="black", linewidth=0.5, alpha=0.45,
              label="Cold-start (q0)"),
        Patch(facecolor="lightgray", edgecolor="black", linewidth=0.5,
              label="Warm queries"),
    ]
    ax.legend(handles=legend_handles, loc="best")
    style.annotate_fixed(ax, mcols=mcols, error_bound=error_bound)
    fig.tight_layout()
    return fig
