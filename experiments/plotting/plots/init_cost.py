"""Init-cost breakdown (Valinor scan/partition vs DuckDB tableCreation, etc.)."""
from __future__ import annotations

from typing import Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .. import config, metrics, style


def init_bar(queries: pd.DataFrame, scenario: str, *,
             mcols: Optional[int] = None,
             error_bound: float = config.DEFAULT_ERROR_BOUND,
             methods: Optional[Sequence[str]] = None,
             figsize: tuple[float, float] = style.FIG_SINGLE):
    """Per-method cold-start bar.

    The cold-start time is the wall-clock spent before any warm queries can
    run, i.e. the time of q0. For systems that build a table/index up front
    (DuckDB, PilotDB) the bar is stacked: bottom = table+index creation,
    top = q0 evaluation. For Valinor, q0 stats are computed during the
    biased scan, so the whole bar is a single segment.
    """
    methods = methods or config.METHODS_NO_ERROR_SWEEP
    if mcols is None:
        sc = config.parse_scenario(scenario)
        mcols = sc.default_mcols if sc else config.DEFAULT_MCOLS

    df = metrics.add_valinor_exact_view(queries)
    df = metrics.select(df, scenario=scenario, mcols=mcols)
    df = df[df["method"].isin(methods)]
    df = df[(df["error_bound"] == error_bound) | (df["method"].isin(["valinor_exact", "duckdb_projected"]))]

    bk = metrics.init_breakdown_aggregate(df)
    bk = bk.drop_duplicates(subset=["method"], keep="first").set_index("method")
    bk = bk.reindex([m for m in methods if m in bk.index])

    fig, ax = plt.subplots(figsize=figsize)
    x = np.arange(len(bk))
    table = bk["table_create_mean"].fillna(0).to_numpy()
    q0eval = bk["q0_eval_mean"].fillna(0).to_numpy()
    total = table + q0eval  # full cold-start
    total_std = bk["cold_total_std"].fillna(0).to_numpy()

    # Single solid bar per method = cold-start time (build + q0 evaluation).
    # The build/q0 split varies in semantic meaning across systems (Valinor
    # has no separable q0 cost), so reporting the union is the apples-to-apples
    # comparison.
    for i, mkey in enumerate(bk.index):
        s = style.bar_style_for(mkey)
        ax.bar(x[i], total[i], color=s["color"],
               edgecolor="black", linewidth=0.5)
        ax.errorbar(x[i], total[i], yerr=total_std[i], fmt="none",
                    ecolor="black", capsize=2, linewidth=0.6)

    ax.set_xticks(x)
    ax.set_xticklabels([config.METHODS[m].label for m in bk.index], rotation=20, ha="right")
    ax.set_ylabel("Cold-start time (s)")
    style.annotate_fixed(ax, mcols=mcols, error_bound=error_bound)
    fig.tight_layout()
    return fig
