"""Cross-scenario plots: sweep over selectivity / dataset size."""
from __future__ import annotations

from typing import Optional, Sequence

import matplotlib.pyplot as plt
import pandas as pd

from .. import config, metrics, style


_ROW_MULT = {"M": 1e6, "B": 1e9, "K": 1e3}


def _rows_to_n(rows: str) -> float:
    return float(rows[:-1]) * _ROW_MULT.get(rows[-1], 1)


def _select_scenarios(scenarios_meta: dict[str, "config.Scenario"], dataset: str,
                      pattern: str, vary: str,
                      pin_rows: Optional[str], pin_selectivity: Optional[float]
                      ) -> list[tuple[float, str]]:
    """Return ``[(x_value, scenario_key)]`` sorted by ``x``.

    The non-varying dimension is *pinned* so that we never collect multiple
    scenarios sharing the same x.  For example when ``vary='rows'``, all
    chosen scenarios must share the same ``selectivity`` (default 1%).
    """
    out: list[tuple[float, str]] = []
    for sc in scenarios_meta.values():
        if sc.dataset != dataset or sc.pattern != pattern:
            continue
        if vary == "selectivity":
            if sc.selectivity is None or sc.rows != pin_rows:
                continue
            out.append((sc.selectivity, sc.key))
        elif vary == "rows":
            if sc.rows is None or sc.selectivity != pin_selectivity:
                continue
            out.append((_rows_to_n(sc.rows), sc.key))
        else:
            raise ValueError(f"unknown vary={vary!r}")
    return sorted(out)


def sweep(queries: pd.DataFrame, scenarios_meta: dict, *, vary: str,
          dataset: str = "synth10", pattern: str = "clustered",
          mcols: Optional[int] = None,
          error_bound: float = config.DEFAULT_ERROR_BOUND,
          pin_rows: Optional[str] = "300M",
          pin_selectivity: Optional[float] = 1.0,
          methods: Optional[Sequence[str]] = None,
          figsize: tuple[float, float] = style.FIG_SINGLE):
    """Mean per-query response time across a scenario sweep.

    Parameters
    ----------
    vary
        ``'selectivity'`` or ``'rows'``.
    pin_rows, pin_selectivity
        The dimension that is *not* varying is held at this value.  Defaults
        match the per-dataset typical operating point (300M rows, 1 %% sel).
    """
    methods = methods or config.METHODS_NO_ERROR_SWEEP
    if mcols is None:
        mcols = config.DATASET_DEFAULT_MCOLS.get(dataset, config.DEFAULT_MCOLS)
    pairs = _select_scenarios(scenarios_meta, dataset, pattern, vary,
                              pin_rows=pin_rows, pin_selectivity=pin_selectivity)
    if not pairs:
        raise ValueError(
            f"No scenarios for dataset={dataset} pattern={pattern} vary={vary} "
            f"pin_rows={pin_rows} pin_selectivity={pin_selectivity}"
        )

    df = metrics.add_valinor_exact_view(queries)
    df = df[df["mcols"] == mcols]
    df = df[df["method"].isin(methods)]
    df = df[(df["error_bound"] == error_bound) | (df["method"].isin(["valinor_exact", "duckdb_projected"]))]
    df = df[df["q"] >= 1]

    rows = []
    for x, sk in pairs:
        sub = df[df["scenario"] == sk]
        for mkey in methods:
            ms = sub[sub["method"] == mkey]
            if ms.empty:
                continue
            per_run = ms.groupby("run")["query_time"].mean()
            rows.append({"x": x, "method": mkey,
                         "mean": per_run.mean(),
                         "std": per_run.std() if per_run.size > 1 else 0.0})
    agg = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=figsize)
    for mkey in methods:
        sub = agg[agg["method"] == mkey].sort_values("x")
        if sub.empty:
            continue
        s = style.style_for(mkey)
        ax.errorbar(sub["x"], sub["mean"], yerr=sub["std"].fillna(0),
                    capsize=2, linewidth=1.0, **s)

    xs = sorted({x for x, _ in pairs})
    if vary == "selectivity":
        ax.set_xlabel("Query selectivity")
        ax.set_xscale("log")
        ax.set_xticks(xs)
        ax.xaxis.set_major_formatter(style.percent_formatter)
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
        fixed = dict(mcols=mcols, error_bound=error_bound,
                     dataset=dataset, rows=pin_rows)
    else:  # rows
        ax.set_xlabel("Dataset size")
        ax.set_xscale("log")
        ax.set_xticks(xs)
        ax.xaxis.set_major_formatter(style.rows_formatter)
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
        sel_label = f"{pin_selectivity:g}%" if pin_selectivity is not None else None
        fixed = dict(mcols=mcols, error_bound=error_bound,
                     dataset=dataset, selectivity=sel_label)
    ax.set_ylabel("Mean response time (s)")
    ax.set_yscale("log")
    ax.legend(loc="best")
    style.annotate_fixed(ax, **fixed)
    fig.tight_layout()
    return fig
