"""Matplotlib style for plots.

Usage:
    from plotting.style import use, FIG_SINGLE, FIG_DOUBLE, save
    use()
    fig, ax = plt.subplots(figsize=FIG_SINGLE)
    ...
    save(fig, 'response_time_synth10_pan_sel1')
"""
from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt

from . import config

# Sigplan/VLDB single column ≈ 3.33 in; double column ≈ 7.0 in.
FIG_SINGLE = (3.33, 2.05)
FIG_SINGLE_TALL = (3.33, 2.6)
FIG_DOUBLE = (7.0, 2.4)
FIG_DOUBLE_TALL = (7.0, 3.0)

_RC = {
    "font.family": "serif",
    "font.serif": ["Times New Roman", "Times", "Nimbus Roman", "DejaVu Serif"],
    "mathtext.fontset": "stix",
    "pdf.fonttype": 42,        # TrueType – editor-friendly, ACM accepts
    "ps.fonttype": 42,
    "axes.labelsize": 9,
    "axes.titlesize": 9,
    "axes.linewidth": 0.6,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "xtick.direction": "in",
    "ytick.direction": "in",
    "xtick.major.size": 3,
    "ytick.major.size": 3,
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "legend.fontsize": 8,
    "legend.frameon": False,
    "legend.handlelength": 1.6,
    "legend.handletextpad": 0.5,
    "legend.columnspacing": 1.0,
    "legend.borderaxespad": 0.3,
    "lines.linewidth": 1.2,
    "lines.markersize": 3.5,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linewidth": 0.4,
    "grid.linestyle": "--",
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.02,
    "figure.dpi": 120,
}


def use() -> None:
    mpl.rcParams.update(_RC)


# Toggle for the small "fixed: ..." annotation above each axes. The defaults
# are useful when reviewing plots interactively but should be turned off for
# camera-ready figures (the caption will describe the fixed parameters).
SHOW_FIXED_ANNOTATION: bool = True


def set_annotate_fixed(enabled: bool) -> None:
    global SHOW_FIXED_ANNOTATION
    SHOW_FIXED_ANNOTATION = bool(enabled)


def style_for(method_key: str, *, marker: bool = True) -> dict:
    m = config.METHODS[method_key]
    out = {"color": m.color, "linestyle": m.linestyle, "label": m.label}
    if marker:
        out["marker"] = m.marker
    return out


def annotate_fixed(ax, **fixed) -> None:
    """Stamp a small "fixed: k=v, ..." annotation just above the axes.

    Use to make non-swept defaults visible while iterating on plots; disable
    via :func:`set_annotate_fixed` (or the ``--no-annotate`` CLI flag) for
    camera-ready figures.
    """
    if not SHOW_FIXED_ANNOTATION:
        return
    parts = []
    for k, v in fixed.items():
        if v is None:
            continue
        if isinstance(v, float):
            parts.append(f"{k}={v:g}")
        else:
            parts.append(f"{k}={v}")
    if not parts:
        return
    # Place just above the axes as a subtitle-like stamp so it never collides
    # with the legend, bars, or data lines.
    ax.text(0.0, 1.02, "fixed: " + ", ".join(parts),
            transform=ax.transAxes, fontsize=7, color="#555",
            ha="left", va="bottom")


# ---------------------------------------------------------------------------
# Tick formatters (camera-ready, no scientific notation where avoidable)
# ---------------------------------------------------------------------------
from matplotlib.ticker import FuncFormatter


def _fmt_rows(n: float, _pos=None) -> str:
    if n >= 1e9:
        v = n / 1e9
        return (f"{v:.0f}B" if v == int(v) else f"{v:g}B")
    if n >= 1e6:
        v = n / 1e6
        return (f"{v:.0f}M" if v == int(v) else f"{v:g}M")
    if n >= 1e3:
        v = n / 1e3
        return (f"{v:.0f}K" if v == int(v) else f"{v:g}K")
    return f"{n:g}"


def _fmt_percent(v: float, _pos=None) -> str:
    """Render percentages without scientific notation, at adaptive precision.

    Examples: 0.001 -> '0.001%', 0.01 -> '0.01%', 1 -> '1%', 10 -> '10%'.
    """
    if v == 0:
        return "0%"
    av = abs(v)
    if av >= 1:
        s = f"{v:g}"
    else:
        # adaptive precision: enough significant digits to never round to 0
        import math
        digits = max(0, -int(math.floor(math.log10(av))))
        s = f"{v:.{digits}f}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")
    return f"{s}%"


rows_formatter = FuncFormatter(_fmt_rows)
percent_formatter = FuncFormatter(_fmt_percent)


def bar_style_for(method_key: str) -> dict:
    m = config.METHODS[method_key]
    return {"color": m.color, "hatch": m.hatch, "edgecolor": "black", "linewidth": 0.5, "label": m.label}


def save(fig, name: str, subdir: str = "", formats: tuple[str, ...] = ("pdf",)) -> list[Path]:
    out_dir = config.PLOTS_DIR / subdir if subdir else config.PLOTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for fmt in formats:
        p = out_dir / f"{name}.{fmt}"
        fig.savefig(p, format=fmt)
        paths.append(p)
    plt.close(fig)
    return paths
