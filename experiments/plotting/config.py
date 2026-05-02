"""Configuration: methods, scenarios, defaults.

Scenario metadata (label, dataset, axis position, etc.) is *derived* from the
directory name to avoid drift. Override only when display labels need to be
prettier than the auto-generated ones.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EXPERIMENTS_DIR = Path(__file__).resolve().parent.parent          # .../experiments
RESULTS_DIR = EXPERIMENTS_DIR / "results"
# Cache is kept outside the (possibly read-only) results tree.
CACHE_DIR = EXPERIMENTS_DIR / ".plot_cache"
PLOTS_DIR = EXPERIMENTS_DIR / "plots"

# ---------------------------------------------------------------------------
# Methods
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Method:
    key: str
    label: str
    subdir: str            # path under <scenario>/
    has_error_bound: bool  # filenames carry _error{e}_
    color: str
    marker: str
    linestyle: object       # str or matplotlib dash tuple, e.g. (0,(4,1.2))
    hatch: str = ""        # for bar plots

# Canonical methods. Color palette is colorblind-friendly + print-safe.
METHODS: dict[str, Method] = {
    "valinor_a":         Method("valinor_a",         "Valinor-A",        "valinor_a",          True,  "#1b9e77", "o", "-",            "//"),
    "valinor_s":         Method("valinor_s",         "Valinor-S",        "valinor_s",          True,  "#7570b3", "v", (0,(4,1.2)),    "\\\\"),
    "valinor_exact":     Method("valinor_exact",     "Valinor",          "valinor_a",          True,  "#444444", "s", (0,(1,1.2)),    "xx"),
    "duckdb_projected":  Method("duckdb_projected",  "DuckDB",           "duckdb/tableProjected", False, "#d95f02", "^", "-.",           ".."),
    "pilotdb":           Method("pilotdb",           "PilotDB",          "pilotdb",            True,  "#e7298a", "D", (0,(2,1,1,1)), "++"),
}

# Default method ordering for legends / bars
DEFAULT_METHOD_ORDER = ["valinor_a", "valinor_s", "valinor_exact", "duckdb_projected", "pilotdb"]

# Methods to include when the swept axis is NOT error_bound
METHODS_NO_ERROR_SWEEP = ["valinor_a", "valinor_s", "valinor_exact", "duckdb_projected", "pilotdb"]
# Methods to include when error_bound IS the swept axis (exact valinor would be a single point)
METHODS_ERROR_SWEEP = ["valinor_a", "valinor_s", "pilotdb"]

# Ground-truth method (used to compute observed relative error)
GROUND_TRUTH_METHOD = "duckdb_projected"

# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Scenario:
    key: str                     # directory name
    dataset: str                 # 'synth10' | 'taxi' | 'gaia_dr3' | 'ebird_us'
    label: str                   # short label for plots
    pattern: str                 # 'pan' | 'zoom'
    rows: Optional[str] = None   # '300M', '1B', etc.
    selectivity: Optional[float] = None  # percent (1.0 = 1%)
    default_mcols: int = 4

DATASET_LABELS = {
    "synth10":  "Synth-10",
    "taxi":     "Taxi",
    "gaia_dr3": "Gaia DR3",
    "ebird_us": "eBird US",
}

DATASET_DEFAULT_MCOLS = {
    "synth10": 4,
    "taxi": 4,
    "gaia_dr3": 4,
    "ebird_us": 4,
}

_WORKLOAD_TOKEN = r"pan|zoom|clustered|random"
_RE_SYNTH = re.compile(rf"^synth(\d+)_(\d+[MB])_({_WORKLOAD_TOKEN})_sel(\d+)$")
_RE_OTHER = re.compile(rf"^(.+?)_({_WORKLOAD_TOKEN})$")

_SEL_FROM_TOKEN = {"001": 0.01, "01": 0.1, "1": 1.0, "5": 5.0, "10": 10.0}


def parse_scenario(dir_name: str) -> Optional[Scenario]:
    m = _RE_SYNTH.match(dir_name)
    if m:
        ncols, rows, pattern, sel_tok = m.groups()
        sel = _SEL_FROM_TOKEN.get(sel_tok)
        dataset = f"synth{ncols}"
        return Scenario(
            key=dir_name, dataset=dataset, pattern=pattern, rows=rows,
            selectivity=sel,
            label=f"{DATASET_LABELS.get(dataset, dataset)} {rows} ({sel}%)",
            default_mcols=DATASET_DEFAULT_MCOLS.get(dataset, 4),
        )
    m = _RE_OTHER.match(dir_name)
    if m:
        dataset, pattern = m.groups()
        return Scenario(
            key=dir_name, dataset=dataset, pattern=pattern,
            label=f"{DATASET_LABELS.get(dataset, dataset)} ({pattern})",
            default_mcols=DATASET_DEFAULT_MCOLS.get(dataset, 4),
        )
    return None


def discover_scenarios(results_dir: Path = RESULTS_DIR) -> dict[str, Scenario]:
    out: dict[str, Scenario] = {}
    for child in sorted(results_dir.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        sc = parse_scenario(child.name)
        if sc is not None:
            out[sc.key] = sc
    return out


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_ERROR_BOUND = 0.01
DEFAULT_MCOLS = 4
DEFAULT_RUN: Optional[int] = None  # None = aggregate across runs

EXPECTED_QUERIES = 100  # i = 1..100; i=0 is init
