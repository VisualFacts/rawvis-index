#!/usr/bin/env python3
"""Render generated query sequences as paper-quality static PDF/PNG figures.

For every requested scenario this script:

  1. Resolves a deterministic query file under
      ``experiments/query_sequences/<scenario>.txt``. If none exists, it calls
      the Java ``generateAndSaveQuerySequence`` command to materialise one.
  2. Renders the resulting query rectangles as a single PDF (or PNG) under
     ``experiments/plots/workloads/``.

The axis limits are taken from the dataset's ``bounds`` field in
``experiment_scenarios.yaml``, so the three workloads of one dataset
(exploratory / random / clustered) are drawn at IDENTICAL scale and a
rectangle of a given size occupies the same pixel area across plots.

For geospatial datasets (``taxi``, ``ebird_us``) the ``--basemap`` flag
overlays an OpenStreetMap tile background via the optional ``contextily``
dependency.

Usage examples
--------------

  # Default: render the full 11-cell workload grid as PDFs.
  python3 experiments/render_query_sequence_pdf.py

  # Single scenario as PNG, regenerating the query file.
  python3 experiments/render_query_sequence_pdf.py \\
      --scenarios taxi_clustered --format png

    # Reuse already-generated query files and skip the Java step.
  python3 experiments/render_query_sequence_pdf.py --reuse-query-files

  # Add an OSM basemap for the geospatial datasets.
  python3 experiments/render_query_sequence_pdf.py --basemap

  # Also produce the interactive Leaflet HTML for each scenario.
  python3 experiments/render_query_sequence_pdf.py --html

Optional dependencies
---------------------

  * PyYAML       (required) — parses dataset bounds from the scenarios YAML.
  * contextily   (optional) — used by ``--basemap`` to fetch OSM tiles.
                 Install with::

                     pip install contextily

The HTML output (``--html``) is produced by delegating to
``visualize_query_sequence.py``; the PDF script itself never writes HTML.
"""

import argparse
import math
import os
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import yaml
from matplotlib.collections import PatchCollection
from matplotlib.patches import Patch, Rectangle

from visualize_query_sequence import load_queries

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_FILE = PROJECT_ROOT / "src/main/resources/experiments/experiment_scenarios.yaml"
JAR_FILE = PROJECT_ROOT / "target/experiments.jar"
NATIVE_LIB = PROJECT_ROOT / "native/build"

COLORS = {
    "Q0": "#4c78a8",
    "P": "#4c78a8",
    "ZI": "#f58518",
    "ZO": "#54a24b",
    "R": "#b279a2",
    None: "#4c78a8",
}

SCENARIO_LABELS = {
    "synth10_300M_clustered_sel1": "Synth10 Clustered",
    "synth10_300M_random_sel1": "Synth10 Random",
    "taxi_exploratory": "Taxi Exploratory",
    "taxi_random": "Taxi Random",
    "taxi_clustered": "Taxi Clustered",
    "gaia_dr3_exploratory": "Gaia DR3 Exploratory",
    "gaia_dr3_random": "Gaia DR3 Random",
    "gaia_dr3_clustered": "Gaia DR3 Clustered",
    "ebird_us_exploratory": "eBird US Exploratory",
    "ebird_us_random": "eBird US Random",
    "ebird_us_clustered": "eBird US Clustered",
}

# Datasets whose (x, y) columns are (longitude, latitude) in WGS84 degrees.
# These are the only ones for which an OSM basemap makes sense.
GEOSPATIAL_DATASETS = {"taxi", "ebird_us"}

DEFAULT_SCENARIOS = list(SCENARIO_LABELS.keys())


# ---------------------------------------------------------------------------
# Scenario / dataset metadata
# ---------------------------------------------------------------------------

def _parse_bounds(bounds_str):
    """Parse "xmin:xmax,ymin:ymax" into (xmin, xmax, ymin, ymax)."""
    x_part, y_part = bounds_str.split(",")
    xmin, xmax = (float(v) for v in x_part.split(":"))
    ymin, ymax = (float(v) for v in y_part.split(":"))
    return xmin, xmax, ymin, ymax


def load_scenario_metadata():
    """Return ``{scenario_name: (dataset_name, (xmin, xmax, ymin, ymax))}``."""
    with open(CONFIG_FILE) as f:
        cfg = yaml.safe_load(f)
    datasets = cfg.get("datasets", {})
    scenarios = cfg.get("scenarios", {})
    out = {}
    for name, sc in scenarios.items():
        ds_name = sc.get("dataset")
        if ds_name and ds_name in datasets:
            out[name] = (ds_name, _parse_bounds(datasets[ds_name]["bounds"]))
    return out


# ---------------------------------------------------------------------------
# Query generation
# ---------------------------------------------------------------------------

def ensure_jar():
    if not JAR_FILE.exists():
        subprocess.check_call(["mvn", "-q", "-DskipTests", "package"], cwd=PROJECT_ROOT)


def generate_query_file(scenario: str, output: Path):
    ensure_jar()
    output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "java", f"-Djava.library.path={NATIVE_LIB}",
        "-jar", str(JAR_FILE),
        "-c", "generateAndSaveQuerySequence",
        "-scenario", scenario,
        "-configFile", str(CONFIG_FILE),
        "-out", str(output),
    ]
    subprocess.check_call(cmd, cwd=PROJECT_ROOT)
    return output


def resolve_query_file(query_dir: Path, scenario: str) -> Path | None:
    candidate = query_dir / f"{scenario}.txt"
    return candidate if candidate.exists() else None


# ---------------------------------------------------------------------------
# Basemap helpers (optional)
# ---------------------------------------------------------------------------

def _add_basemap(ax, xmin, xmax, ymin, ymax):
    """Reproject the current axes to Web Mercator and overlay OSM tiles.

    Returns the projected bounds (xmin, xmax, ymin, ymax) so the caller can
    re-clip after the basemap is drawn. Raises ``RuntimeError`` if contextily
    is not installed.
    """
    try:
        import contextily as cx
        from pyproj import Transformer
    except ImportError as exc:
        raise RuntimeError(
            "--basemap requires the 'contextily' package. "
            "Install with: pip install contextily"
        ) from exc

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    return transformer, cx


def _project_lonlat(transformer, x, y):
    px, py = transformer.transform(x, y)
    return px, py


# ---------------------------------------------------------------------------
# Figure rendering
# ---------------------------------------------------------------------------

def render_sequence(query_file, output_file, title, *, dataset_bounds,
                    dataset_name=None, max_queries=None, basemap=False):
    queries = load_queries(query_file)
    if max_queries is not None:
        queries = queries[:max_queries]
    if not queries:
        raise ValueError(f"No queries parsed from {query_file}")

    use_basemap = basemap and dataset_name in GEOSPATIAL_DATASETS

    transformer = None
    cx_module = None
    if use_basemap:
        transformer, cx_module = _add_basemap(None, *dataset_bounds)

    # Project bounds (and per-query rectangles) to Web Mercator if a basemap
    # is requested; otherwise keep raw coordinates.
    def _proj(x, y):
        if transformer is not None:
            return transformer.transform(x, y)
        return x, y

    xmin, xmax, ymin, ymax = dataset_bounds
    pxmin, pymin = _proj(xmin, ymin)
    pxmax, pymax = _proj(xmax, ymax)

    fig, ax = plt.subplots(figsize=(4.2, 3.6), constrained_layout=True)

    patches_by_op = {}
    centers_x = []
    centers_y = []
    for q in queries:
        op = q.get("opType") or "Q0"
        x0, y0 = _proj(q["xLow"], q["yLow"])
        x1, y1 = _proj(q["xHigh"], q["yHigh"])
        patches_by_op.setdefault(op, []).append(Rectangle(
            (x0, y0), x1 - x0, y1 - y0,
        ))
        centers_x.append((x0 + x1) / 2.0)
        centers_y.append((y0 + y1) / 2.0)

    for op, patches in patches_by_op.items():
        color = COLORS.get(op, "#777777")
        # Light fill so overlapping rectangles don't obscure each other.
        ax.add_collection(PatchCollection(
            patches,
            facecolor=color,
            edgecolor="none",
            linewidth=0,
            alpha=0.08,
            label="_nolegend_",
        ))
        # Opaque edges so every query rect is clearly discernible.
        ax.add_collection(PatchCollection(
            patches,
            facecolor="none",
            edgecolor=color,
            linewidth=1.2,
            alpha=1.0,
            label="_nolegend_",
        ))

    ax.plot(centers_x, centers_y, color="#222222", linewidth=1.0, alpha=0.55)
    ax.scatter(centers_x[0], centers_y[0], s=18, color="#111111", marker="o",
               label="first", zorder=5)
    ax.scatter(centers_x[-1], centers_y[-1], s=22, color="#111111", marker="x",
               label="last", zorder=5)

    # ---- Identical axis limits across all workloads of this dataset --------
    ax.set_xlim(pxmin, pxmax)
    ax.set_ylim(pymin, pymax)
    # Equal aspect so a width-w / height-h rectangle is drawn proportionally.
    ax.set_aspect("equal", adjustable="box")

    if use_basemap:
        try:
            cx_module.add_basemap(
                ax,
                source=cx_module.providers.OpenStreetMap.Mapnik,
                crs="EPSG:3857",
                attribution_size=4,
            )
        except Exception as exc:
            print(f"  [warn] basemap fetch failed for {title}: {exc}",
                  file=sys.stderr)
        ax.set_xlabel("longitude")
        ax.set_ylabel("latitude")
    else:
        ax.set_xlabel("x")
        ax.set_ylabel("y")

    ax.set_title(title, fontsize=10)
    ax.tick_params(axis="both", labelsize=7)
    ax.grid(True, linewidth=0.3, alpha=0.25)

    op_handles = [Patch(facecolor=COLORS.get(op, "#777777"),
                        edgecolor=COLORS.get(op, "#777777"),
                        linewidth=1.2, alpha=0.6, label=op)
                  for op in patches_by_op]
    handles, labels = ax.get_legend_handles_labels()
    unique = {h.get_label(): h for h in op_handles}
    unique.update(dict(zip(labels, handles)))
    ax.legend(unique.values(), unique.keys(), fontsize=6, loc="best",
              frameon=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_file)
    plt.close(fig)
    print(f"Wrote {output_file}")


# ---------------------------------------------------------------------------
# Optional HTML delegation
# ---------------------------------------------------------------------------

def emit_html(scenario, query_file, out_dir):
    """Delegate to visualize_query_sequence to produce the Leaflet HTML."""
    from visualize_query_sequence import visualize
    queries = load_queries(query_file)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{scenario}.html"
    visualize(queries, str(out_file), open_browser=False)
    print(f"Wrote {out_file}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate static workload sequence figures (PDF/PNG).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--scenarios", nargs="*", default=DEFAULT_SCENARIOS,
                        help="Scenario names to render; defaults to the main "
                             "11-cell workload grid")
    parser.add_argument("--query-dir",
                        default=str(PROJECT_ROOT / "experiments/query_sequences"))
    parser.add_argument("--out-dir",
                        default=str(PROJECT_ROOT / "experiments/plots/workloads"))
    parser.add_argument("--format", choices=["pdf", "png"], default="pdf")
    parser.add_argument("--max-queries", type=int, default=None,
                        help="Render only the first N queries for dense "
                             "preview figures")
    parser.add_argument("--reuse-query-files", action="store_true",
                        help="Do not regenerate existing query files; reuse the canonical scenario .txt if present")
    parser.add_argument("--basemap", action="store_true",
                        help="Overlay an OSM basemap for geospatial datasets "
                             "(taxi, ebird_us). Requires the 'contextily' "
                             "package.")
    parser.add_argument("--html", action="store_true",
                        help="Also emit an interactive Leaflet HTML for each "
                             "scenario (delegated to visualize_query_sequence).")
    args = parser.parse_args()

    query_dir = Path(args.query_dir)
    out_dir = Path(args.out_dir)
    html_dir = out_dir / "html"

    metadata = load_scenario_metadata()

    for scenario in args.scenarios:
        if scenario not in metadata:
            print(f"  [skip] unknown scenario (not in YAML): {scenario}",
                  file=sys.stderr)
            continue
        dataset_name, dataset_bounds = metadata[scenario]

        query_file = resolve_query_file(query_dir, scenario)
        if not args.reuse_query_files or query_file is None:
            query_file = query_dir / f"{scenario}.txt"
            generate_query_file(scenario, query_file)

        title = SCENARIO_LABELS.get(scenario, scenario.replace("_", " "))
        render_sequence(
            query_file,
            out_dir / f"{scenario}.{args.format}",
            title,
            dataset_bounds=dataset_bounds,
            dataset_name=dataset_name,
            max_queries=args.max_queries,
            basemap=args.basemap,
        )

        if args.html:
            emit_html(scenario, query_file, html_dir)


if __name__ == "__main__":
    main()
