#!/usr/bin/env python3
"""Analyze Valinor adaptation curves for paired clustered/random workloads.

Run from the repository root after activating the Python environment, for
example::

    source venv/bin/activate
    python experiments/analysis/analyze_adaptation_curves.py \
        --results-dir experiments/results \
        --random-scenario synth10_300M_random_sel1 \
        --clustered-scenario synth10_300M_clustered_sel1 \
        --mcols 4 --query-count 500 --run 1 --out-k 0 \
        --dataset-label "Synth10 300M" \
        --output-prefix synth10_300M_adaptation

The script expects Valinor-A files named
``results_mcols<M>_error<E>_res<R>_str<S>_n<Q>_run<RUN>.csv`` and skips the
Valinor-A/Valinor-S comparison figure when matching Valinor-S files are absent.
When ``--out-k`` is positive, approximate files are expected to include the
``_outK<K>`` suffix used by the current experiment runner.
Row ``i=0`` is treated as the cold query and is excluded from adaptation trends.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

EXPERIMENTS_DIR = Path(__file__).resolve().parents[1]
if str(EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENTS_DIR))

from plotting.style import rows_formatter, save, use  # noqa: E402


WORKLOAD_LABELS = {
    "clustered": "Clustered",
    "random": "Random",
}

WORKLOAD_COLORS = {
    "clustered": "#1b9e77",
    "random": "#d95f02",
}

ERROR_COLORS = {
    "0.01": "#1b9e77",
    "0.02": "#7570b3",
    "0.05": "#d95f02",
    "0.1": "#e7298a",
}

METHOD_COLORS = {
    "valinor_a": "#1b9e77",
    "valinor_s": "#7570b3",
}

METHOD_LABELS = {
    "valinor_a": "Valinor-A",
    "valinor_s": "Valinor-S",
}


@dataclass(frozen=True)
class SeriesSummary:
    workload: str
    error_token: str
    first50_time: float
    last50_time: float
    time_drop_pct: float
    tail_delta_pct: float
    first50_ios: float
    last50_ios: float
    io_drop_pct: float
    io_tail_delta_pct: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze 500-query adaptation curves for random and clustered workloads."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=EXPERIMENTS_DIR / "results",
        help="Base results directory (default: experiments/results)",
    )
    parser.add_argument("--mcols", type=int, default=4, help="Measure-column count to analyze")
    parser.add_argument("--resolution", type=int, default=500, help="Initial grid resolution")
    parser.add_argument("--subtile-ratio", default="0", help="Subtile ratio token used in filenames")
    parser.add_argument("--query-count", type=int, default=500, help="Expected query count suffix _nN")
    parser.add_argument("--out-k", type=int, default=0, help="Outlier index k suffix for approximate files; 0 omits _outK")
    parser.add_argument("--run", type=int, default=1, help="Run number to analyze")
    parser.add_argument("--random-scenario", default="synth10_300M_random_sel1", help="Scenario directory for the random workload")
    parser.add_argument("--clustered-scenario", default="synth10_300M_clustered_sel1", help="Scenario directory for the clustered workload")
    parser.add_argument("--dataset-label", default="Synth10 300M", help="Label used in plot titles and summaries")
    parser.add_argument("--output-prefix", default="adaptation", help="Prefix for generated plot and summary filenames")
    parser.add_argument("--object-count", type=int, default=None, help="Optional dataset object count for realized selectivity reporting")
    parser.add_argument(
        "--approx-errors",
        nargs="+",
        default=["0.01", "0.02", "0.05", "0.1"],
        help="Approximate error-bound tokens to plot",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=50,
        help="Window size for asymptote diagnostics (default: 50 queries)",
    )
    parser.add_argument(
        "--compare-error",
        default="0.01",
        help="Approximate error token used for the Valinor-A vs Valinor-S comparison plot (default: 0.01)",
    )
    return parser.parse_args()


def scenario_for(workload: str, args: argparse.Namespace) -> str:
    return args.clustered_scenario if workload == "clustered" else args.random_scenario


def pct_change(start: float, end: float) -> float:
    if start == 0:
        return float("nan")
    return (end - start) / start * 100.0


def rolling_mean(series: pd.Series, window: int = 25) -> np.ndarray:
    return series.rolling(window, min_periods=1, center=True).mean().to_numpy()


def window_means(df: pd.DataFrame, column: str, window: int) -> pd.DataFrame:
    rows = []
    max_query = int(df["i"].max())
    for start in range(1, max_query + 1, window):
        end = min(start + window - 1, max_query)
        values = df.loc[df["i"].between(start, end), column]
        rows.append(
            {
                "start": start,
                "end": end,
                "mid": 0.5 * (start + end),
                "mean": float(values.mean()),
            }
        )
    return pd.DataFrame(rows)


def build_filename(
    mcols: int,
    error_token: str,
    resolution: int,
    subtile_ratio: str,
    query_count: int,
    run: int,
    out_k: int,
) -> str:
    outlier_suffix = f"_outK{out_k}" if error_token != "0" and out_k > 0 else ""
    return (
        f"results_mcols{mcols}_error{error_token}_res{resolution}_str{subtile_ratio}"
        f"_n{query_count}{outlier_suffix}_run{run}.csv"
    )


def load_series(
    results_dir: Path,
    workload: str,
    error_token: str,
    *,
    args: argparse.Namespace,
    method: str = "valinor_a",
    mcols: int,
    resolution: int,
    subtile_ratio: str,
    query_count: int,
    run: int,
) -> pd.DataFrame:
    scenario = scenario_for(workload, args)
    filename = build_filename(mcols, error_token, resolution, subtile_ratio, query_count, run, args.out_k)
    path = results_dir / scenario / method / filename
    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_csv(path)
    df = df.loc[df["i"] > 0].copy()
    for column in ["i", "Time (sec)", "I/Os", "Leaf tiles", "Sampling Tiles", "Sampling Rate"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df["query"] = df["i"]
    return df


def realized_selectivity_stats(df: pd.DataFrame, object_count: int) -> dict[str, float]:
    counts = df["Total Count"].to_numpy(dtype=float)
    selectivities = counts / object_count
    return {
        "mean": float(np.mean(selectivities)),
        "std": float(np.std(selectivities)),
        "min": float(np.min(selectivities)),
        "max": float(np.max(selectivities)),
    }


def summarize_series(df: pd.DataFrame, *, workload: str, error_token: str, window: int) -> SeriesSummary:
    time_windows = window_means(df, "Time (sec)", window)
    io_windows = window_means(df, "I/Os", window)
    return SeriesSummary(
        workload=workload,
        error_token=error_token,
        first50_time=float(time_windows.iloc[0]["mean"]),
        last50_time=float(time_windows.iloc[-1]["mean"]),
        time_drop_pct=pct_change(float(time_windows.iloc[0]["mean"]), float(time_windows.iloc[-1]["mean"])),
        tail_delta_pct=pct_change(float(time_windows.iloc[-2]["mean"]), float(time_windows.iloc[-1]["mean"])),
        first50_ios=float(io_windows.iloc[0]["mean"]),
        last50_ios=float(io_windows.iloc[-1]["mean"]),
        io_drop_pct=pct_change(float(io_windows.iloc[0]["mean"]), float(io_windows.iloc[-1]["mean"])),
        io_tail_delta_pct=pct_change(float(io_windows.iloc[-2]["mean"]), float(io_windows.iloc[-1]["mean"])),
    )


def asymptote_label(summary: SeriesSummary) -> str:
    tail = abs(summary.tail_delta_pct)
    if summary.tail_delta_pct <= -7.0:
        return "still improving"
    if tail <= 3.0:
        return "flat"
    if tail <= 7.0:
        return "near-flat"
    return "noisy tail"


def make_adaptation_effect_plot(
    data: dict[tuple[str, str], pd.DataFrame],
    approx_errors: list[str],
    *,
    dataset_label: str,
    output_prefix: str,
    query_count: int,
) -> list[Path]:
    fig, axes = plt.subplots(2, 2, figsize=(7.0, 4.8), sharex="col")

    ax = axes[0, 0]
    for workload in ["clustered", "random"]:
        df = data[(workload, "0")]
        ax.plot(df["query"], rolling_mean(df["Time (sec)"], 25), color=WORKLOAD_COLORS[workload], label=WORKLOAD_LABELS[workload])
    ax.set_title("Exact Query Time")
    ax.set_ylabel("Time (sec)")
    ax.legend(ncol=2, loc="upper right")

    ax = axes[0, 1]
    for workload in ["clustered", "random"]:
        df = data[(workload, "0")]
        ax.plot(df["query"], rolling_mean(df["I/Os"], 25), color=WORKLOAD_COLORS[workload], label=WORKLOAD_LABELS[workload])
    ax.set_title("Exact I/Os")
    ax.set_ylabel("I/Os")
    ax.yaxis.set_major_formatter(rows_formatter)

    ax = axes[1, 0]
    for error_token in approx_errors:
        df = data[("clustered", error_token)]
        ax.plot(df["query"], rolling_mean(df["Time (sec)"], 25), color=ERROR_COLORS[error_token], label=f"eb={error_token}")
    ax.set_title("Approximate Query Time (Clustered)")
    ax.set_xlabel("Query index")
    ax.set_ylabel("Time (sec)")
    ax.set_yscale("log")
    ax.legend(ncol=2, loc="upper right")

    ax = axes[1, 1]
    for error_token in approx_errors:
        df = data[("random", error_token)]
        ax.plot(df["query"], rolling_mean(df["Time (sec)"], 25), color=ERROR_COLORS[error_token], label=f"eb={error_token}")
    ax.set_title("Approximate Query Time (Random)")
    ax.set_xlabel("Query index")
    ax.set_ylabel("Time (sec)")
    ax.set_yscale("log")

    fig.suptitle(f"{dataset_label} — Valinor-A Adaptation Over {query_count} Queries", y=1.02, fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    return save(fig, f"{output_prefix}_effect_n{query_count}", formats=("pdf", "png"))


def make_asymptote_plot(
    data: dict[tuple[str, str], pd.DataFrame],
    summaries: dict[tuple[str, str], SeriesSummary],
    *,
    dataset_label: str,
    output_prefix: str,
    query_count: int,
    window: int,
) -> list[Path]:
    fig, axes = plt.subplots(2, 2, figsize=(7.0, 4.8), sharex=True)
    specs = [
        ("clustered", "0", "Exact"),
        ("clustered", "0.01", "Approximate (eb=0.01)"),
        ("random", "0", "Exact"),
        ("random", "0.01", "Approximate (eb=0.01)"),
    ]

    for ax, (workload, error_token, mode_label) in zip(axes.flat, specs):
        df = data[(workload, error_token)]
        series_summary = summaries[(workload, error_token)]
        windows = window_means(df, "Time (sec)", window)
        ax.plot(windows["end"], windows["mean"], marker="o", color=WORKLOAD_COLORS[workload])
        ax.axhline(windows.iloc[-1]["mean"], color=WORKLOAD_COLORS[workload], linestyle="--", linewidth=0.9, alpha=0.6)
        ax.set_title(f"{WORKLOAD_LABELS[workload]} — {mode_label}")
        ax.set_xlabel("Query index")
        ax.set_ylabel(f"Mean time per {window} queries (sec)")
        ax.text(
            0.03,
            0.97,
            (
                f"first→last: {series_summary.time_drop_pct:+.1f}%\n"
                f"last vs prev: {series_summary.tail_delta_pct:+.1f}%\n"
                f"status: {asymptote_label(series_summary)}"
            ),
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=7,
            bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "none"},
        )

    fig.suptitle(f"{dataset_label} — Windowed Asymptote Diagnostics (window={window}, n={query_count})", y=1.02, fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    return save(fig, f"{output_prefix}_asymptote_n{query_count}", formats=("pdf", "png"))


def make_cumulative_plot(
    data: dict[tuple[str, str], pd.DataFrame],
    summaries: dict[tuple[str, str], SeriesSummary],
    *,
    dataset_label: str,
    output_prefix: str,
    query_count: int,
) -> list[Path]:
    fig, axes = plt.subplots(2, 2, figsize=(7.0, 4.8), sharex="col")
    mode_specs = [
        ("0", "Exact"),
        ("0.01", "Approximate (eb=0.01)"),
    ]

    for col, (error_token, mode_label) in enumerate(mode_specs):
        ax = axes[0, col]
        for workload in ["clustered", "random"]:
            df = data[(workload, error_token)]
            cumulative = df["Time (sec)"].cumsum()
            ax.plot(
                df["query"],
                cumulative,
                color=WORKLOAD_COLORS[workload],
                label=WORKLOAD_LABELS[workload],
            )
        ax.set_title(f"{mode_label} — Cumulative Query Time")
        ax.set_ylabel("Cumulative time (sec)")
        if col == 0:
            ax.legend(ncol=2, loc="upper left")

        ax = axes[1, col]
        for workload in ["clustered", "random"]:
            df = data[(workload, error_token)]
            cumulative_mean = df["Time (sec)"].cumsum() / df["query"]
            tail_mean = summaries[(workload, error_token)].last50_time
            ax.plot(
                df["query"],
                cumulative_mean,
                color=WORKLOAD_COLORS[workload],
                label=WORKLOAD_LABELS[workload],
            )
            ax.axhline(
                tail_mean,
                color=WORKLOAD_COLORS[workload],
                linestyle="--",
                linewidth=0.9,
                alpha=0.6,
            )
        ax.set_title(f"{mode_label} — Cumulative Mean")
        ax.set_xlabel("Query index")
        ax.set_ylabel("Mean time so far (sec/query)")
        ax.text(
            0.03,
            0.05,
            "dashed = last-50 mean",
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=7,
            color="#444444",
        )

    fig.suptitle(f"{dataset_label} — Cumulative Adaptation View (n={query_count})", y=1.02, fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    return save(fig, f"{output_prefix}_cumulative_n{query_count}", formats=("pdf", "png"))


def make_method_comparison_plot(
    comparison_data: dict[tuple[str, str], pd.DataFrame],
    *,
    dataset_label: str,
    output_prefix: str,
    compare_error: str,
    query_count: int,
) -> list[Path]:
    fig, axes = plt.subplots(2, 2, figsize=(7.0, 4.8), sharex="col")

    for col, workload in enumerate(["clustered", "random"]):
        ax = axes[0, col]
        for method in ["valinor_a", "valinor_s"]:
            df = comparison_data[(workload, method)]
            ax.plot(
                df["query"],
                rolling_mean(df["Time (sec)"], 25),
                color=METHOD_COLORS[method],
                label=METHOD_LABELS[method],
            )
        ax.set_title(f"{WORKLOAD_LABELS[workload]} — eb={compare_error} Query Time")
        ax.set_ylabel("Time (sec)")
        if col == 0:
            ax.legend(ncol=2, loc="upper right")

        ax = axes[1, col]
        for method in ["valinor_a", "valinor_s"]:
            df = comparison_data[(workload, method)]
            cumulative_mean = df["Time (sec)"].cumsum() / df["query"]
            ax.plot(
                df["query"],
                cumulative_mean,
                color=METHOD_COLORS[method],
                label=METHOD_LABELS[method],
            )
        ax.set_title(f"{WORKLOAD_LABELS[workload]} — eb={compare_error} Cumulative Mean")
        ax.set_xlabel("Query index")
        ax.set_ylabel("Mean time so far (sec/query)")

    fig.suptitle(f"{dataset_label} — Valinor-A vs Valinor-S at eb={compare_error}", y=1.02, fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    return save(fig, f"{output_prefix}_method_compare_eb{compare_error.replace('.', 'p')}_n{query_count}", formats=("pdf", "png"))


def write_summary(
    summaries: dict[tuple[str, str], SeriesSummary],
    *,
    data: dict[tuple[str, str], pd.DataFrame],
    comparison_data: dict[tuple[str, str], pd.DataFrame],
    dataset_label: str,
    random_scenario: str,
    clustered_scenario: str,
    output_prefix: str,
    compare_error: str,
    object_count: int | None,
    mcols: int,
    resolution: int,
    subtile_ratio: str,
    query_count: int,
    run: int,
    window: int,
    output_dir: Path,
) -> Path:
    lines = []
    lines.append(f"Adaptation analysis for {dataset_label}")
    lines.append(
        f"Configuration: random={random_scenario}, clustered={clustered_scenario}, "
        f"mcols={mcols}, res={resolution}, subtileRatio={subtile_ratio}, n={query_count}, outK={out_k}, run={run}"
    )
    lines.append(f"Window size for asymptote diagnostics: {window} queries")
    lines.append("")
    lines.append("Per-series summary")
    lines.append("workload\terror\tfirst50_time\tlast50_time\ttime_drop_pct\ttail_delta_pct\tfirst50_ios\tlast50_ios\tio_drop_pct\tio_tail_delta_pct\tstatus")

    order = [
        ("clustered", "0"),
        ("clustered", "0.01"),
        ("clustered", "0.02"),
        ("clustered", "0.05"),
        ("clustered", "0.1"),
        ("random", "0"),
        ("random", "0.01"),
        ("random", "0.02"),
        ("random", "0.05"),
        ("random", "0.1"),
    ]
    for key in order:
        summary = summaries[key]
        lines.append(
            "\t".join(
                [
                    summary.workload,
                    summary.error_token,
                    f"{summary.first50_time:.4f}",
                    f"{summary.last50_time:.4f}",
                    f"{summary.time_drop_pct:+.2f}",
                    f"{summary.tail_delta_pct:+.2f}",
                    f"{summary.first50_ios:.1f}",
                    f"{summary.last50_ios:.1f}",
                    f"{summary.io_drop_pct:+.2f}",
                    f"{summary.io_tail_delta_pct:+.2f}",
                    asymptote_label(summary),
                ]
            )
        )

    lines.append("")
    if object_count is not None:
        lines.append("Realized selectivity (exact runs)")
        lines.append("workload\tmean\tstd\tmin\tmax")
        for workload in ["random", "clustered"]:
            stats = realized_selectivity_stats(data[(workload, "0")], object_count)
            lines.append(
                "\t".join(
                    [
                        workload,
                        f"{stats['mean']:.6%}",
                        f"{stats['std']:.6%}",
                        f"{stats['min']:.6%}",
                        f"{stats['max']:.6%}",
                    ]
                )
            )
        lines.append("")

    lines.append("Takeaways")
    clustered_exact = summaries[("clustered", "0")]
    clustered_approx = summaries[("clustered", "0.01")]
    random_exact = summaries[("random", "0")]
    random_approx = summaries[("random", "0.01")]
    lines.append(
        f"- Clustered exact still improves materially through the tail: {clustered_exact.first50_time:.3f}s -> {clustered_exact.last50_time:.3f}s "
        f"({clustered_exact.time_drop_pct:+.1f}%), with the last-{window} window still {clustered_exact.tail_delta_pct:+.1f}% versus the prior one."
    )
    lines.append(
        f"- Clustered approximate eb=0.01 also still improves late, but more gently: {clustered_approx.first50_time:.3f}s -> {clustered_approx.last50_time:.3f}s "
        f"({clustered_approx.time_drop_pct:+.1f}%), tail delta {clustered_approx.tail_delta_pct:+.1f}%."
    )
    lines.append(
        f"- Random exact shows only weak net adaptation: {random_exact.first50_time:.3f}s -> {random_exact.last50_time:.3f}s "
        f"({random_exact.time_drop_pct:+.1f}%), and the final window bounces {random_exact.tail_delta_pct:+.1f}% versus the previous one."
    )
    lines.append(
        f"- Random approximate eb=0.01 is closer to flat than clustered, with {random_approx.time_drop_pct:+.1f}% total change and tail delta {random_approx.tail_delta_pct:+.1f}%."
    )
    lines.append(
        "- At higher approximate error bounds (0.05 and 0.1), both workloads are much closer to asymptotic behavior than exact mode."
    )
    if comparison_data:
        clustered_a = comparison_data[("clustered", "valinor_a")]["Time (sec)"].mean()
        clustered_s = comparison_data[("clustered", "valinor_s")]["Time (sec)"].mean()
        random_a = comparison_data[("random", "valinor_a")]["Time (sec)"].mean()
        random_s = comparison_data[("random", "valinor_s")]["Time (sec)"].mean()
        lines.append(
            f"- At eb={compare_error}, Valinor-A is {clustered_s/clustered_a:.2f}x faster than Valinor-S on clustered and {random_s/random_a:.2f}x faster on random, which is the direct runtime signal of adaptation plus metadata reuse."
        )
    lines.append("")
    lines.append("Notes")
    lines.append("- Query index i=0 (initialization query) is excluded from all trend plots and window statistics.")
    lines.append("- In the cumulative-mean panels, the dashed horizontal reference is the mean query time over the final 50-query window.")
    if not comparison_data:
        lines.append("- Matching Valinor-S runs for the comparison error bound were not present, so the method-comparison figure was skipped.")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{output_prefix}_summary_n{query_count}.txt"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def main() -> int:
    args = parse_args()
    use()

    data: dict[tuple[str, str], pd.DataFrame] = {}
    summaries: dict[tuple[str, str], SeriesSummary] = {}
    comparison_data: dict[tuple[str, str], pd.DataFrame] = {}

    for workload in ["clustered", "random"]:
        for error_token in ["0", *args.approx_errors]:
            df = load_series(
                args.results_dir,
                workload,
                error_token,
                args=args,
                mcols=args.mcols,
                resolution=args.resolution,
                subtile_ratio=args.subtile_ratio,
                query_count=args.query_count,
                run=args.run,
            )
            data[(workload, error_token)] = df
            summaries[(workload, error_token)] = summarize_series(
                df,
                workload=workload,
                error_token=error_token,
                window=args.window,
            )

    for workload in ["clustered", "random"]:
        try:
            comparison_data[(workload, "valinor_a")] = load_series(
                args.results_dir,
                workload,
                args.compare_error,
                args=args,
                method="valinor_a",
                mcols=args.mcols,
                resolution=args.resolution,
                subtile_ratio=args.subtile_ratio,
                query_count=args.query_count,
                run=args.run,
            )
            comparison_data[(workload, "valinor_s")] = load_series(
                args.results_dir,
                workload,
                args.compare_error,
                args=args,
                method="valinor_s",
                mcols=args.mcols,
                resolution=args.resolution,
                subtile_ratio=args.subtile_ratio,
                query_count=args.query_count,
                run=args.run,
            )
        except FileNotFoundError:
            comparison_data = {}
            break

    adaptation_paths = make_adaptation_effect_plot(
        data,
        args.approx_errors,
        dataset_label=args.dataset_label,
        output_prefix=args.output_prefix,
        query_count=args.query_count,
    )
    asymptote_paths = make_asymptote_plot(
        data,
        summaries,
        dataset_label=args.dataset_label,
        output_prefix=args.output_prefix,
        query_count=args.query_count,
        window=args.window,
    )
    cumulative_paths = make_cumulative_plot(
        data,
        summaries,
        dataset_label=args.dataset_label,
        output_prefix=args.output_prefix,
        query_count=args.query_count,
    )
    compare_paths = []
    if comparison_data:
        compare_paths = make_method_comparison_plot(
            comparison_data,
            dataset_label=args.dataset_label,
            output_prefix=args.output_prefix,
            compare_error=args.compare_error,
            query_count=args.query_count,
        )
    summary_path = write_summary(
        summaries,
        data=data,
        comparison_data=comparison_data,
        dataset_label=args.dataset_label,
        random_scenario=args.random_scenario,
        clustered_scenario=args.clustered_scenario,
        output_prefix=args.output_prefix,
        compare_error=args.compare_error,
        object_count=args.object_count,
        mcols=args.mcols,
        resolution=args.resolution,
        subtile_ratio=args.subtile_ratio,
        query_count=args.query_count,
        out_k=args.out_k,
        run=args.run,
        window=args.window,
        output_dir=EXPERIMENTS_DIR / "plots",
    )

    print("Wrote:")
    for path in [*adaptation_paths, *asymptote_paths, *cumulative_paths, *compare_paths, summary_path]:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())