#!/usr/bin/env python3
"""Check whether a scenario's realised average selectivity matches its target.

Usage examples:
    python experiments/analysis/check_scenario_selectivity.py taxi_clustered
    python experiments/analysis/check_scenario_selectivity.py taxi_random
    python experiments/analysis/check_scenario_selectivity.py gaia_dr3_clustered --mode reservoir

Notes:
    - The scenario name is always passed as a parameter. No scenarios are hard-coded.
    - Default mode is auto: prefer an exact error0 results CSV and fall back to the
        saved canonical query sequence plus spatial reservoir when no exact results are available.
    - Use --mode exact or --mode reservoir to force a verification path.
    - Use --exact-file to point to a specific exact-results CSV.
    - Use --results-root, --query-root, or --scenarios-file to override locations.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
from dataclasses import dataclass
from pathlib import Path
from statistics import median


REPO_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS_DIR = REPO_ROOT / "experiments"
DEFAULT_SCENARIOS_FILE = REPO_ROOT / "src/main/resources/experiments/experiment_scenarios.yaml"

COUNT_PATTERN = re.compile(r"count=([0-9.eE+-]+)")
ROWS_ACCEPTED_PATTERN = re.compile(r"rowsAccepted=(\d+)")
RUN_PATTERN = re.compile(r"_run(\d+)\.csv$")
MCOLS_PATTERN = re.compile(r"results_mcols(\d+)_")


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    object_count: int | None


@dataclass(frozen=True)
class ScenarioConfig:
    name: str
    dataset: str
    query_count: int | None
    target_selectivity: float | None
    seed: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check whether a workload scenario's realised average selectivity matches its configured target. "
            "The script prefers exact error=0 results when available and otherwise falls back to the saved "
            "query sequence plus spatial reservoir."
        ),
        epilog=(
            "Examples:\n"
            "  python experiments/analysis/check_scenario_selectivity.py taxi_clustered\n"
            "  python experiments/analysis/check_scenario_selectivity.py taxi_random\n"
            "  python experiments/analysis/check_scenario_selectivity.py gaia_dr3_clustered --mode reservoir\n\n"
            "Notes:\n"
            "  - The scenario name is always passed as a parameter. No scenarios are hard-coded.\n"
            "  - Default mode is auto: prefer an exact error0 results CSV and fall back to the saved canonical query sequence plus reservoir.\n"
            "  - Use --mode exact or --mode reservoir to force a verification path.\n"
            "  - Use --exact-file to point to a specific exact-results CSV.\n"
            "  - Use --results-root, --query-root, or --scenarios-file to override locations."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("scenario", help="Scenario name from experiment_scenarios.yaml, e.g. taxi_clustered")
    parser.add_argument(
        "--mode",
        choices=("auto", "exact", "reservoir"),
        default="auto",
        help="Verification source: exact results, reservoir estimate, or auto-detect (default: auto)",
    )
    parser.add_argument(
        "--results-root",
        type=Path,
        default=EXPERIMENTS_DIR / "results",
        help="Base results directory (default: experiments/results)",
    )
    parser.add_argument(
        "--query-root",
        type=Path,
        default=EXPERIMENTS_DIR / "query_sequences",
        help="Query-sequence directory (default: experiments/query_sequences)",
    )
    parser.add_argument(
        "--scenarios-file",
        type=Path,
        default=DEFAULT_SCENARIOS_FILE,
        help="Path to experiment_scenarios.yaml",
    )
    parser.add_argument(
        "--method",
        default="valinor_a",
        help="Method subdirectory to inspect for exact results (default: valinor_a)",
    )
    parser.add_argument(
        "--run",
        type=int,
        default=1,
        help="Preferred run number when auto-selecting an exact results CSV (default: 1)",
    )
    parser.add_argument(
        "--exact-file",
        type=Path,
        default=None,
        help="Explicit exact-results CSV to use instead of auto-discovery",
    )
    return parser.parse_args()


def strip_comments(line: str) -> str:
    if "#" not in line:
        return line.rstrip("\n")
    prefix, _comment = line.split("#", 1)
    return prefix.rstrip()


def parse_scalar(value: str) -> str:
    value = value.strip()
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]
    return value


def load_configs(path: Path) -> tuple[dict[str, DatasetConfig], dict[str, ScenarioConfig]]:
    datasets: dict[str, dict[str, str]] = {}
    scenarios: dict[str, dict[str, str]] = {}
    section: str | None = None
    current_name: str | None = None
    in_workload = False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = strip_comments(raw_line)
        if not line.strip():
            continue

        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        if indent == 0:
            if stripped == "datasets:":
                section = "datasets"
            elif stripped == "scenarios:":
                section = "scenarios"
            else:
                section = None
            current_name = None
            in_workload = False
            continue

        if section == "datasets":
            if indent == 2 and stripped.endswith(":"):
                current_name = stripped[:-1]
                datasets[current_name] = {}
                continue
            if indent == 4 and current_name and ":" in stripped:
                key, value = stripped.split(":", 1)
                datasets[current_name][key.strip()] = parse_scalar(value)
            continue

        if section == "scenarios":
            if indent == 2 and stripped.endswith(":"):
                current_name = stripped[:-1]
                scenarios[current_name] = {}
                in_workload = False
                continue
            if not current_name or ":" not in stripped:
                continue
            if indent == 4:
                key, value = stripped.split(":", 1)
                key = key.strip()
                if key == "workload":
                    in_workload = True
                else:
                    in_workload = False
                    scenarios[current_name][key] = parse_scalar(value)
                continue
            if indent == 6 and in_workload:
                key, value = stripped.split(":", 1)
                scenarios[current_name][f"workload.{key.strip()}"] = parse_scalar(value)

    dataset_configs = {
        name: DatasetConfig(
            name=name,
            object_count=int(values["objectCount"]) if values.get("objectCount") else None,
        )
        for name, values in datasets.items()
    }
    scenario_configs = {
        name: ScenarioConfig(
            name=name,
            dataset=values.get("dataset", ""),
            query_count=int(values["workload.seqCount"]) if values.get("workload.seqCount") else None,
            target_selectivity=float(values["workload.selectivity"]) if values.get("workload.selectivity") else None,
            seed=int(values.get("workload.seed", 0) or 0),
        )
        for name, values in scenarios.items()
    }
    return dataset_configs, scenario_configs


def parse_query_rectangles(path: Path) -> list[tuple[float, float, float, float]]:
    rects: list[tuple[float, float, float, float]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            rect_part = line.split("|", 1)[0]
            try:
                xpart, ypart = rect_part.split("),(")
                xmin_s, xmax_s = xpart[1:].split("..")
                ymin_s, ymax_s = ypart[:-1].split("..")
            except ValueError as exc:
                raise ValueError(f"Could not parse query rectangle from line: {line}")
            rects.append((float(xmin_s), float(xmax_s), float(ymin_s), float(ymax_s)))
    return rects


def parse_reservoir(path: Path) -> tuple[list[tuple[float, float]], int | None, str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        header = handle.readline().strip()
        rows_accepted_match = ROWS_ACCEPTED_PATTERN.search(header)
        rows_accepted = int(rows_accepted_match.group(1)) if rows_accepted_match else None
        reader = csv.DictReader(handle)
        points = [(float(row["x"]), float(row["y"])) for row in reader]
    return points, rows_accepted, header


def run_number(path: Path) -> int:
    match = RUN_PATTERN.search(path.name)
    return int(match.group(1)) if match else math.inf


def mcols_number(path: Path) -> int:
    match = MCOLS_PATTERN.search(path.name)
    return int(match.group(1)) if match else math.inf


def find_exact_results_file(args: argparse.Namespace, scenario: str) -> Path | None:
    if args.exact_file is not None:
        return args.exact_file

    method_dir = args.results_root / scenario / args.method
    if not method_dir.exists():
        return None

    candidates = sorted(
        method_dir.glob("results_mcols*_error0_res*_run*.csv"),
        key=lambda path: (abs(run_number(path) - args.run), run_number(path), mcols_number(path), path.name),
    )
    return candidates[0] if candidates else None


def parse_total_count(value: str) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed


def recover_count_from_result(query_result: str) -> float | None:
    match = COUNT_PATTERN.search(query_result or "")
    if not match:
        return None
    return float(match.group(1))


def summarize_exact_results(path: Path) -> tuple[dict[str, float | int | str], int | None]:
    rows = list(csv.DictReader(path.open("r", encoding="utf-8", newline="")))
    counts: list[float] = []
    recovered_rows = 0

    for row in rows:
        total_count = parse_total_count(row.get("Total Count", ""))
        if total_count is None or total_count <= 0.0:
            recovered = recover_count_from_result(row.get("Query Result", ""))
            if recovered is not None:
                total_count = recovered
                recovered_rows += 1
        if total_count is None:
            continue
        counts.append(total_count)

    if not counts:
        raise ValueError(f"No usable counts found in {path}")

    return (
        {
            "source": "exact",
            "source_path": str(path),
            "rows_in_results": len(rows),
            "usable_queries": len(counts),
            "recovered_count_rows": recovered_rows,
            "mean_count": float(sum(counts) / len(counts)),
            "median_count": float(median(counts)),
            "min_count": float(min(counts)),
            "max_count": float(max(counts)),
        },
        len(rows),
    )


def summarize_reservoir(query_path: Path, reservoir_path: Path) -> tuple[dict[str, float | int | str], int | None, int | None]:
    rects = parse_query_rectangles(query_path)
    points, rows_accepted, header = parse_reservoir(reservoir_path)
    if not points:
        raise ValueError(f"Reservoir file is empty: {reservoir_path}")

    selectivities: list[float] = []
    for xmin, xmax, ymin, ymax in rects:
        hits = sum(1 for x, y in points if xmin <= x <= xmax and ymin <= y <= ymax)
        selectivities.append(hits / len(points))

    mean_selectivity = float(sum(selectivities) / len(selectivities))
    summary = {
        "source": "reservoir",
        "source_path": str(query_path),
        "reservoir_path": str(reservoir_path),
        "reservoir_header": header,
        "usable_queries": len(selectivities),
        "mean_selectivity": mean_selectivity,
        "median_selectivity": float(median(selectivities)),
        "min_selectivity": float(min(selectivities)),
        "max_selectivity": float(max(selectivities)),
    }
    if rows_accepted is not None:
        summary["rows_accepted"] = rows_accepted
        summary["estimated_mean_count"] = mean_selectivity * rows_accepted
    return summary, len(rects), rows_accepted


def find_reservoir_file(query_root: Path, dataset: str, seed: int) -> Path | None:
    reservoir_dir = query_root / "reservoirs"
    candidates = sorted(reservoir_dir.glob(f"{dataset}_R*_seed{seed}.csv"))
    return candidates[0] if candidates else None


def find_query_sequence_file(query_root: Path, scenario: str) -> Path | None:
    candidate = query_root / f"{scenario}.txt"
    return candidate if candidate.exists() else None


def resolve_denominator(dataset_cfg: DatasetConfig, reservoir_rows_accepted: int | None) -> int | None:
    return reservoir_rows_accepted if reservoir_rows_accepted is not None else dataset_cfg.object_count


def print_summary(summary: dict[str, float | int | str], scenario_cfg: ScenarioConfig, denominator: int | None) -> None:
    target = scenario_cfg.target_selectivity
    if summary["source"] == "exact":
        mean_count = float(summary["mean_count"])
        mean_selectivity = mean_count / denominator if denominator else None
        print(f"scenario              : {scenario_cfg.name}")
        print(f"dataset               : {scenario_cfg.dataset}")
        print(f"verification source   : exact")
        print(f"results csv           : {summary['source_path']}")
        print(f"queries in results    : {summary['rows_in_results']}")
        print(f"usable queries        : {summary['usable_queries']}")
        print(f"recovered zero-counts : {summary['recovered_count_rows']}")
        print(f"mean exact count      : {mean_count:.6f}")
        print(f"median exact count    : {float(summary['median_count']):.6f}")
        print(f"min exact count       : {float(summary['min_count']):.6f}")
        print(f"max exact count       : {float(summary['max_count']):.6f}")
        if denominator:
            print(f"denominator rows      : {denominator}")
            print(f"mean selectivity      : {mean_selectivity:.8f}")
            if target is not None:
                abs_delta = mean_selectivity - target
                rel_delta = abs_delta / target if target else float("nan")
                print(f"target selectivity    : {target:.8f}")
                print(f"absolute delta        : {abs_delta:+.8f}")
                print(f"relative delta        : {rel_delta:+.4%}")
        return

    mean_selectivity = float(summary["mean_selectivity"])
    print(f"scenario              : {scenario_cfg.name}")
    print(f"dataset               : {scenario_cfg.dataset}")
    print(f"verification source   : reservoir")
    print(f"query sequence        : {summary['source_path']}")
    print(f"reservoir csv         : {summary['reservoir_path']}")
    print(f"usable queries        : {summary['usable_queries']}")
    print(f"mean selectivity      : {mean_selectivity:.8f}")
    print(f"median selectivity    : {float(summary['median_selectivity']):.8f}")
    print(f"min selectivity       : {float(summary['min_selectivity']):.8f}")
    print(f"max selectivity       : {float(summary['max_selectivity']):.8f}")
    if "rows_accepted" in summary:
        print(f"rows accepted         : {int(summary['rows_accepted'])}")
    if "estimated_mean_count" in summary:
        print(f"estimated mean count  : {float(summary['estimated_mean_count']):.6f}")
    if target is not None:
        abs_delta = mean_selectivity - target
        rel_delta = abs_delta / target if target else float("nan")
        print(f"target selectivity    : {target:.8f}")
        print(f"absolute delta        : {abs_delta:+.8f}")
        print(f"relative delta        : {rel_delta:+.4%}")


def main() -> int:
    args = parse_args()
    dataset_cfgs, scenario_cfgs = load_configs(args.scenarios_file)

    if args.scenario not in scenario_cfgs:
        known = ", ".join(sorted(scenario_cfgs)[:8])
        raise SystemExit(f"Unknown scenario '{args.scenario}'. Example known scenarios: {known}")

    scenario_cfg = scenario_cfgs[args.scenario]
    if scenario_cfg.dataset not in dataset_cfgs:
        raise SystemExit(f"Dataset '{scenario_cfg.dataset}' for scenario '{args.scenario}' is missing from config")
    dataset_cfg = dataset_cfgs[scenario_cfg.dataset]

    exact_path = find_exact_results_file(args, args.scenario)
    reservoir_path = find_reservoir_file(args.query_root, scenario_cfg.dataset, scenario_cfg.seed)
    query_path = find_query_sequence_file(args.query_root, args.scenario)

    mode = args.mode
    if mode == "auto":
        mode = "exact" if exact_path is not None else "reservoir"

    if mode == "exact":
        if exact_path is None or not exact_path.exists():
            raise SystemExit(
                f"No exact results CSV found for scenario '{args.scenario}' under {args.results_root / args.scenario / args.method}"
            )
        summary, observed_query_count = summarize_exact_results(exact_path)
        denominator = resolve_denominator(dataset_cfg, parse_reservoir(reservoir_path)[1] if reservoir_path and reservoir_path.exists() else None)
        if scenario_cfg.query_count is not None and observed_query_count != scenario_cfg.query_count:
            print(
                f"warning: expected {scenario_cfg.query_count} query rows from config but found {observed_query_count} in {exact_path}",
            )
        print_summary(summary, scenario_cfg, denominator)
        return 0

    if query_path is None or not query_path.exists():
        raise SystemExit(
            f"Query sequence not found for scenario '{args.scenario}' under {args.query_root} "
            f"(expected {args.scenario}.txt)"
        )
    if reservoir_path is None or not reservoir_path.exists():
        raise SystemExit(
            f"Reservoir CSV not found for dataset '{scenario_cfg.dataset}' and seed {scenario_cfg.seed} under {args.query_root / 'reservoirs'}"
        )

    summary, observed_query_count, rows_accepted = summarize_reservoir(query_path, reservoir_path)
    if scenario_cfg.query_count is not None and observed_query_count != scenario_cfg.query_count:
        print(
            f"warning: expected {scenario_cfg.query_count} queries from config but found {observed_query_count} in {query_path}",
        )
    _denominator = resolve_denominator(dataset_cfg, rows_accepted)
    print_summary(summary, scenario_cfg, _denominator)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())