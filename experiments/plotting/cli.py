"""CLI entry point. Run from anywhere:

    python -m plotting.cli all
    python -m plotting.cli per-query --scenario taxi_zoom --error-bound 0.05
    python -m plotting.cli error-cdf --scenario synth10_300M_pan_sel1
    python -m plotting.cli sweep --vary selectivity
    python -m plotting.cli list-scenarios
    python -m plotting.cli refresh-cache
"""
from __future__ import annotations

import argparse
import sys
from typing import Callable

from . import config, loader, style as st
from .plots import error, init_cost, io_sampling, response_time, scenario_sweep, workload


def _add_common(p: argparse.ArgumentParser, *, needs_scenario: bool = True):
    if needs_scenario:
        p.add_argument("--scenario", required=True, help="Scenario directory name")
    p.add_argument("--mcols", type=int, default=None)
    p.add_argument("--error-bound", type=float, default=config.DEFAULT_ERROR_BOUND)
    p.add_argument("--methods", nargs="+", default=None)
    p.add_argument("--include-init", action="store_true")
    p.add_argument("--name", default=None, help="Override output file stem")
    p.add_argument("--subdir", default="", help="Output subdirectory under experiments/plots/")


def _save(fig, args, default_name: str):
    name = args.name or default_name
    paths = st.save(fig, name, subdir=args.subdir)
    print("wrote", *paths, sep="\n  ")


def _stem(prefix: str, args, *, with_error: bool = True) -> str:
    """Filename stem encoding the resolved fixed parameters."""
    sc = config.parse_scenario(args.scenario) if getattr(args, "scenario", None) else None
    mcols = args.mcols if args.mcols is not None else (sc.default_mcols if sc else config.DEFAULT_MCOLS)
    parts = [prefix, args.scenario, f"m{mcols}"]
    if with_error:
        parts.append(f"e{args.error_bound}")
    return "_".join(parts)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(prog="plotting", description="VLDB camera-ready plots")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list-scenarios"); p.set_defaults(fn=cmd_list)
    p = sub.add_parser("refresh-cache"); p.set_defaults(fn=cmd_refresh)

    p = sub.add_parser("per-query"); _add_common(p); p.set_defaults(fn=cmd_per_query)
    p = sub.add_parser("cdf");       _add_common(p); p.set_defaults(fn=cmd_cdf)
    p = sub.add_parser("error-cdf"); _add_common(p); p.set_defaults(fn=cmd_error_cdf)
    p = sub.add_parser("error-sweep"); _add_common(p); p.set_defaults(fn=cmd_error_sweep)
    p = sub.add_parser("rt-sweep-error"); _add_common(p); p.set_defaults(fn=cmd_rt_error_sweep)
    p = sub.add_parser("rt-sweep-mcols"); _add_common(p); p.set_defaults(fn=cmd_rt_mcols_sweep)
    p = sub.add_parser("workload");   _add_common(p); p.set_defaults(fn=cmd_workload)
    p = sub.add_parser("init");       _add_common(p); p.set_defaults(fn=cmd_init)
    p = sub.add_parser("io-per-query"); _add_common(p); p.set_defaults(fn=cmd_io_pq)
    p = sub.add_parser("sampling");   _add_common(p); p.set_defaults(fn=cmd_sampling)

    p = sub.add_parser("sweep")
    _add_common(p, needs_scenario=False)
    p.add_argument("--vary", choices=["selectivity", "rows"], required=True)
    p.add_argument("--dataset", default="synth10")
    p.add_argument("--pattern", default="pan")
    p.add_argument("--pin-rows", default="300M",
                   help="Rows held fixed when vary=selectivity (default 300M)")
    p.add_argument("--pin-selectivity", type=float, default=1.0,
                   help="Selectivity %% held fixed when vary=rows (default 1)")
    p.set_defaults(fn=cmd_sweep)

    p = sub.add_parser("all"); p.set_defaults(fn=cmd_all)

    parser.add_argument("--no-annotate", action="store_true",
                        help="Suppress the small 'fixed: ...' stamp above each "
                             "axes (use for camera-ready figures).")
    args = parser.parse_args(argv)
    st.use()
    st.set_annotate_fixed(not getattr(args, "no_annotate", False))
    args.fn(args)


# ---------------------------------------------------------------------------
def cmd_list(args):
    scen = config.discover_scenarios()
    for k, sc in scen.items():
        print(f"  {k:40s}  {sc.label}")


def cmd_refresh(args):
    for old in config.CACHE_DIR.glob("*.parquet"):
        old.unlink()
    print("cache cleared")
    loader.load()
    print("cache rebuilt")


def _load(args, scenario):
    return loader.load([scenario] if scenario else None)


def cmd_per_query(args):
    ds = _load(args, args.scenario)
    fig = response_time.per_query(ds.queries, args.scenario, mcols=args.mcols,
                                  error_bound=args.error_bound, methods=args.methods,
                                  include_init=args.include_init)
    _save(fig, args, _stem("per_query", args))


def cmd_cdf(args):
    ds = _load(args, args.scenario)
    fig = response_time.cdf(ds.queries, args.scenario, mcols=args.mcols,
                            error_bound=args.error_bound, methods=args.methods,
                            include_init=args.include_init)
    _save(fig, args, _stem("cdf_rt", args))


def cmd_error_cdf(args):
    ds = _load(args, args.scenario)
    fig = error.cdf(ds.measures, args.scenario, mcols=args.mcols,
                    error_bound=args.error_bound, methods=args.methods)
    _save(fig, args, _stem("cdf_err", args))


def cmd_error_sweep(args):
    ds = _load(args, args.scenario)
    fig = error.sweep_error_bound(ds.measures, args.scenario, mcols=args.mcols,
                                  methods=args.methods)
    _save(fig, args, _stem("err_sweep", args, with_error=False))


def cmd_rt_error_sweep(args):
    ds = _load(args, args.scenario)
    fig = response_time.sweep_error_bound(ds.queries, args.scenario, mcols=args.mcols,
                                          methods=args.methods)
    _save(fig, args, _stem("rt_sweep_error", args, with_error=False))


def cmd_rt_mcols_sweep(args):
    ds = _load(args, args.scenario)
    fig = response_time.sweep_mcols(ds.queries, args.scenario,
                                    error_bound=args.error_bound, methods=args.methods)
    _save(fig, args, f"rt_sweep_mcols_{args.scenario}_e{args.error_bound}")


def cmd_workload(args):
    ds = _load(args, args.scenario)
    fig = workload.stacked_workload(ds.queries, args.scenario, mcols=args.mcols,
                                    error_bound=args.error_bound, methods=args.methods)
    _save(fig, args, _stem("workload", args))


def cmd_init(args):
    ds = _load(args, args.scenario)
    fig = init_cost.init_bar(ds.queries, args.scenario, mcols=args.mcols,
                             error_bound=args.error_bound, methods=args.methods)
    _save(fig, args, _stem("init", args))


def cmd_io_pq(args):
    ds = _load(args, args.scenario)
    fig = io_sampling.io_per_query(ds.queries, args.scenario, mcols=args.mcols,
                                   error_bound=args.error_bound)
    _save(fig, args, _stem("io_per_query", args))


def cmd_sampling(args):
    ds = _load(args, args.scenario)
    fig = io_sampling.sampling_rate(ds.queries, args.scenario, mcols=args.mcols)
    _save(fig, args, _stem("sampling_rate", args, with_error=False))


def cmd_sweep(args):
    ds = loader.load()  # all scenarios
    fig = scenario_sweep.sweep(ds.queries, ds.scenarios, vary=args.vary,
                               dataset=args.dataset, pattern=args.pattern,
                               mcols=args.mcols, error_bound=args.error_bound,
                               pin_rows=getattr(args, "pin_rows", "300M"),
                               pin_selectivity=getattr(args, "pin_selectivity", 1.0),
                               methods=args.methods)
    _save(fig, args, f"sweep_{args.vary}_{args.dataset}_{args.pattern}")


def cmd_all(args):
    """Generate a sensible default set of plots for every scenario, plus
    the cross-scenario selectivity / scalability sweeps for synth10."""
    ds = loader.load()
    print(f"Defaults: error_bound={config.DEFAULT_ERROR_BOUND}, mcols=<scenario default>")
    for sk, sc in ds.scenarios.items():
        try:
            args.scenario = sk
            args.mcols = sc.default_mcols
            args.methods = None
            args.error_bound = config.DEFAULT_ERROR_BOUND
            args.include_init = False
            args.name = None
            args.subdir = sk
            for cmd in (cmd_per_query, cmd_cdf, cmd_error_cdf,
                        cmd_workload, cmd_init):
                cmd(args)
            try:
                cmd_error_sweep(args); cmd_rt_error_sweep(args); cmd_sampling(args)
            except Exception as e:
                print(f"  skip sweeps for {sk}: {e}")
        except Exception as e:
            print(f"failed scenario {sk}: {e}")

    # Cross-scenario sweeps (synth10 only — that's where we have the matrix).
    args.subdir = "_sweeps"
    args.scenario = None
    args.methods = None
    args.error_bound = config.DEFAULT_ERROR_BOUND
    args.mcols = None
    args.name = None
    args.pin_rows = "300M"
    args.pin_selectivity = 1.0
    for vary in ("selectivity", "rows"):
        for dataset in ("synth10",):
            args.vary = vary
            args.dataset = dataset
            args.pattern = "pan"
            try:
                cmd_sweep(args)
            except Exception as e:
                print(f"  skip sweep vary={vary} dataset={dataset}: {e}")


if __name__ == "__main__":
    main()
