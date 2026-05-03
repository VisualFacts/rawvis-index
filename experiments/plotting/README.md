# Plotting — VLDB camera-ready figures

Library + CLI for the Valinor approximate-query experiments. Pure Python,
no notebooks, fully reproducible from the command line.

## Layout

```
plotting/
  config.py          # methods, scenarios (auto-discovered), defaults
  loader.py          # CSV parse + parquet cache → tidy DataFrames
  metrics.py         # GT-relative error, per-config aggregates
  style.py           # camera-ready matplotlib RC, save() helper
  plots/
    response_time.py # per-query, CDF, sweep over error_bound / mcols
    error.py         # observed-error CDF + sweep
    workload.py      # stacked init + queries bar
    init_cost.py     # init-only bar
    io_sampling.py   # I/Os, sampling-rate (Valinor only)
    scenario_sweep.py# selectivity / dataset-size sweeps across scenarios
  cli.py             # argparse entry: `python -m plotting.cli ...`
```

The legacy notebook-based pipeline is preserved under `plotting_legacy/`.

## Quick start

```bash
source venv/bin/activate
cd experiments

# What scenarios were discovered?
python -m plotting.cli list-scenarios

# A single plot (one PDF written under experiments/plots/)
python -m plotting.cli per-query \
  --scenario synth10_300M_clustered_sel1 --error-bound 0.01

# Everything for every scenario, into per-scenario subdirs
python -m plotting.cli all
```

Outputs are vector PDFs (TrueType embedded, no titles, serif Times/STIX,
single-column 3.33 in × 2.05 in). Drop them straight into LaTeX with
`\includegraphics`. Use `\caption` in the paper for figure text.

## Design choices

- **Ground-truth error**: every approximate result is joined to an exact
  result for the same `(scenario, mcols, q, measure_id)`. DuckDB is used
  when present; otherwise Valinor-A with `error_bound=0` is used. Reported
  error is `|est - gt| / |gt|`, averaged across measures per query.
- **Init handling**: CSV row `i=0` is the cold query and includes
  initialization work. Plotting exposes this as `q=0`; warm-query plots
  exclude it by default and `--include-init` includes it. Workload bars stack
  cold-start time (lighter shade) with the warm-query sum (full color).
- **Query-index normalization**: `q` follows the CSV `i` value for every
  method. Result filenames can include `_n<N>`; this is the effective number
  of query rows in that file, so slow baselines may use a shorter prefix.
- **Run aggregation**: mean ± std across runs (error bars / shaded band).
- **Valinor-Exact**: synthesized as a view of `valinor_a` rows with
  `error_bound==0`; auto-included in non-error-sweep plots so the cost
  of "no approximation" is visible.
- **Caching**: parsed CSVs go to `experiments/.plot_cache/*.parquet`;
  cache is invalidated automatically on file mtime change.

## Adding a new plot

1. Add `make_xxx(queries|measures, scenario, *, ...) -> Figure` in a
   `plots/<topic>.py`. Use `style.style_for(method_key)` for line styles
   and `style.bar_style_for(...)` for bars.
2. Add a `cmd_xxx` + `sub.add_parser("xxx")` in `cli.py`.
3. Test: `python -m plotting.cli xxx --scenario <s>`.

No need to touch the loader unless a new method or new CSV column is
introduced — see `loader._parse_query_result` / `_parse_init_timing`
for the per-method regexes.

## Adding a new method or scenario

- **New scenario**: just create `experiments/results/<name>/`
  matching one of the parsed patterns
  (`synth<N>_<rows>_<clustered|random|exploratory>_sel<sel>` or
  `<dataset>_<clustered|random|exploratory>`).
  It will auto-appear in `list-scenarios`.
- **New method**: add an entry to `config.METHODS`, set its `subdir`,
  filename regex (`loader._RE_*`), and a `Query Result` parser in
  `loader._parse_query_result`.
