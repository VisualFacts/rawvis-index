#!/bin/bash

# Keep long experiment runs alive across terminal/session disconnects.
trap '' HUP

# =============================================================================
# Experiment orchestration — runs Valinor + DuckDB + PilotDB experiments.
#
# Each experiment is dispatched to exp_valinor.sh / exp_duckdb.sh /
# exp_pilotdb.sh via env-var overrides.
#
# All experiments run under a fixed 16 GB cgroup cap (MEM_LIMIT=16G).
# JVM_XMX defaults to MEM_LIMIT - 2G (=14G) for all datasets.
#
# ---- Plan structure ---------------------------------------------------------
#
#   DATASETS       : synth10 | taxi | gaia_dr3 | ebird_us
#   WORKLOADS      : exploratory | random | clustered
#                    (synth10 has no exploratory cell — pan over uniform
#                     synthetic data is degenerate; only random + clustered.)
#   CELL_SWEEPS    : per-cell single-axis sweeps to run on every grid cell
#                    in addition to the default operating point
#                    (default: "eb nm"; empty = default-only)
#   SWEEPS         : synth-only macro sweeps
#                    (default: "selectivity scalability"; empty = none)
#
#   For every (dataset, workload) cell we always run the DEFAULT operating
#   point, plus optionally per-cell single-axis sweeps:
#
#     DEFAULT     : nm=4, eb={0, 0.01}   (paper headline + exact baseline)
#                   eb=0 runs only when METHODS includes valinor.
#     CELL_SWEEPS=eb : nm=4, eb={0, 0.01, 0.02, 0.05, 0.1}   (eb anchored)
#     CELL_SWEEPS=nm : nm={1, 2, 4, 6, 8}, eb={0, 0.01}      (nm anchored)
#                      (eb=0 is the exact-valinor baseline reported alongside
#                       DuckDB / PilotDB; PilotDB has no eb=0 mode and DuckDB
#                       has no eb axis, so they only run at eb=0.01.)
#
#   The synth macro sweeps (selectivity ladder + size scalability) are run at
#   the DEFAULT operating point only — they vary the SCENARIO axis, not the
#   per-cell sweep axes, so cross-multiplying would explode the grid without
#   adding insight.
#
# ---- Parameters (all via env vars) ------------------------------------------
#
#   RESULTS_BASE  — directory for results (default: experiments/results)
#   METHODS       — space-separated list of methods to run
#                   (default: "valinor valinor_a duckdb_table_projected pilotdb valinor_s")
#                   Recognised: valinor, valinor_a, valinor_s,
#                               duckdb_table, duckdb_table_projected,
#                               pilotdb
#   DATASETS      — datasets to include (default: all 4)
#   WORKLOADS     — workloads to include (default: all 3)
#   CELL_SWEEPS   — per-cell sweeps (default: "eb nm"; "" = default-only)
#   SWEEPS        — synth macro sweeps (default: "selectivity scalability")
#   NUM_RUNS      — number of repeated runs (default: 3)
#   RUN_START     — first run index (default: 1)
#
#   MAX_QUERIES_VALINOR — cap query count for Valinor only (default: 0=full)
#   MAX_QUERIES_DUCKDB  — cap query count for DuckDB only  (default: 100)
#   MAX_QUERIES_PILOTDB — cap query count for PilotDB only (default: 100)
#                   Truncates the scenario's seqCount to the first N queries
#                   (deterministic prefix of the full sequence). Useful when
#                   Valinor needs many queries to show its adaptation curve
#                   asymptote, but the slow constant-cost baselines only need
#                   a short prefix for an apples-to-apples comparison. Result
#                   files always include the effective query count as _n<Q>.
#                   Example: Valinor at 500 queries, baselines at 100:
#                       MAX_QUERIES_DUCKDB=100 MAX_QUERIES_PILOTDB=100 \
#                           sudo -E ./experiments/run_experiments.sh
#
# Loop order: runs are the OUTERMOST loop, so you get run 1 for all configs
# first, then run 2, etc.  This lets you collect quick initial results and add
# more runs incrementally.
#
# ---- Resumability -----------------------------------------------------------
#
# Result CSVs are written to:
#   $RESULTS_BASE/<scenario>/<approach>/results_mcols<M>_error<E>_res<R>_str<S>_n<Q>_run<R>.csv
#
# Helper scripts skip cells whose CSV already exists, so re-invocation is
# idempotent and the per-cell sweeps overlap freely with the default cell
# (the shared (nm=4, eb=0.01) point is computed exactly once).
#
# Use --dry-run to print the full resolved plan without executing anything.
#
# ---- Usage ------------------------------------------------------------------
#
#   The script must run as root (sudo) because it drops page caches between
#   runs.  Use sudo -E to preserve the calling user's environment variables.
#
#   # 0. Build first (only needed when Java sources change)
#   mvn -q -DskipTests package
#
#   # 1. Sanity-check the plan without executing
#   ./experiments/run_experiments.sh --dry-run
#
#   # 2. Default full run
#   sudo -E nohup ./experiments/run_experiments.sh \
#       >> experiments/run_experiments.log 2>&1 &
#   tail -f experiments/run_experiments.log
#
#   # Headline only: default operating point, no per-cell sweeps, no macro sweeps
#   CELL_SWEEPS="" SWEEPS="" sudo -E ./experiments/run_experiments.sh
#
#   # Only the eb sweep (skip nm sweep) on real datasets
#   DATASETS="taxi gaia_dr3 ebird_us" CELL_SWEEPS="eb" SWEEPS="" \
#       sudo -E ./experiments/run_experiments.sh
#
#   # Only synth macro sweeps
#   DATASETS="" WORKLOADS="" CELL_SWEEPS="" SWEEPS="selectivity scalability" \
#       sudo -E ./experiments/run_experiments.sh
#
#   # Add run 2 later, only Valinor-A
#   RUN_START=2 NUM_RUNS=1 METHODS="valinor_a" \
#       sudo -E ./experiments/run_experiments.sh
#
#   # Re-run approximate-only run 2 without eb=0 exact Valinor baselines
#   RUN_START=2 NUM_RUNS=1 METHODS="valinor_a" \
#       sudo -E ./experiments/run_experiments.sh
# =============================================================================

set -e
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# ---- Single-instance guard ---------------------------------------------------
# Enforce exclusive execution via flock on a stable lockfile.
# Override with RUN_EXPERIMENTS_LOCK=/path/to/file or RUN_EXPERIMENTS_NO_LOCK=1
RUN_EXPERIMENTS_LOCK=${RUN_EXPERIMENTS_LOCK:-/tmp/rawvis_run_experiments.lock}
if [[ "${RUN_EXPERIMENTS_NO_LOCK:-0}" != "1" ]]; then
    exec 9>"$RUN_EXPERIMENTS_LOCK"
    if ! flock -n 9; then
        holder=$(cat "${RUN_EXPERIMENTS_LOCK}.pid" 2>/dev/null || echo "?")
        echo "ERROR: another run_experiments.sh is already running (pid=$holder, lock=$RUN_EXPERIMENTS_LOCK)." >&2
        echo "       Wait for it to finish, kill it, or set RUN_EXPERIMENTS_NO_LOCK=1 to override (unsafe)." >&2
        exit 1
    fi
    echo $$ >"${RUN_EXPERIMENTS_LOCK}.pid"
    trap 'rm -f "${RUN_EXPERIMENTS_LOCK}.pid"' EXIT
fi

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

# Fixed cgroup memory cap for ALL experiments
export MEM_LIMIT=16G

# ---- Configurable parameters ----
export RESULTS_BASE=${RESULTS_BASE:-experiments/results}
METHODS=(${METHODS:-valinor valinor_a duckdb_table_projected pilotdb valinor_s})
DATASETS=(${DATASETS-synth10 taxi gaia_dr3 ebird_us})
WORKLOADS=(${WORKLOADS-exploratory random clustered})
CELL_SWEEPS=(${CELL_SWEEPS-eb nm})
SWEEPS=(${SWEEPS-selectivity scalability})

# Outlier-aware AQP: list of K values to test (Valinor only; ignored by other
# methods).  K=0 disables the outlier index and reproduces the pre-outlier
# code path byte-for-byte.  Multiple values cause exp_valinor.sh to loop and
# write one CSV per K; filenames use _outK<k> only for approximate runs with
# k > 0.
# Default: just K=0 to preserve historical behavior.
export OUTLIER_K_LIST=${OUTLIER_K_LIST:-0}

_total_runs=${NUM_RUNS:-3}
_run_start=${RUN_START:-1}
_run_end=$((_run_start + _total_runs - 1))
export NUM_RUNS=1   # each helper script runs exactly 1 run; we loop externally

# ---- Operating points -------------------------------------------------------
#
# DEFAULT — the headline (nm, eb) point used for every grid cell and every
# synth macro-sweep scenario. eb=0 captures exact-mode I/O and is scheduled
# only when METHODS includes valinor. eb=0.01 is the tight-error default
# reported in the paper. nm=4 is the standard projected measure count.
DEFAULT_NUM_MEASURES="${DEFAULT_NUM_MEASURES:-4}"
DEFAULT_ERROR_BOUNDS="${DEFAULT_ERROR_BOUNDS:-0 0.01}"
DEFAULT_PILOTDB_ERROR_BOUNDS="${DEFAULT_PILOTDB_ERROR_BOUNDS:-0.01}"       # pilotdb has no eb=0 mode

# Per-cell EB sweep (anchored at nm=4): all 5 error bounds. Exact eb=0 is
# scheduled only for the valinor method; approximate methods use eb>0.
SWEEP_EB_VALUES="${SWEEP_EB_VALUES:-0 0.01 0.02 0.05 0.1}"
SWEEP_EB_PILOTDB_VALUES="${SWEEP_EB_PILOTDB_VALUES:-0.01 0.02 0.05 0.1}"   # pilotdb skips eb=0

# Per-cell NM sweep: all 5 measure counts. The valinor exact baseline runs at
# eb=0; valinor_a and valinor_s run only approximate eb>0 points. DuckDB has
# no eb axis; PilotDB has no eb=0 mode so it sweeps only at eb=0.01.
SWEEP_NM_VALUES="${SWEEP_NM_VALUES:-1 2 4 6 8}"
SWEEP_NM_EB_ANCHOR="${SWEEP_NM_EB_ANCHOR:-0 0.01}"
SWEEP_NM_PILOTDB_EB_ANCHOR="${SWEEP_NM_PILOTDB_EB_ANCHOR:-0.01}"

keep_exact_error_bounds() {
    local values="$1"
    local value
    local filtered=()
    for value in $values; do
        if [[ "$value" =~ ^0+([.]0+)?$ ]]; then
            filtered+=("$value")
        fi
    done
    printf '%s' "${filtered[*]}"
}

drop_exact_error_bounds() {
    local values="$1"
    local value
    local filtered=()
    for value in $values; do
        if [[ "$value" =~ ^0+([.]0+)?$ ]]; then
            continue
        fi
        filtered+=("$value")
    done
    printf '%s' "${filtered[*]}"
}

# Synth selectivity ladder (5 scenarios).
SWEEP_SELECTIVITY_SCENARIOS="\
synth10_300M_clustered_sel001 \
synth10_300M_clustered_sel01 \
synth10_300M_clustered_sel1 \
synth10_300M_clustered_sel5 \
synth10_300M_clustered_sel10"

# Synth scalability ladder (4 sizes; 300M is already covered by the main grid).
SWEEP_SCALABILITY_SCENARIOS=(
    synth10_50M_clustered_sel1
    synth10_100M_clustered_sel1
    synth10_500M_clustered_sel1
    synth10_1B_clustered_sel1
)

run_valinor() { "$SCRIPT_DIR/exp_valinor.sh"; }
run_duckdb()  { "$SCRIPT_DIR/exp_duckdb.sh";  }
run_pilotdb() { "$SCRIPT_DIR/exp_pilotdb.sh"; }

# -- Membership helpers --------------------------------------------------------
should_run() {
    local method="$1" m
    for m in "${METHODS[@]}"; do
        [[ "$m" == "$method" ]] && return 0
    done
    return 1
}

cell_sweep_enabled() {
    local s="$1" x
    for x in "${CELL_SWEEPS[@]}"; do
        [[ "$x" == "$s" ]] && return 0
    done
    return 1
}

# Build the APPROACHES string for exp_valinor.sh from METHODS.
valinor_approx_approaches() {
    local apps=""
    should_run valinor_a && apps="$apps valinor_a"
    should_run valinor_s && apps="$apps valinor_s"
    apps="${apps# }"
    [[ -z "$apps" ]] && return 1
    echo "$apps"
}

valinor_exact_approaches() {
    should_run valinor || return 1
    echo "valinor_a"
}

# Build the MODES string for exp_duckdb.sh from METHODS.
duckdb_modes() {
    local modes=""
    should_run duckdb_table           && modes="$modes table"
    should_run duckdb_table_projected && modes="$modes tableProjected"
    modes="${modes# }"
    [[ -z "$modes" ]] && return 1
    echo "$modes"
}

# -- (dataset, workload) -> scenario name --------------------------------------
scenario_for() {
    local ds="$1" wl="$2"
    case "$ds:$wl" in
        # synth10 has no exploratory cell (pan over uniform synthetic data is
        # degenerate). The clustered cell is the synthetic locality stress
        # test; the random cell is the no-locality baseline.
        synth10:exploratory)   return 1 ;;
        synth10:clustered)     echo "synth10_300M_clustered_sel1" ;;
        synth10:random)        echo "synth10_300M_random_sel1" ;;
        taxi:exploratory)      echo "taxi_exploratory" ;;
        taxi:clustered)        echo "taxi_clustered" ;;
        taxi:random)           echo "taxi_random" ;;
        gaia_dr3:exploratory)  echo "gaia_dr3_exploratory" ;;
        gaia_dr3:clustered)    echo "gaia_dr3_clustered" ;;
        gaia_dr3:random)       echo "gaia_dr3_random" ;;
        ebird_us:exploratory)  echo "ebird_us_exploratory" ;;
        ebird_us:clustered)    echo "ebird_us_clustered" ;;
        ebird_us:random)       echo "ebird_us_random" ;;
        *) return 1 ;;
    esac
}

# -- Per-cell dispatch ---------------------------------------------------------
# Always passes explicit nm + eb to every helper, so the dry-run prints the
# full plan and helper-script defaults are never silently invoked.
#
# Usage:
#   run_cell <label> <scenarios> <num_measures> <error_bounds> \
#            <pilotdb_error_bounds> [skip_duckdb]
#
# skip_duckdb=1 omits DuckDB (used for eb sweeps, since DuckDB has no eb axis).
run_cell() {
    local label="$1"
    local scenarios="$2"
    local nm="$3"
    local eb="$4"
    local pilot_eb="$5"
    local skip_duckdb="${6:-0}"

    local _va_exact _va_approx _dm
    local eb_exact eb_approx
    eb_exact="$(keep_exact_error_bounds "$eb")"
    eb_approx="$(drop_exact_error_bounds "$eb")"

    if (( DRY_RUN )); then
        printf '  [dry-run] %-48s scenarios="%s"\n' "$label" "$scenarios"
        printf '              nm="%s"\n' "$nm"
        if [[ -n "$eb_exact" ]] && _va_exact=$(valinor_exact_approaches); then
            printf '              valinor_exact: approaches="%s" eb="%s"\n' \
                "$_va_exact" "$eb_exact"
        fi
        if [[ -n "$eb_approx" ]] && _va_approx=$(valinor_approx_approaches); then
            printf '              valinor_approx: approaches="%s" eb="%s"\n' \
                "$_va_approx" "$eb_approx"
        fi
        if [[ "$skip_duckdb" == 1 ]]; then
            printf '              duckdb: skipped\n'
        elif _dm=$(duckdb_modes); then
            printf '              duckdb: modes="%s"\n' "$_dm"
        fi
        if should_run pilotdb; then
            printf '              pilotdb: eb="%s"\n' "$pilot_eb"
        fi
        return 0
    fi

    if [[ -n "$eb_exact" ]] && _va_exact=$(valinor_exact_approaches); then
        echo "===== ${label} (Valinor exact) [run $run] ====="
        SCENARIOS="$scenarios" APPROACHES="$_va_exact" \
            NUM_MEASURES="$nm" ERROR_BOUNDS="$eb_exact" \
            MAX_QUERIES="${MAX_QUERIES_VALINOR:-0}" \
            run_valinor
    fi

    if [[ -n "$eb_approx" ]] && _va_approx=$(valinor_approx_approaches); then
        echo "===== ${label} (Valinor approximate) [run $run] ====="
        SCENARIOS="$scenarios" APPROACHES="$_va_approx" \
            NUM_MEASURES="$nm" ERROR_BOUNDS="$eb_approx" \
            MAX_QUERIES="${MAX_QUERIES_VALINOR:-0}" \
            run_valinor
    fi

    if [[ "$skip_duckdb" != 1 ]] && _dm=$(duckdb_modes); then
        echo "===== ${label} (DuckDB) [run $run] ====="
        SCENARIOS="$scenarios" MODES="$_dm" \
            NUM_MEASURES="$nm" \
            MAX_QUERIES="${MAX_QUERIES_DUCKDB:-100}" \
            run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== ${label} (PilotDB) [run $run] ====="
        SCENARIOS="$scenarios" \
            NUM_MEASURES="$nm" ERROR_BOUNDS="$pilot_eb" \
            MAX_QUERIES="${MAX_QUERIES_PILOTDB:-100}" \
            run_pilotdb
    fi
}

# -- (default + optional eb/nm sub-sweeps) for a single (ds, wl) cell ---------
run_cell_with_sweeps() {
    local ds="$1" wl="$2" sc="$3"

    # 1. Default operating point — always.
    run_cell "${ds}/${wl} default" "$sc" \
        "$DEFAULT_NUM_MEASURES" "$DEFAULT_ERROR_BOUNDS" \
        "$DEFAULT_PILOTDB_ERROR_BOUNDS"

    # 2. EB sweep (anchored at nm=4). Skips DuckDB (no eb axis).
    if cell_sweep_enabled eb; then
        run_cell "${ds}/${wl} eb-sweep" "$sc" \
            "$DEFAULT_NUM_MEASURES" "$SWEEP_EB_VALUES" \
            "$SWEEP_EB_PILOTDB_VALUES" 1
    fi

    # 3. NM sweep (anchored at eb=0.01).
    if cell_sweep_enabled nm; then
        run_cell "${ds}/${wl} nm-sweep" "$sc" \
            "$SWEEP_NM_VALUES" "$SWEEP_NM_EB_ANCHOR" \
            "$SWEEP_NM_PILOTDB_EB_ANCHOR"
    fi
}

# -- Pre-flight summary --------------------------------------------------------
default_exact_display="<none>"
default_approx_display="<none>"
if should_run valinor; then
    default_exact_display="$(keep_exact_error_bounds "$DEFAULT_ERROR_BOUNDS")"
    [[ -n "$default_exact_display" ]] || default_exact_display="<none>"
fi
if should_run valinor_a || should_run valinor_s; then
    default_approx_display="$(drop_exact_error_bounds "$DEFAULT_ERROR_BOUNDS")"
    [[ -n "$default_approx_display" ]] || default_approx_display="<none>"
fi

echo "===== Experiment runner ====="
echo "  RESULTS_BASE: $RESULTS_BASE"
echo "  METHODS:      ${METHODS[*]:-<none>}"
echo "  DATASETS:     ${DATASETS[*]:-<none>}"
echo "  WORKLOADS:    ${WORKLOADS[*]:-<none>}"
echo "  CELL_SWEEPS:  ${CELL_SWEEPS[*]:-<none>}"
echo "  SWEEPS:       ${SWEEPS[*]:-<none>}"
echo "  OUTLIER_K_LIST: $OUTLIER_K_LIST  (Valinor only; 0 = disabled)"
echo "  RUNS:         $_run_start .. $_run_end"
echo "  DEFAULT op:   nm=$DEFAULT_NUM_MEASURES  eb_valinor_exact=\"$default_exact_display\"  eb_valinor_approx=\"$default_approx_display\"  eb_pilotdb=\"$DEFAULT_PILOTDB_ERROR_BOUNDS\""
echo "  MAX_QUERIES:  valinor=${MAX_QUERIES_VALINOR:-0}  duckdb=${MAX_QUERIES_DUCKDB:-100}  pilotdb=${MAX_QUERIES_PILOTDB:-100}  (0 = use scenario's seqCount)"
(( DRY_RUN )) && echo "  MODE:         DRY-RUN (no execution)"
echo "============================="

# -- Main loop -----------------------------------------------------------------
for run in $(seq "$_run_start" "$_run_end"); do
    export RUN_START=$run

    echo
    echo "################################################################"
    echo "#  RUN $run"
    echo "################################################################"

    # ---- Main grid: DATASETS × WORKLOADS, each with default + cell sweeps ---
    for ds in "${DATASETS[@]}"; do
        for wl in "${WORKLOADS[@]}"; do
            sc=$(scenario_for "$ds" "$wl") || {
                # synth10:exploratory is intentionally unmapped; skip silently.
                [[ "$ds:$wl" == "synth10:exploratory" ]] || \
                    echo "  [skip] no scenario mapped for ${ds} / ${wl}"
                continue
            }
            run_cell_with_sweeps "$ds" "$wl" "$sc"
        done
    done

    # ---- Synth macro sweeps (DEFAULT op point only; no nm/eb sub-sweeps) ----
    # Rationale: the per-cell sweeps already characterise the (nm, eb) axes
    # at the anchor synth10_300M_clustered_sel1 scenario; the macro sweeps
    # vary the orthogonal scenario axis (selectivity, dataset size).
    for sw in "${SWEEPS[@]}"; do
        case "$sw" in
            selectivity)
                run_cell "Sweep: synth10 selectivity" \
                    "$SWEEP_SELECTIVITY_SCENARIOS" \
                    "$DEFAULT_NUM_MEASURES" "$DEFAULT_ERROR_BOUNDS" \
                    "$DEFAULT_PILOTDB_ERROR_BOUNDS"
                ;;
            scalability)
                for sc in "${SWEEP_SCALABILITY_SCENARIOS[@]}"; do
                    run_cell "Sweep: synth10 scalability ($sc)" \
                        "$sc" \
                        "$DEFAULT_NUM_MEASURES" "$DEFAULT_ERROR_BOUNDS" \
                        "$DEFAULT_PILOTDB_ERROR_BOUNDS"
                done
                ;;
            *)
                echo "  [warn] unknown sweep: $sw"
                ;;
        esac
    done

    echo
    echo "===== Run $run completed ====="
done

echo "===== All experiments completed ====="
