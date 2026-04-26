#!/bin/bash
# =============================================================================
# Experiment orchestration — runs Valinor + DuckDB + PilotDB experiments.
#
# Each experiment is dispatched to exp_valinor.sh / exp_duckdb.sh /
# exp_pilotdb.sh via env-var overrides.
#
# All experiments run under a fixed 16 GB cgroup cap (MEM_LIMIT=16G).
# JVM_XMX defaults to MEM_LIMIT - 2G (=14G) for all datasets.
#
# ---- Experiment design (orthogonal axes) ------------------------------------
#
#   Main grid:   DATASETS × WORKLOADS                       full mcols × eb sweep
#   Sweeps:      synth10-only sensitivity studies           reduced mcols/eb grid
#                 - selectivity:  vary query rect size on synth10_300M
#                 - scalability:  vary dataset size at 1% selectivity
#
#   DATASETS  : synth10 | taxi | gaia_dr3 | ebird_us
#   WORKLOADS : exploratory | random
#   SWEEPS    : selectivity | scalability
#
#   Each (dataset, workload) pair maps to exactly one scenario in the YAML;
#   the mapping lives in scenario_for() below.
#
# ---- Parameters (all via env vars) ------------------------------------------
#
#   RESULTS_BASE — directory for results (default: experiments/results)
#   METHODS      — space-separated list of methods to run
#                  (default: "valinor_a duckdb_table_projected pilotdb valinor_s")
#                  Recognised: valinor_a, valinor_s,
#                              duckdb_table, duckdb_table_projected,
#                              pilotdb
#   DATASETS     — datasets to include in the main grid
#                  (default: "synth10 taxi gaia_dr3 ebird_us"; empty = none)
#   WORKLOADS    — workloads to include in the main grid
#                  (default: "exploratory random"; empty = none)
#   SWEEPS       — synth10 sensitivity sweeps to run
#                  (default: "selectivity scalability"; empty = none)
#   NUM_RUNS     — total number of runs (default: 3)
#   RUN_START    — first run index (default: 1)
#
# Loop order: runs are the OUTERMOST loop, so you get run 1 for all configs
# first, then run 2, etc.  This lets you collect quick initial results and add
# more runs incrementally.
#
# ---- Resumability -----------------------------------------------------------
#
# Result CSVs are written to:
#   $RESULTS_BASE/<scenario>/<approach>/results_mcols<M>_error<E>_res<R>_str<S>_run<N>.csv
#
# Before running each cell, the helper scripts (exp_valinor.sh,
# exp_duckdb.sh, exp_pilotdb.sh) check `if [[ -f "$out_file" ]]` and skip if
# the file already exists.  This means:
#   - re-invoking this script is safe and idempotent
#   - completed cells are NEVER overwritten
#   - to force re-execution of a cell, manually `rm` its CSV first
#   - adding a new scenario, method, or run only runs the missing cells
#
# Use --dry-run to print the full plan without executing anything.
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
#   # 2. Default full run: grid + sweeps, 3 runs, in background, logged
#   sudo -E nohup ./experiments/run_experiments.sh \
#       >> experiments/run_experiments.log 2>&1 &
#   tail -f experiments/run_experiments.log
#
#   # Only the main grid (skip synth10 sweeps)
#   SWEEPS="" sudo -E ./experiments/run_experiments.sh
#
#   # Only random workload on real datasets
#   DATASETS="taxi gaia_dr3 ebird_us" WORKLOADS="random" SWEEPS="" \
#       sudo -E ./experiments/run_experiments.sh
#
#   # Only the scalability sweep (no main grid)
#   DATASETS="" WORKLOADS="" SWEEPS="scalability" \
#       sudo -E ./experiments/run_experiments.sh
#
#   # Add run 2 later, only Valinor-A
#   RUN_START=2 NUM_RUNS=1 METHODS="valinor_a" \
#       sudo -E ./experiments/run_experiments.sh
#
#   # Force-rerun a single cell: delete its CSV and run that scenario only
#   rm experiments/results/taxi_zoom/valinor_a/results_mcols4_error0.01_res500_str0.2_run1.csv
#   DATASETS=taxi WORKLOADS=exploratory SWEEPS="" METHODS=valinor_a NUM_RUNS=1 \
#       sudo -E ./experiments/run_experiments.sh
# =============================================================================

set -e
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

# Fixed cgroup memory cap for ALL experiments
export MEM_LIMIT=16G

# ---- Configurable parameters ----
export RESULTS_BASE=${RESULTS_BASE:-experiments/results}
METHODS=(${METHODS:-valinor_a duckdb_table_projected pilotdb valinor_s})
DATASETS=(${DATASETS-synth10 taxi gaia_dr3 ebird_us})
WORKLOADS=(${WORKLOADS-exploratory random})
SWEEPS=(${SWEEPS-selectivity scalability})

_total_runs=${NUM_RUNS:-3}
_run_start=${RUN_START:-1}
_run_end=$((_run_start + _total_runs - 1))
export NUM_RUNS=1   # each helper script runs exactly 1 run; we loop externally

# Reduced sweep grid (for synth10 sensitivity studies)
SWEEP_NUM_MEASURES="1 4"
SWEEP_ERROR_BOUNDS="0 0.01 0.05"
SWEEP_PILOTDB_ERROR_BOUNDS="0.01 0.05"   # pilotdb has no eb=0 mode

# Main-grid random workloads are intentionally run at a single paper-default
# operating point to keep DuckDB / PilotDB runtime manageable.
RANDOM_NUM_MEASURES="4"
RANDOM_ERROR_BOUNDS="0 0.01"             # valinor_a runs both; valinor_s skips 0
RANDOM_PILOTDB_ERROR_BOUNDS="0.01"       # pilotdb has no eb=0 mode

run_valinor() { "$SCRIPT_DIR/exp_valinor.sh"; }
run_duckdb()  { "$SCRIPT_DIR/exp_duckdb.sh";  }
run_pilotdb() { "$SCRIPT_DIR/exp_pilotdb.sh"; }

# -- METHODS membership --------------------------------------------------------
should_run() {
    local method="$1"
    local m
    for m in "${METHODS[@]}"; do
        [[ "$m" == "$method" ]] && return 0
    done
    return 1
}

# Build the APPROACHES string for exp_valinor.sh from METHODS.
# Returns 1 if no Valinor variant is selected.
valinor_approaches() {
    local apps=""
    should_run valinor_a && apps="$apps valinor_a"
    should_run valinor_s && apps="$apps valinor_s"
    apps="${apps# }"
    [[ -z "$apps" ]] && return 1
    echo "$apps"
}

# Build the MODES string for exp_duckdb.sh from METHODS.
# Returns 1 if no DuckDB mode is selected.
duckdb_modes() {
    local modes=""
    should_run duckdb_table           && modes="$modes table"
    should_run duckdb_table_projected && modes="$modes tableProjected"
    modes="${modes# }"
    [[ -z "$modes" ]] && return 1
    echo "$modes"
}

# -- (dataset, workload) -> scenario name --------------------------------------
# Single source of truth for which YAML scenario backs each grid cell.
# To add a new dataset or workload, add a case here and (if needed) extend the
# default DATASETS / WORKLOADS arrays above.
scenario_for() {
    local ds="$1" wl="$2"
    case "$ds:$wl" in
        synth10:exploratory)   echo "synth10_300M_pan_sel1" ;;
        synth10:random)        echo "synth10_300M_random_sel1" ;;
        taxi:exploratory)      echo "taxi_zoom" ;;
        taxi:random)           echo "taxi_random" ;;
        gaia_dr3:exploratory)  echo "gaia_dr3_pan" ;;
        gaia_dr3:random)       echo "gaia_dr3_random" ;;
        ebird_us:exploratory)  echo "ebird_us_pan" ;;
        ebird_us:random)       echo "ebird_us_random" ;;
        *) return 1 ;;
    esac
}

# -- Unified per-cell dispatch -------------------------------------------------
# Runs all selected methods on a given list of scenarios, optionally with
# overridden NUM_MEASURES / ERROR_BOUNDS (for the reduced sensitivity sweeps).
#
# Usage:
#   run_cell <label> <scenarios> [num_measures] [error_bounds] [pilotdb_error_bounds]
#
# Empty num_measures / error_bounds keep the helper-script defaults
# (= the full sweep).
run_cell() {
    local label="$1"
    local scenarios="$2"
    local nm="${3:-}"
    local eb="${4:-}"
    local pilot_eb="${5:-}"

    if (( DRY_RUN )); then
        printf '  [dry-run] %-44s scenarios="%s"' "$label" "$scenarios"
        [[ -n "$nm" ]] && printf ' NUM_MEASURES="%s"' "$nm"
        [[ -n "$eb" ]] && printf ' ERROR_BOUNDS="%s"' "$eb"
        printf '\n'
        return 0
    fi

    local _va _dm
    if _va=$(valinor_approaches); then
        echo "===== ${label} (Valinor) [run $run] ====="
        SCENARIOS="$scenarios" APPROACHES="$_va" \
            ${nm:+NUM_MEASURES="$nm"} \
            ${eb:+ERROR_BOUNDS="$eb"} \
            run_valinor
    fi

    if _dm=$(duckdb_modes); then
        echo "===== ${label} (DuckDB) [run $run] ====="
        SCENARIOS="$scenarios" MODES="$_dm" \
            ${nm:+NUM_MEASURES="$nm"} \
            run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== ${label} (PilotDB) [run $run] ====="
        local pilot_eb_use="${pilot_eb:-$eb}"
        SCENARIOS="$scenarios" \
            ${nm:+NUM_MEASURES="$nm"} \
            ${pilot_eb_use:+ERROR_BOUNDS="$pilot_eb_use"} \
            run_pilotdb
    fi
}

# -- Pre-flight summary --------------------------------------------------------
echo "===== Experiment runner ====="
echo "  RESULTS_BASE: $RESULTS_BASE"
echo "  METHODS:      ${METHODS[*]:-<none>}"
echo "  DATASETS:     ${DATASETS[*]:-<none>}"
echo "  WORKLOADS:    ${WORKLOADS[*]:-<none>}"
echo "  SWEEPS:       ${SWEEPS[*]:-<none>}"
echo "  RUNS:         $_run_start .. $_run_end"
(( DRY_RUN )) && echo "  MODE:         DRY-RUN (no execution)"
echo "============================="

# -- Main loop -----------------------------------------------------------------
for run in $(seq "$_run_start" "$_run_end"); do
    export RUN_START=$run

    echo
    echo "################################################################"
    echo "#  RUN $run"
    echo "################################################################"

    # ---- Main grid: DATASETS × WORKLOADS ----
    for ds in "${DATASETS[@]}"; do
        for wl in "${WORKLOADS[@]}"; do
            sc=$(scenario_for "$ds" "$wl") || {
                echo "  [skip] no scenario mapped for ${ds} / ${wl}"
                continue
            }
            if [[ "$wl" == "random" ]]; then
                run_cell "Grid: ${ds} / ${wl}" "$sc" \
                    "$RANDOM_NUM_MEASURES" "$RANDOM_ERROR_BOUNDS" \
                    "$RANDOM_PILOTDB_ERROR_BOUNDS"
            else
                run_cell "Grid: ${ds} / ${wl}" "$sc"
            fi
        done
    done

    # ---- Sweeps: synth10 sensitivity studies (reduced grid) ----
    for sw in "${SWEEPS[@]}"; do
        case "$sw" in
            selectivity)
                run_cell \
                    "Sweep: synth10 selectivity" \
                    "synth10_300M_pan_sel001 synth10_300M_pan_sel01 synth10_300M_pan_sel1 synth10_300M_pan_sel5 synth10_300M_pan_sel10" \
                    "$SWEEP_NUM_MEASURES" "$SWEEP_ERROR_BOUNDS" \
                    "$SWEEP_PILOTDB_ERROR_BOUNDS"
                ;;
            scalability)
                # 300M is already covered by the synth10 main-grid cell.
                for sc in synth10_50M_pan_sel1 synth10_100M_pan_sel1 \
                          synth10_500M_pan_sel1 synth10_1B_pan_sel1; do
                    run_cell \
                        "Sweep: synth10 scalability ($sc)" \
                        "$sc" \
                        "$SWEEP_NUM_MEASURES" "$SWEEP_ERROR_BOUNDS" \
                        "$SWEEP_PILOTDB_ERROR_BOUNDS"
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
