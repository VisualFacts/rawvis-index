#!/bin/bash
# =============================================================================
# Experiment orchestration — runs all Valinor + DuckDB + PilotDB experiments.
#
# Each block calls exp_valinor.sh or exp_duckdb.sh with env-var overrides.
#
# All experiments run under a fixed 16 GB cgroup cap (MEM_LIMIT=16G).
# JVM_XMX defaults to MEM_LIMIT - 2G (=14G) for all datasets.
# -Xmx is a ceiling, not a reservation: the JVM only commits pages as needed.
# After init, -XX:MaxHeapFreeRatio=30 + System.gc() shrinks committed heap
# back to ~live-data size, freeing memory for page cache during queries.
# Datasets ≤ 328M rows fit in-memory; 500M+ spill partition to disk automatically.
#
# Sweep design:
#   Pivot (synth10_100M_pan_sel1) + real datasets → full error_bound × num_measures sweep
#   Non-pivot selectivity/scalability sweeps       → fixed NUM_MEASURES="1 4", ERROR_BOUNDS="0 0.01 0.05"
#
# Parameters (all via env vars):
#   RESULTS_BASE — directory for results (default: experiments/results)
#   METHODS      — space-separated list of methods to run
#                  (default: "valinor_a valinor_s duckdb pilotdb")
#                  Use METHODS="valinor_a" to run only VALINOR-A,
#                  METHODS="valinor_a valinor_s" for both Valinor variants, etc.
#   NUM_RUNS     — total number of runs (default: 3)
#   RUN_START    — first run index (default: 1)
#
# Loop order: runs are the OUTERMOST loop, so you get run 1 for all configs
# first, then run 2, etc.  This lets you get quick initial results and add
# more runs incrementally.
#
# Usage:
#   # Re-run only VALINOR-A to a new results folder:
#   RESULTS_BASE=experiments/results_v2 METHODS="valinor_a" \
#     nohup ./experiments/run_experiments.sh >> experiments/run_experiments.log 2>&1 &
#
#   # Run only run 1 for all configs:
#   NUM_RUNS=1 nohup ./experiments/run_experiments.sh >> experiments/run_experiments.log 2>&1 &
#
#   # Add run 2 later:
#   RUN_START=2 NUM_RUNS=1 nohup ./experiments/run_experiments.sh >> experiments/run_experiments.log 2>&1 &
#
#   tail -f experiments/run_experiments.log
# =============================================================================

set -e
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# Fixed cgroup memory cap for ALL experiments
export MEM_LIMIT=16G

# ---- Configurable parameters ----
export RESULTS_BASE=${RESULTS_BASE:-experiments/results}
METHODS=(${METHODS:-valinor_a valinor_s duckdb pilotdb})
_total_runs=${NUM_RUNS:-3}
_run_start=${RUN_START:-1}
_run_end=$((_run_start + _total_runs - 1))
export NUM_RUNS=1   # each helper script runs exactly 1 run; we loop externally

run_valinor() { "$SCRIPT_DIR/exp_valinor.sh"; }
run_duckdb()  { "$SCRIPT_DIR/exp_duckdb.sh";  }
run_pilotdb() { "$SCRIPT_DIR/exp_pilotdb.sh";  }

# Fixed values used for non-pivot sweep scenarios (no full cross-product)
SWEEP_NUM_MEASURES="1 4"
SWEEP_ERROR_BOUNDS="0 0.01 0.05"

should_run() {
    local method="$1"
    for m in "${METHODS[@]}"; do
        if [[ "$m" == "$method" ]]; then
            return 0
        fi
    done
    return 1
}

# Build the APPROACHES string from whichever valinor variants are in METHODS.
# Returns false (1) if neither valinor_a nor valinor_s is selected.
valinor_approaches() {
    local apps=""
    should_run valinor_a && apps="$apps valinor_a"
    should_run valinor_s && apps="$apps valinor_s"
    apps="${apps# }"  # trim leading space
    if [[ -z "$apps" ]]; then
        return 1
    fi
    echo "$apps"
    return 0
}

echo "===== Experiment runner ====="
echo "  RESULTS_BASE: $RESULTS_BASE"
echo "  METHODS:      ${METHODS[*]}"
echo "  RUNS:         $_run_start .. $_run_end"
echo "============================="

for run in $(seq $_run_start $_run_end); do
    export RUN_START=$run

    echo ""
    echo "################################################################"
    echo "#  RUN $run"
    echo "################################################################"

    # =============================================================================
    # Block 1 — Pivot: synth10 100M, 1% selectivity — full error_bound × num_measures sweep
    # =============================================================================
    _va=$(valinor_approaches) && {
        echo "===== Block 1: synth10 100M sel1 pivot (Valinor — full sweep) [run $run] ====="
        SCENARIOS="synth10_100M_pan_sel1" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 1b: synth10 100M sel1 pivot (DuckDB) [run $run] ====="
        SCENARIOS="synth10_100M_pan_sel1" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 1c: synth10 100M sel1 pivot (PilotDB — full sweep) [run $run] ====="
        SCENARIOS="synth10_100M_pan_sel1" \
        run_pilotdb
    fi

    # =============================================================================
    # Block 2 — Selectivity axis: non-pivot selectivities (fixed measures & error bounds)
    # =============================================================================
    _va=$(valinor_approaches) && {
        echo "===== Block 2: synth10 100M selectivity sweep, non-pivot (Valinor) [run $run] ====="
        SCENARIOS="synth10_100M_pan_sel001 synth10_100M_pan_sel01 synth10_100M_pan_sel5 synth10_100M_pan_sel10" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="$SWEEP_ERROR_BOUNDS" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 2b: synth10 100M selectivity sweep, non-pivot (DuckDB) [run $run] ====="
        SCENARIOS="synth10_100M_pan_sel001 synth10_100M_pan_sel01 synth10_100M_pan_sel5 synth10_100M_pan_sel10" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 2c: synth10 100M selectivity sweep, non-pivot (PilotDB) [run $run] ====="
        SCENARIOS="synth10_100M_pan_sel001 synth10_100M_pan_sel01 synth10_100M_pan_sel5 synth10_100M_pan_sel10" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="0.01 0.05" \
        run_pilotdb
    fi

    # =============================================================================
    # Block 3 — Scalability axis: 1% selectivity, non-pivot sizes (fixed measures & error bounds)
    # =============================================================================
    _va=$(valinor_approaches) && {
        echo "===== Block 3a: synth10 50M (Valinor) [run $run] ====="
        SCENARIOS="synth10_50M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="$SWEEP_ERROR_BOUNDS" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 3a: synth10 50M (DuckDB) [run $run] ====="
        SCENARIOS="synth10_50M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 3a: synth10 50M (PilotDB) [run $run] ====="
        SCENARIOS="synth10_50M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="0.01 0.05" \
        run_pilotdb
    fi

    # synth10_100M_pan_sel1 is the pivot — already ran in Block 1

    _va=$(valinor_approaches) && {
        echo "===== Block 3b: synth10 200M (Valinor) [run $run] ====="
        SCENARIOS="synth10_200M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="$SWEEP_ERROR_BOUNDS" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 3b: synth10 200M (DuckDB) [run $run] ====="
        SCENARIOS="synth10_200M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 3b: synth10 200M (PilotDB) [run $run] ====="
        SCENARIOS="synth10_200M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="0.01 0.05" \
        run_pilotdb
    fi

    _va=$(valinor_approaches) && {
        echo "===== Block 3c: synth10 500M (Valinor) [run $run] ====="
        SCENARIOS="synth10_500M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="$SWEEP_ERROR_BOUNDS" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 3c: synth10 500M (DuckDB) [run $run] ====="
        SCENARIOS="synth10_500M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 3c: synth10 500M (PilotDB) [run $run] ====="
        SCENARIOS="synth10_500M_pan_sel1" \
        NUM_MEASURES="$SWEEP_NUM_MEASURES" \
        ERROR_BOUNDS="0.01 0.05" \
        run_pilotdb
    fi

    # =============================================================================
    # Block 4 — Taxi (165M rows): real-world dataset — full sweep
    # =============================================================================
    _va=$(valinor_approaches) && {
        echo "===== Block 4: taxi (Valinor — full sweep) [run $run] ====="
        SCENARIOS="taxi_pan taxi_zoom" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 4b: taxi (DuckDB) [run $run] ====="
        SCENARIOS="taxi_pan taxi_zoom" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 4c: taxi (PilotDB — full sweep) [run $run] ====="
        SCENARIOS="taxi_pan taxi_zoom" \
        run_pilotdb
    fi

    # =============================================================================
    # Block 5 — Gaia DR3 (328M rows): real-world dataset — full sweep
    # =============================================================================
    _va=$(valinor_approaches) && {
        echo "===== Block 5: gaia DR3 (Valinor — full sweep) [run $run] ====="
        SCENARIOS="gaia_dr3_pan" \
        APPROACHES="$_va" \
        run_valinor
    }

    if should_run duckdb; then
        echo "===== Block 5: gaia DR3 (DuckDB) [run $run] ====="
        SCENARIOS="gaia_dr3_pan" \
        run_duckdb
    fi

    if should_run pilotdb; then
        echo "===== Block 5c: gaia DR3 (PilotDB — full sweep) [run $run] ====="
        SCENARIOS="gaia_dr3_pan" \
        run_pilotdb
    fi

    echo ""
    echo "===== Run $run completed ====="
done

echo "===== All experiments completed ====="
