#!/bin/bash

# =============================================================================
# VALINOR-A experiment runner
#
# Supports two approaches:
#   valinor_a — full system with aggregate metadata reuse (VALINOR-A)
#   valinor_s — VALINOR-S baseline: plain sampling, no metadata reuse
#
# All parameters are customizable via environment variables. Examples:
#
#   # Run only VALINOR-S with specific error bounds:
#   APPROACHES="valinor_s" ERROR_BOUNDS="0.05 0.1" ./exp_valinor.sh
#
#   # Run both approaches for taxi_pan with 2 runs:
#   SCENARIOS="taxi_pan" NUM_RUNS=2 ./exp_valinor.sh
#
#   # Run only VALINOR-A, starting from run 3:
#   APPROACHES="valinor_a" RUN_START=3 ./exp_valinor.sh
#
#   # Custom measure columns:
#   NUM_MEASURES="1 4" ./exp_valinor.sh
# =============================================================================

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

config_file="src/main/resources/experiments/experiment_scenarios.yaml"

# ---- Customizable parameters (override via env vars) ----

# Approaches to run: "valinor_a" (full system) and/or "valinor_s" (sampling-only baseline)
approaches=(${APPROACHES:-valinor_s})

# List of scenarios to run
scenarios=(${SCENARIOS:-taxi_zoom})
# All scenarios: synth10_pan synth50_pan taxi_pan taxi_zoom sdss_100cols_pan

# Error bounds to sweep
error_bounds=(${ERROR_BOUNDS:-0.01 0.02 0.05 0.1})

# Number of measure columns to test
num_measures_list=(${NUM_MEASURES:-1 2 4 6 8})

# Combination pruning: which measure counts get the full error bound sweep
fixed_measures_for_error_bounds=(${FIXED_MEASURES_FOR_EB:-1 4})

# Combination pruning: which error bounds get the full measure count sweep
fixed_error_bounds_for_measures=(${FIXED_EB_FOR_MEASURES:-0 0.01 0.05})

# Number of runs
num_runs=${NUM_RUNS:-2}

# Start run index
run_start=${RUN_START:-1}
run_end=$((run_start + num_runs - 1))

# ---- Helper ----

contains() {
    local value="$1"
    shift
    for item in "$@"; do
        if [[ "$item" == "$value" ]]; then
            return 0
        fi
    done
    return 1
}

# ---- Main loop ----

for approach in "${approaches[@]}"
do
    # Build extra args and results subdirectory based on approach
    extra_args=""
    if [[ "$approach" == "valinor_s" ]]; then
        extra_args="--samplingOnly"
        subdir="valinor_s/"
    elif [[ "$approach" == "valinor_a" ]]; then
        extra_args=""
        subdir="valinor_a/"
    else
        echo "Unknown approach: $approach (expected valinor_a or valinor_s)"
        continue
    fi

    for run in $(seq $run_start $run_end)
    do
        for scenario in "${scenarios[@]}"
        do
            results_dir="experiments/results/${scenario}/${subdir}"
            mkdir -p "$results_dir"
            for num_measures in "${num_measures_list[@]}"
            do
                for error_bound in "${error_bounds[@]}"
                do
                    # valinor_s only makes sense with errorBound > 0
                    if [[ "$approach" == "valinor_s" && "$error_bound" == "0" ]]; then
                        continue
                    fi

                    # Reduce combinations:
                    # - When varying measures: only fixed_error_bounds_for_measures
                    # - When varying error bounds: only fixed_measures_for_error_bounds
                    if ! contains "$num_measures" "${fixed_measures_for_error_bounds[@]}"; then
                        if ! contains "$error_bound" "${fixed_error_bounds_for_measures[@]}"; then
                            continue
                        fi
                    fi

                    out_file="${results_dir}results_mcols${num_measures}_error${error_bound}_run${run}.csv"
                    if [[ -f "$out_file" ]]; then
                        echo "Skipping existing result: $out_file"
                        continue
                    fi
                    echo "[$approach] Running scenario=$scenario mcols=$num_measures error=$error_bound run=$run..."
                    # Force cold disk reads for reproducible initialization timing
                    sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                    java -Xmx16G -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
                        -c timeApproximateQueries \
                        -scenario "$scenario" \
                        -configFile "$config_file" \
                        -initMode valinor \
                        -numMeasures $num_measures \
                        -errorBound $error_bound \
                        -run $run \
                        $extra_args \
                        -out "$out_file"
                    echo "[$approach] Completed scenario=$scenario mcols=$num_measures error=$error_bound run=$run."
                done
            done
        done
    done
done

echo "All experiments completed."