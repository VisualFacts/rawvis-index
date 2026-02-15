#!/bin/bash

# PilotDB requires Python >= 3.11 so it lives in a separate venv (.venv-pilotdb).
# To set it up on a new machine:
#   uv python install 3.11
#   uv venv .venv-pilotdb --python 3.11
#   source .venv-pilotdb/bin/activate
#   uv pip install pyyaml duckdb
#   git clone --depth 1 https://github.com/uiuc-kang-lab/PilotDB.git /tmp/PilotDB
#   uv pip install -e /tmp/PilotDB
#   uv pip install "sqlglot==26.30.0"   # PilotDB breaks with sqlglot >= 28

# Set DuckDB memory limit
export DUCKDB_MEMORY_LIMIT=8GB
# Set DuckDB temporary directory
export DUCKDB_TEMP_DIR=/data-nonraid/maroulis/data/.duckdb_tmp

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

config_file="src/main/resources/experiments/experiment_scenarios.yaml"

# ---- Customizable parameters (override via env vars) ----

# List of scenarios to run
scenarios=(${SCENARIOS:-synth10_pan synth50_pan taxi_pan sdss_100cols_pan})
# All scenarios: synth10_pan synth50_pan taxi_pan taxi_zoom sdss_100cols_pan

# Define the number of measure columns to test
num_measures_list=(${NUM_MEASURES:-1 2 4 6 8})

# Define error bounds
error_bounds=(${ERROR_BOUNDS:-0.01 0.02 0.05 0.1})

# Combination pruning: which measure counts get the full error bound sweep
fixed_measures_for_error_bounds=(${FIXED_MEASURES_FOR_EB:-1 4})

# Combination pruning: which error bounds get the full measure count sweep
fixed_error_bounds_for_measures=(${FIXED_EB_FOR_MEASURES:-0.01 0.05})

# Number of runs
num_runs=${NUM_RUNS:-2}

# Start run index (e.g., RUN_START=3 ./exp_pilotdb.sh to start at run 3)
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

# ---- Pre-generate query sequences ----

for scenario in "${scenarios[@]}"
do
    results_dir="experiments/results/${scenario}/pilotdb/"
    mkdir -p "$results_dir"

    generated_queries_file="${results_dir}queries_generated.txt"
    if [[ ! -f "$generated_queries_file" ]]; then
        echo "Generating query sequence for scenario $scenario..."
        java -Xmx16G -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
            -c generateAndSaveQuerySequence \
            -scenario "$scenario" \
            -configFile "$config_file" \
            -out "$generated_queries_file"
        if [[ ! -f "$generated_queries_file" ]]; then
            echo "Failed to generate queries for scenario $scenario. Skipping."
        fi
    fi
done

# ---- Main loop (run is outermost so run 1 completes across all configs first) ----

for run in $(seq $run_start $run_end)
do
    for scenario in "${scenarios[@]}"
    do
        results_dir="experiments/results/${scenario}/pilotdb/"
        generated_queries_file="${results_dir}queries_generated.txt"
        if [[ ! -f "$generated_queries_file" ]]; then
            echo "No queries file for scenario $scenario. Skipping."
            continue
        fi

        for num_measures in "${num_measures_list[@]}"
        do
            for error_bound in "${error_bounds[@]}"
            do
                # Combination pruning (same logic as exp_valinor.sh):
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

                echo "[run $run] PilotDB scenario=$scenario mcols=$num_measures error=$error_bound..."
                # Force cold disk reads for reproducible initialization timing
                sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

                .venv-pilotdb/bin/python ./experiments/run_pilotdb_queries.py \
                    --queries-file "$generated_queries_file" \
                    --scenario "$scenario" \
                    --config-file "$config_file" \
                    --num-measures "$num_measures" \
                    --error "$error_bound" \
                    --out "$out_file"

                echo "[run $run] Completed scenario=$scenario mcols=$num_measures error=$error_bound."
            done
        done
    done
done

echo "All PilotDB experiments completed."