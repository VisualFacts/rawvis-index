#!/bin/bash

# Set DuckDB memory limit
export DUCKDB_MEMORY_LIMIT=12GB
# Set DuckDB temporary directory
export DUCKDB_TEMP_DIR=/data-nonraid/maroulis/data/.duckdb_tmp

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

config_file="src/main/resources/experiments/experiment_scenarios.yaml"


# List of scenarios to run (override via env var)
scenarios=(${SCENARIOS:-synth10_pan synth50_pan taxi_pan taxi_zoom gaia_dr3_pan})

# DuckDB execution modes (override via env var)
modes=(${MODES:-table})
# Available modes: table directCSV spatialIndex

# Define the number of measure columns to test (override via env var)
num_measures_list=(${NUM_MEASURES:-1 2 4 6 8})

# Number of times to run each experiment (override via env var)
num_runs=${NUM_RUNS:-1}

# Optional: start run index (e.g., RUN_START=3 ./exp_duckdb.sh to start at run 3)
run_start=${RUN_START:-1}
run_end=$((run_start + num_runs - 1))

for mode in "${modes[@]}"
do
    for scenario in "${scenarios[@]}"
    do
        # Create the directory for results for each mode and scenario
        mode_results_dir="experiments/results/${scenario}/duckdb/${mode}/"
        mkdir -p "$mode_results_dir"
        for num_measures in "${num_measures_list[@]}"
        do
            for run in $(seq $run_start $run_end)
            do
                out_file="${mode_results_dir}results_mcols${num_measures}_run${run}.csv"
                if [[ -f "$out_file" ]]; then
                    echo "Skipping existing result: $out_file"
                    continue
                fi
                echo "Running DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run..."
                # Force cold disk reads for reproducible initialization timing
                sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                java -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
                    -c timeDuckDBQueries \
                    -scenario "$scenario" \
                    -configFile "$config_file" \
                    -duckDbMode "$mode" \
                    -numMeasures $num_measures \
                    -run $run \
                    -out "$out_file"
                echo "Completed DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run."
            done
        done
    done
done

echo "All DuckDB experiments completed."
