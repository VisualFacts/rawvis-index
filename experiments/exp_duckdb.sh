#!/bin/bash

# Set DuckDB memory limit
export DUCKDB_MEMORY_LIMIT=8GB
# Set DuckDB temporary directory
export DUCKDB_TEMP_DIR=/data-nonraid/maroulis/data/.duckdb_tmp

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

config_file="src/main/resources/experiments/experiment_scenarios.yaml"


# List of scenarios to run
# scenarios=("synth10_pan" "synth50_pan" "taxi_pan")
scenarios=("taxi_pan")

# DuckDB execution modes
modes=(directCSV)
# modes=(table directCSV spatialIndex)

# Define the number of measure columns to test (as integers)
# num_measures_list=(1 2 4 6 8)
num_measures_list=(1)

# Number of times to run each experiment
num_runs=1

for mode in "${modes[@]}"
do
    for scenario in "${scenarios[@]}"
    do
        # Create the directory for results for each mode and scenario
        mode_results_dir="experiments/results/${scenario}/duckdb/${mode}/"
        mkdir -p "$mode_results_dir"
        for num_measures in "${num_measures_list[@]}"
        do
            for run in $(seq 1 $num_runs)
            do
                echo "Running DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run..."
                # Force cold disk reads for reproducible initialization timing
                sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                java -Xmx16G -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
                    -c timeDuckDBQueries \
                    -scenario "$scenario" \
                    -configFile "$config_file" \
                    -duckDbMode "$mode" \
                    -numMeasures $num_measures \
                    -run $run \
                    -out "${mode_results_dir}results_mcols${num_measures}_run${run}.csv"
                echo "Completed DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run."
            done
        done
    done
done

echo "All DuckDB experiments completed."
