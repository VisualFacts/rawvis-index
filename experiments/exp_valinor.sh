#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

config_file="src/main/resources/experiments/experiment_scenarios.yaml"

# List of scenarios to run
scenarios=("sdss_pan" "synth10_pan" "synth50_pan" "taxi_pan")

# Define the error bounds
error_bounds=(0 0.01 0.02 0.05 0.1)

# Define the number of measure columns to test (as integers)
num_measures_list=(1 2 4 6 8)

# Default values used when varying the other dimension
fixed_measures_for_error_bounds=(1 4)
fixed_error_bounds_for_measures=(0 0.01)

# Number of times to run each experiment
num_runs=3

# Optional: start run index (e.g., RUN_START=3 ./exp_valinor.sh to start at run 3)
run_start=${RUN_START:-1}
run_end=$((run_start + num_runs - 1))

for run in $(seq $run_start $run_end)
do
    for scenario in "${scenarios[@]}"
    do
        results_dir="experiments/results/${scenario}/"
        mkdir -p "$results_dir"
        for num_measures in "${num_measures_list[@]}"
        do
            for error_bound in "${error_bounds[@]}"
            do
                # Reduce combinations:
                # - When varying measures: only fixed_error_bounds_for_measures
                # - When varying error bounds: only fixed_measures_for_error_bounds
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
                echo "Running experiment for scenario $scenario with $num_measures measureCols, errorBound $error_bound, run $run..."
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
                    -out "$out_file"
                echo "Completed valinor experiment for scenario $scenario with $num_measures measureCols, errorBound $error_bound, run $run."
            done
        done
    done
done

echo "All valinor experiments completed."