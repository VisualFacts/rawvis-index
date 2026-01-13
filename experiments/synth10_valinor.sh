#!/bin/bash

# Create the directory if it doesn't exist
mkdir -p experiments/synth10_only_sampling/different_errors

# Define the error bounds
error_bounds=(0 0.2 0.1 0.05 0.02 0.01)

# Number of times to run each experiment
num_runs=1

# Iterate over each error bound and run the experiment multiple times
for error_bound in "${error_bounds[@]}"
do
    for run in $(seq 1 $num_runs)
    do
        echo "Running experiment with errorBound $error_bound, run $run..."
        java -Xmx16G -jar target/experiments.jar -c \
        timeApproximateQueries -csv /data-nonraid/maroulis/data/data_10_cols.csv -bounds 0:1000,0:1000 \
        -rect 544:574,323:353 -xCol 0 -yCol 1 -measureCols 9 \
        -initMode valinor \
        -objCount 100000000 -seqCount 100 -minShift 10 \
        -maxShift 20 -out experiments/synth10_new/different_errors/results_${error_bound}_run${run}.csv \
        -errorBound $error_bound -run $run
    done
done

echo "All experiments completed."
