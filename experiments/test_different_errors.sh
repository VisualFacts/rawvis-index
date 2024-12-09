#!/bin/bash

# Create the directory if it doesn't exist
mkdir -p experiments/different_errors

# Define the error bounds
error_bounds=(0.05 0.01 0)

# Number of times to run each experiment
num_runs=5

# Iterate over each error bound and run the experiment multiple times
for error_bound in "${error_bounds[@]}"
do
    for run in $(seq 1 $num_runs)
    do
        echo "Running experiment with errorBound $error_bound, run $run..."
        java -Xmx16G -jar target/experiments.jar -c \
        timeApproximateQueries -csv synth10.csv -bounds 0:1000,0:1000 \
        -rect 544:574,323:353 -xCol 0 -yCol 1 -measureCol 9 -groupBy 2 \
        -filters 3:1000000000,4:1000000000 -catCols 2,3,4,5,6,7 \
        -initMode valinor -binCount 100 -catBudget 2 \
        -objCount 100000000 -seqCount 50 -minShift 10 \
        -maxShift 10 -minFilters 2 -maxFilters 2 -out experiments/different_errors/results_${error_bound}_run${run}.csv \
        -errorBound $error_bound -run $run
    done
done

echo "All experiments completed."