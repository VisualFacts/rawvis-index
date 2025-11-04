#!/bin/bash

# Create the directory if it doesn't exist
mkdir -p experiments/taxi_only_sampling/different_errors

# Define the error bounds
error_bounds=(0.2 0.1 0.05 0.02 0.01)
# error_bounds=(0.01 0)

# Number of times to run each experiment
num_runs=3

# Iterate over each error bound and run the experiment multiple times
for error_bound in "${error_bounds[@]}"
do
    for run in $(seq 1 $num_runs)
    do
        echo "Running experiment with errorBound $error_bound, run $run..."
        java -Xmx31G -jar target/experiments.jar -c \
        timeApproximateQueries -csv /data-nonraid/maroulis/data/taxi_data/yellow_tripdata_2014_cleaned.csv -bounds -74.106216:-73.842545,40.676993:40.839788 \
        -rect -73.99831454:-73.97500902,40.7151612032:40.73263959 -xCol 5 -yCol 6 -measureCol 12 -valid "12<0,12>400" \
        -initMode valinor \
        -objCount 165000000 -seqCount 100 -minShift 10 \
        -maxShift 10 -out experiments/taxi_only_sampling/different_errors/results_${error_bound}_run${run}.csv \
        -errorBound $error_bound -run $run
    done
done

echo "All experiments completed."
