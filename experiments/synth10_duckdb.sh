#!/bin/bash

# Create the directory if it doesn't exist
mkdir -p experiments/synth10_duckdb/different_errors

# Number of times to run each experiment
num_runs=1

for run in $(seq 1 $num_runs)
do
  java -Xmx8G  -jar target/experiments.jar \
    -c timeDuckDBQueries \
    -csv /data-nonraid/maroulis/data/data_10_cols.csv  \
    -xCol 0 \
    -yCol 1 \
    -objCount 100000000 \
    -measureCols 9 \
    -bounds 0:1000,0:1000 \
    -rect 544:574,323:353 \
    -seqCount 100 \
    -minShift 10 \
    -maxShift 20 \
    -duckDbMode table \
    -out experiments/synth10_duckdb/results_0_run${run}.csv \
    -run $run
done

echo "All experiments completed."

