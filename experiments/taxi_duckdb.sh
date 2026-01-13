#!/bin/bash

# Create the directory if it doesn't exist
mkdir -p experiments/taxi_duckdb

# Number of times to run each experiment
num_runs=3

for run in $(seq 1 $num_runs)
do
  java -Xmx8G  -jar target/experiments.jar \
    -c timeDuckDBQueries \
    -csv /Users/vasilisstamatopoulos/Documents/Works/01_ATHENA/01_Projects/XtremeXP/Code/approximate-valinor/data/yellow_tripdata_2014_cleaned.csv  \
    -xCol 5 \
    -yCol 6 \
    -objCount 165000000 \
    -valid "12<0,12>400" \
    -measureCols 12 \
    -bounds -74.106216:-73.842545,40.676993:40.839788 \
    -rect -73.99831454:-73.97500902,40.7151612032:40.73263959 \
    -seqCount 100 \
    -minShift 10 \
    -maxShift 20 \
    -duckDbMode table \
    -out experiments/taxi_duckdb/results_0_run${run}.csv \
    -run $run
done



echo "All experiments completed."
