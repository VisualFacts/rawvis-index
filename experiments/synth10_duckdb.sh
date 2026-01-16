#!/bin/bash

# Set DuckDB memory limit
export DUCKDB_MEMORY_LIMIT=8GB
# Set DuckDB temporary directory
export DUCKDB_TEMP_DIR=/data-nonraid/maroulis/data/.duckdb_tmp

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

# Set scenario and base output directory
scenario="pan_zoom_scenario"
base_out_dir="experiments/results/synth10/${scenario}/duckdb"

# Number of times to run each experiment
num_runs=3

# DuckDB execution modes
modes=(table directCSV)
# modes=(table directCSV spatialIndex)


# Define sets of measure columns
measure_cols_list=("2" "2,3" "2,3,4,5" "2,3,4,5,6,7" "2,3,4,5,6,7,8,9")


for mode in "${modes[@]}"
do
  out_dir="$base_out_dir/${mode}"
  mkdir -p "$out_dir"
  for measure_cols in "${measure_cols_list[@]}"
  do
    # Count number of measure columns (comma-separated list)
    num_cols=$(echo "$measure_cols" | awk -F',' '{print NF}')
    for run in $(seq 1 $num_runs)
    do
      echo "[INFO] DuckDB Experiments - Running mode=$mode, measure_cols=$measure_cols, run=$run"
      java -Xmx16G -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
        -c timeDuckDBQueries \
        -csv /data-nonraid/maroulis/data/data_10_cols.csv \
        -xCol 0 \
        -yCol 1 \
        -measureCols "$measure_cols" \
        -rect 544:574,323:353 \
        -objCount 100000000 \
        -seqCount 100 \
        -minShift 10 \
        -maxShift 20 \
        -zoomFactor 1.5 \
        -duckDbMode "$mode" \
        -out "$out_dir/results_mcols${num_cols}_run${run}.csv" \
        -run $run
    done    
  done
done

echo "All experiments completed."

