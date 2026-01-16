#!/bin/bash

# Set DuckDB memory limit
export DUCKDB_MEMORY_LIMIT=8GB
# Set DuckDB temporary directory
export DUCKDB_TEMP_DIR=/data-nonraid/maroulis/data/.duckdb_tmp

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

# Set scenario and base output directory
scenario="pan_zoom_scenario"
base_out_dir="experiments/results/synth10/${scenario}/pilotdb"

# Number of times to run each experiment
num_runs=3

mkdir -p "$base_out_dir"

# Define sets of measure columns
measure_cols_list=("2" "2,3" "2,3,4,5" "2,3,4,5,6,7" "2,3,4,5,6,7,8,9")


for measure_cols in "${measure_cols_list[@]}"
do
  # Count number of measure columns (comma-separated list)
  num_cols=$(echo "$measure_cols" | awk -F',' '{print NF}')
  for run in $(seq 1 $num_runs)
  do
    echo "[INFO] PilotDB Experiments - Running measure_cols=$measure_cols, run=$run"
    /home/bstam/.local/bin/uv run python ./scripts/run_pilotdb_bbox_queries.py \
      --queries-file ./queries/synth10_new \
      --csv /data-nonraid/maroulis/data/data_10_cols.csv \
      --lon-col 1 --lat-col 0 \
      --measure-cols $measure_cols \
      --error 0.05 \
      --out "$base_out_dir/results_mcols${num_cols}_run${run}.csv" \    
  done
done

echo "All experiments completed."