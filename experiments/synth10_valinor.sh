#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

# Create the directory if it doesn't exist
mkdir -p experiments/results/synth10/pan_zoom_scenario/

# Define the error bounds
error_bounds=(0 0.01 0.02 0.05 0.1 0.2)

# Define sets of measure columns
measure_cols_list=("2" "2,3" "2,3,4,5" "2,3,4,5,6,7" "2,3,4,5,6,7,8,9")


# Number of times to run each experiment
num_runs=3

for measure_cols in "${measure_cols_list[@]}"
do
    num_cols=$(echo $measure_cols | awk -F',' '{print NF}')
    for error_bound in "${error_bounds[@]}"
    do
        for run in $(seq 1 $num_runs)
        do
            echo "Running experiment with $num_cols measureCols ($measure_cols), errorBound $error_bound, run $run..."
            java -Xmx16G -Djava.library.path="$LIBPATH" -jar target/experiments.jar -c \
            timeApproximateQueries -csv /data-nonraid/maroulis/data/data_10_cols.csv -bounds 0:1000,0:1000 \
            -rect 544:574,323:353 -xCol 0 -yCol 1 -measureCols $measure_cols \
            -initMode valinor \
            -objCount 100000000 -seqCount 100 -minShift 10 \
            -maxShift 20 -zoomFactor 1.5 -out experiments/results/synth10/pan_zoom_scenario/results_mcols${num_cols}_error${error_bound}_run${run}.csv \
            -errorBound $error_bound -run $run
        done
    done
done

echo "All experiments completed."
