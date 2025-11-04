#!/bin/bash

# Create the directory if it doesn't exist

# java -Xmx31G -jar target/experiments.jar -c \
#         timeApproximateQueries -csv /data-nonraid/maroulis/data/taxi_data/yellow_tripdata_2014_cleaned.csv -bounds -74.106216:-73.842545,40.676993:40.839788 \
#         -rect -73.99831454:-73.97500902,40.7151612032:40.73263959 -xCol 5 -yCol 6 -measureCol 12 -valid "12<0,12>400" \
#         -initMode valinor \
#         -objCount 165000000 -seqCount 100 -minShift 10 \
#         -maxShift 10 -out results_valinor_approx.csv \
#         -errorBound 0.95 -run 1


echo "All experiments completed."
