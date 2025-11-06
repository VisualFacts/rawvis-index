java -Xmx16G  -jar target/experiments.jar \
  -c timeDuckDBQueries \
  -csv /data-nonraid/maroulis/data/data_10_cols.csv  \
  -xCol 0 \
  -yCol 1 \
  -objCount 100000000 \
  -measureCol 9 \
  -bounds 0:1000,0:1000 \
  -rect 544:574,323:353 \
  -seqCount 100 \
  -minShift 10 \
  -maxShift 20 \
  -duckDbMode spatialIndex \
  -out results.csv \
  -run 0

#taxi
java -Xmx8G  -jar target/experiments.jar \
  -c timeDuckDBQueries \
  -csv /data-nonraid/maroulis/data/taxi_data/yellow_tripdata_2014_cleaned.csv  \
  -xCol 5 \
  -yCol 6 \
  -objCount 165000000 \
  -valid "12<0,12>400" \
  -measureCol0 12\
  -bounds -74.106216:-73.842545,40.676993:40.839788 \
  -rect -73.99831454:-73.97500902,40.7151612032:40.73263959 \
  -seqCount 100 \
  -minShift 10 \
  -maxShift 20 \
  -duckDbMode table \
  -out results.csv \
  -run 0

explain select count(column9) as count, min(column9) as min, max(column9) as max, sum(column9) as sum, avg(column9) as avg, sum(column9 * column9) as sum_of_squares  from data_table where column0 > 490.60004 AND column0 < 520.60004 AND column1 > 457.69998 AND column1 < 487.69998;

explain SELECT count(column9) as count, 
       min(column9) as min, 
       max(column9) as max, 
       sum(column9) as sum, 
       avg(column9) as avg, 
       sum(column9 * column9) as sum_of_squares  
FROM data_table 
WHERE ST_Within(geometry, ST_MakeEnvelope(490.60004, 457.69998, 520.60004, 487.69998));


 java -Xmx31G -jar target/experiments.jar -c \
        timeApproximateQueries -csv /data-nonraid/maroulis/data/taxi_data/yellow_tripdata_2014_cleaned.csv -bounds -74.106216:-73.842545,40.676993:40.839788 \
        -rect -73.99831454:-73.97500902,40.7151612032:40.73263959 -xCol 5 -yCol 6 -measureCol 12 -valid "12<0,12>400" \
        -initMode valinor \
        -objCount 165000000 -seqCount 100 -minShift 10 \
        -maxShift 10 -out experiments/taxi_only_sampling/different_errors/results_${error_bound}_run${run}.csv \
        -errorBound $error_bound -run $run