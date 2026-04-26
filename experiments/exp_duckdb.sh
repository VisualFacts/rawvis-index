#!/bin/bash

# =============================================================================
# DuckDB experiment runner
#
# ---- Memory constraint setup ----
#
# MEM_LIMIT — cgroup v2 hard cap on total physical memory (heap + native +
#   page cache).  Must be the SAME value used for Valinor experiments so that
#   both systems get an identical hardware budget.  How each system divides
#   that budget (columnar buffer pool vs spatial index vs page cache) is part
#   of the architectural comparison.
#
# DUCKDB_MEMORY_LIMIT — DuckDB's internal buffer pool size.  DuckDB allocates
#   this via native malloc (off-heap), so it is outside JVM -Xmx but inside
#   the cgroup cap.  Auto-computed as MEM_LIMIT − JVM_XMX − 2G (OS overhead),
#   giving DuckDB the largest possible buffer pool within the cgroup — just as
#   Valinor gets the largest possible page cache within its cgroup.  Override
#   via the DUCKDB_MEMORY_LIMIT env var when needed.
#
#   At MEM_LIMIT=16G: buffer pool = 12GB (compressed taxi fits → near-zero I/O)
#   At MEM_LIMIT=8G:  buffer pool = 4GB  (partial caching → some I/O)
#
#   If DuckDB's columnar compression lets it cache the entire dataset within
#   its budget, that is a legitimate architectural advantage — the comparison
#   is fair because both systems operate under the same total memory cap.
#
# JVM_XMX — Java heap cap for the DuckDB Java wrapper.  DuckDB's heavy
#   lifting is in native memory, so the JVM heap only holds query result
#   objects and the thin Java wrapper.  2G is sufficient for all datasets.
#
# drop_caches is system-wide and runs BEFORE the cgroup process starts,
# ensuring a cold start.
# =============================================================================

# ---- Memory settings (override via env vars) ----
# Cgroup cap — must match the value used in exp_valinor.sh
MEM_LIMIT=${MEM_LIMIT:-16G}
# JVM heap — thin wrapper, 2G is enough for all datasets
JVM_XMX=${JVM_XMX:-2G}

# Parse to numeric GB for auto-computation
_mem_gb=${MEM_LIMIT%[Gg]}
_jvm_gb=${JVM_XMX%[Gg]}

# DuckDB buffer pool — auto-sized: MEM_LIMIT - JVM_XMX - 2G (OS overhead)
_duck_gb=$(( _mem_gb - _jvm_gb - 2 ))
(( _duck_gb < 1 )) && _duck_gb=1
export DUCKDB_MEMORY_LIMIT=${DUCKDB_MEMORY_LIMIT:-${_duck_gb}GB}
# DuckDB temporary directory for spills when buffer pool is full
export DUCKDB_TEMP_DIR=${DUCKDB_TEMP_DIR:-/data/smaroulis/.duckdb_tmp}

echo "=== Memory budget (DuckDB) ==="
echo "  Cgroup cap (MEM_LIMIT):       $MEM_LIMIT"
echo "  JVM heap cap (JVM_XMX):       $JVM_XMX"
echo "  DuckDB buffer pool:           $DUCKDB_MEMORY_LIMIT"
echo "  OS/page cache headroom:       ~$(( _mem_gb - _jvm_gb - ${DUCKDB_MEMORY_LIMIT//[^0-9]/} ))G"
echo "==============================="

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

# Explicitly use Java 21 (LTS)
# Override via: JAVA=/path/to/java ./exp_duckdb.sh
JAVA=${JAVA:-/usr/lib/jvm/java-21-openjdk-amd64/bin/java}

config_file="src/main/resources/experiments/experiment_scenarios.yaml"


# List of scenarios to run (override via env var)
# All scenarios (see experiment_scenarios.yaml):
#   exploration: synth10_300M_pan_sel1 synth50_pan_sel1 taxi_zoom gaia_dr3_pan ebird_us_pan
#   random:      gaia_dr3_random ebird_us_random taxi_random synth10_300M_random_sel1
scenarios=(${SCENARIOS:-gaia_dr3_pan})


# DuckDB execution modes (override via env var)
modes=(${MODES:-tableProjected})
# Available modes: tableProjected table directCSV spatialIndex

# Define the number of measure columns to test (override via env var)
num_measures_list=(${NUM_MEASURES:-1 2 4 6 8})

# Number of times to run each experiment (override via env var)
num_runs=${NUM_RUNS:-1}

# Optional: start run index (e.g., RUN_START=3 ./exp_duckdb.sh to start at run 3)
run_start=${RUN_START:-1}
run_end=$((run_start + num_runs - 1))

for mode in "${modes[@]}"
do
    for scenario in "${scenarios[@]}"
    do
        # Create the directory for results for each mode and scenario
        results_base=${RESULTS_BASE:-experiments/results}
        mode_results_dir="${results_base}/${scenario}/duckdb/${mode}/"
        mkdir -p "$mode_results_dir"
        for num_measures in "${num_measures_list[@]}"
        do
            for run in $(seq $run_start $run_end)
            do
                out_file="${mode_results_dir}results_mcols${num_measures}_run${run}.csv"
                if [[ -f "$out_file" ]]; then
                    echo "Skipping existing result: $out_file"
                    continue
                fi
                echo "Running DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run..."
                # Force cold disk reads for reproducible initialization timing
                sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                if sudo systemd-run --scope -p MemoryMax="$MEM_LIMIT" --quiet \
                    --setenv=DUCKDB_MEMORY_LIMIT="$DUCKDB_MEMORY_LIMIT" \
                    --setenv=DUCKDB_TEMP_DIR="$DUCKDB_TEMP_DIR" \
                    "$JAVA" -Xmx"$JVM_XMX" -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
                    -c timeDuckDBQueries \
                    -scenario "$scenario" \
                    -configFile "$config_file" \
                    -duckDbMode "$mode" \
                    -numMeasures $num_measures \
                    -run $run \
                    -out "$out_file"; then
                    if [[ -s "$out_file" ]]; then
                        echo "Completed DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run."
                    else
                        echo "FAILED DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run: output file missing or empty ($out_file)."
                        rm -f "$out_file"
                    fi
                else
                    status=$?
                    echo "FAILED DuckDB experiment for scenario $scenario, mode $mode, $num_measures measureCols, run $run with exit=$status."
                    rm -f "$out_file"
                fi
            done
        done
    done
done

echo "All DuckDB experiments completed."
