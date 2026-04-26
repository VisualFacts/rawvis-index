#!/bin/bash

# PilotDB requires Python >= 3.11 so it lives in a separate venv (.venv-pilotdb).
# To set it up on a new machine:
#   uv python install 3.11
#   uv venv .venv-pilotdb --python 3.11
#   source .venv-pilotdb/bin/activate
#   uv pip install pyyaml duckdb
#   git clone --depth 1 https://github.com/uiuc-kang-lab/PilotDB.git ~/PilotDB
#   uv pip install -e ~/PilotDB
#   uv pip install "sqlglot==26.30.0"   # PilotDB breaks with sqlglot >= 28

# ---- Memory settings (override via env vars) ----
# Cgroup cap — must match the value used in exp_valinor.sh / exp_duckdb.sh
MEM_LIMIT=${MEM_LIMIT:-16G}

# Parse to numeric GB for auto-computation
_mem_gb=${MEM_LIMIT%[Gg]}

# DuckDB buffer pool — same 12GB as in exp_duckdb.sh (16G cap - 4G overhead).
# In exp_duckdb.sh the 4G covers JVM (2G) + OS (2G); here it covers
# Python/PilotDB + OS.  Either way DuckDB gets the same budget.
_duck_gb=$(( _mem_gb - 4 ))
(( _duck_gb < 1 )) && _duck_gb=1
export DUCKDB_MEMORY_LIMIT=${DUCKDB_MEMORY_LIMIT:-${_duck_gb}GB}
# DuckDB temporary directory for spills when buffer pool is full
export DUCKDB_TEMP_DIR=${DUCKDB_TEMP_DIR:-/data/smaroulis/.duckdb_tmp}

echo "=== Memory budget (PilotDB) ==="
echo "  Cgroup cap (MEM_LIMIT):       $MEM_LIMIT"
echo "  DuckDB buffer pool:           $DUCKDB_MEMORY_LIMIT"
echo "==============================="

# Explicitly use Java 21 (LTS, same as exp_valinor.sh) for query generation
JAVA=${JAVA:-/usr/lib/jvm/java-21-openjdk-amd64/bin/java}

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(readlink -f "$SCRIPT_DIR/..")
LIBPATH="$PROJECT_ROOT/native/build"

config_file="src/main/resources/experiments/experiment_scenarios.yaml"

# ---- Customizable parameters (override via env vars) ----

# List of scenarios to run
scenarios=(${SCENARIOS:-gaia_dr3_shuffled_pan})
# All scenarios (see experiment_scenarios.yaml):
#   exploration: synth10_300M_pan_sel1 synth50_pan_sel1 taxi_zoom gaia_dr3_pan ebird_us_pan
#   random:      gaia_dr3_random ebird_us_random taxi_random synth10_300M_random_sel1

# Define the number of measure columns to test
num_measures_list=(${NUM_MEASURES:-1 2 4 6 8})

# Define error bounds
error_bounds=(${ERROR_BOUNDS:-0.01 0.02 0.05 0.1})

# Combination pruning: which measure counts get the full error bound sweep
fixed_measures_for_error_bounds=(${FIXED_MEASURES_FOR_EB:-1 4})

# Combination pruning: which error bounds get the full measure count sweep
fixed_error_bounds_for_measures=(${FIXED_EB_FOR_MEASURES:-0.01 0.05})

# Number of runs
num_runs=${NUM_RUNS:-1}

# Start run index (e.g., RUN_START=3 ./exp_pilotdb.sh to start at run 3)
run_start=${RUN_START:-1}
run_end=$((run_start + num_runs - 1))

# ---- Helper ----

contains() {
    local value="$1"
    shift
    for item in "$@"; do
        if [[ "$item" == "$value" ]]; then
            return 0
        fi
    done
    return 1
}

# ---- Pre-generate SQL files ----

for scenario in "${scenarios[@]}"
do
    results_base=${RESULTS_BASE:-experiments/results}
    results_dir="${results_base}/${scenario}/pilotdb/"
    mkdir -p "$results_dir"

    # Generate PilotDB SQL files (one per num_measures value)
    for num_measures in "${num_measures_list[@]}"
    do
        sql_file="${results_dir}pilotdb_mcols${num_measures}.sql"
        if [[ ! -f "$sql_file" ]]; then
            echo "Generating PilotDB SQL file for scenario=$scenario mcols=$num_measures..."
            "$JAVA" -Xmx2G -Djava.library.path="$LIBPATH" -jar target/experiments.jar \
                -c generatePilotDBSqlFile \
                -scenario "$scenario" \
                -configFile "$config_file" \
                -numMeasures "$num_measures" \
                -out "$sql_file"
            if [[ ! -f "$sql_file" ]]; then
                echo "Failed to generate SQL file for scenario=$scenario mcols=$num_measures."
            fi
        fi
    done
done

# ---- Main loop (run is outermost so run 1 completes across all configs first) ----

for run in $(seq $run_start $run_end)
do
    for scenario in "${scenarios[@]}"
    do
        results_base=${RESULTS_BASE:-experiments/results}
        results_dir="${results_base}/${scenario}/pilotdb/"

        for num_measures in "${num_measures_list[@]}"
        do
            sql_file="${results_dir}pilotdb_mcols${num_measures}.sql"
            if [[ ! -f "$sql_file" ]]; then
                echo "No SQL file for scenario=$scenario mcols=$num_measures. Skipping."
                continue
            fi

            for error_bound in "${error_bounds[@]}"
            do
                # Combination pruning (same logic as exp_valinor.sh):
                # - When varying measures: only fixed_error_bounds_for_measures
                # - When varying error bounds: only fixed_measures_for_error_bounds
                if ! contains "$num_measures" "${fixed_measures_for_error_bounds[@]}"; then
                    if ! contains "$error_bound" "${fixed_error_bounds_for_measures[@]}"; then
                        continue
                    fi
                fi

                out_file="${results_dir}results_mcols${num_measures}_error${error_bound}_run${run}.csv"
                if [[ -f "$out_file" ]]; then
                    echo "Skipping existing result: $out_file"
                    continue
                fi

                echo "[run $run] PilotDB scenario=$scenario mcols=$num_measures error=$error_bound..."
                # Force cold disk reads for reproducible initialization timing
                sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

                if sudo systemd-run --scope -p MemoryMax="$MEM_LIMIT" --quiet \
                    --setenv=DUCKDB_MEMORY_LIMIT="$DUCKDB_MEMORY_LIMIT" \
                    --setenv=DUCKDB_TEMP_DIR="$DUCKDB_TEMP_DIR" \
                    --working-directory="$PROJECT_ROOT" \
                    "$PROJECT_ROOT/.venv-pilotdb/bin/python" "$SCRIPT_DIR/run_pilotdb_queries.py" \
                    --sql-file "$sql_file" \
                    --error "$error_bound" \
                    --out "$out_file"; then
                    if [[ -s "$out_file" ]]; then
                        echo "[run $run] Completed scenario=$scenario mcols=$num_measures error=$error_bound."
                    else
                        echo "[run $run] FAILED scenario=$scenario mcols=$num_measures error=$error_bound: output file missing or empty ($out_file)."
                        rm -f "$out_file"
                    fi
                else
                    status=$?
                    echo "[run $run] FAILED scenario=$scenario mcols=$num_measures error=$error_bound with exit=$status."
                    rm -f "$out_file"
                fi
            done
        done
    done
done

echo "All PilotDB experiments completed."