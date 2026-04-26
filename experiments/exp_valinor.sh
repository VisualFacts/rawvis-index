#!/bin/bash

# =============================================================================
# VALINOR-A experiment runner
#
# Supports two approaches:
#   valinor_a — full system with aggregate metadata reuse (VALINOR-A)
#   valinor_s — VALINOR-S baseline: plain sampling, no metadata reuse
#
# All parameters are customizable via environment variables. Examples:
#
#   # Run only VALINOR-S with specific error bounds:
#   APPROACHES="valinor_s" ERROR_BOUNDS="0.05 0.1" ./exp_valinor.sh
#
#   # Run both approaches for taxi_zoom with 2 runs:
#   SCENARIOS="taxi_zoom" NUM_RUNS=2 ./exp_valinor.sh
#
#   # Run only VALINOR-A, starting from run 3:
#   APPROACHES="valinor_a" RUN_START=3 ./exp_valinor.sh
#
#   # Custom measure columns:
#   NUM_MEASURES="1 4" ./exp_valinor.sh
#
# ---- Memory constraint setup ----
#
# We use cgroups v2 to hard-cap total physical memory (heap + native + page
# cache) per experiment run.  This makes results reproducible regardless of
# how much RAM the host machine has.
#
# MEM_LIMIT — cgroup v2 hard cap on total physical memory.  Must be identical
#   across all systems being compared (Valinor, DuckDB, etc.) so that each
#   system gets the same hardware budget.  The system's architecture determines
#   how that budget is split between index structures, buffer pools, and OS
#   page cache.
#
# JVM_XMX — Java heap ceiling (-Xmx).  This is NOT a reservation; the JVM
#   only commits physical pages as objects are allocated.
#   Set to MEM_LIMIT - 2G by default, leaving ~2 GB for OS + page cache.
#   Since -Xms is unset, G1 grows on demand and shrinks back via
#   -XX:MaxHeapFreeRatio=30 after post-init System.gc().
#   Actual committed heap tracks live data: small datasets (~1 GB live) only
#   commit ~2 GB regardless of the 14G ceiling.
#   Sizing: rows × 24 bytes [SharedPointStore] steady; × 34 peak during partition.
#
# drop_caches is system-wide (not per-cgroup) and runs BEFORE the process
# enters the cgroup, ensuring a cold start.  The cgroup then limits how much
# page cache the process can accumulate during execution.
# =============================================================================

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LIBPATH="$SCRIPT_DIR/../native/build"

# Explicitly use Java 21 (LTS)
# Override via: JAVA=/path/to/java ./exp_valinor.sh
JAVA=${JAVA:-/usr/lib/jvm/java-21-openjdk-amd64/bin/java}

config_file="src/main/resources/experiments/experiment_scenarios.yaml"

# ---- Customizable parameters (override via env vars) ----

# Memory constraint (cgroup v2).  Same value must be used for all systems.
MEM_LIMIT=${MEM_LIMIT:-16G}

# Parse MEM_LIMIT to numeric GB for auto-computation
_mem_gb=${MEM_LIMIT%[Gg]}

# JVM heap cap — defaults to MEM_LIMIT - 2G.  The JVM only commits what it
# needs; G1 shrinks back after init via MaxHeapFreeRatio=30.
JVM_XMX=${JVM_XMX:-$(( _mem_gb - 2 ))G}
_jvm_gb=${JVM_XMX%[Gg]}

# Extra JVM options (e.g., -Dvalinor.mmap.dir=/tmp for mmap partition path)
# Chronicle Bytes requires these --add-opens on Java 17+
JVM_EXTRA_OPTS=${JVM_EXTRA_OPTS:-}
JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED"
JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS --add-opens=java.base/java.io=ALL-UNNAMED"
JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS --add-opens=java.base/java.util=ALL-UNNAMED"
JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS --add-opens=java.base/java.nio=ALL-UNNAMED"

echo "=== Memory budget (Valinor) ==="
echo "  Cgroup cap (MEM_LIMIT):  $MEM_LIMIT"
echo "  JVM heap cap (JVM_XMX):  $JVM_XMX"
echo "  Page cache headroom:     ~$(( _mem_gb - _jvm_gb ))G"
echo "==============================="

# Approaches to run: "valinor_a" (full system) and/or "valinor_s" (sampling-only baseline)
approaches=(${APPROACHES:-valinor_a valinor_s})

# List of scenarios to run
scenarios=(${SCENARIOS:-gaia_dr3_pan})
# All scenarios:
#   pan/zoom (visual exploration):
#     synth10_{50M,100M,300M,500M,1B}_pan_sel1
#     synth10_300M_pan_sel{001,01,5,10}
#     synth50_pan_sel1 taxi_zoom gaia_dr3_pan ebird_us_pan
#   random (uniform): gaia_dr3_random ebird_us_random taxi_random synth10_300M_random_sel1
#
# Convenience groupings (override SCENARIOS to use):
#   SCENARIOS_EXPLORATION="synth10_300M_pan_sel1 taxi_zoom gaia_dr3_pan ebird_us_pan"
#   SCENARIOS_RANDOM="gaia_dr3_random ebird_us_random taxi_random synth10_300M_random_sel1"

# Error bounds to sweep
error_bounds=(${ERROR_BOUNDS:-0 0.01 0.02 0.05 0.1})

# Number of measure columns to test
num_measures_list=(${NUM_MEASURES:-1 2 4 6 8})

# Combination pruning: which measure counts get the full error bound sweep
fixed_measures_for_error_bounds=(${FIXED_MEASURES_FOR_EB:-1 4})

# Combination pruning: which error bounds get the full measure count sweep
fixed_error_bounds_for_measures=(${FIXED_EB_FOR_MEASURES:-0 0.01 0.05})

# Parent results directory (override to write results to a different folder)
results_base=${RESULTS_BASE:-experiments/results}

# Number of runs
num_runs=${NUM_RUNS:-1}

# Start run index
run_start=${RUN_START:-1}
run_end=$((run_start + num_runs - 1))

# Index construction parameters
# RESOLUTION = partitions per axis for the initial uniform grid (G×G cells)
resolution_list=(${RESOLUTION:-500})
# SUBTILE_RATIO = fraction of G² cells that get query-biased sub-tiling
subtile_ratio=${SUBTILE_RATIO:-0.2}

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

# ---- Main loop ----

for approach in "${approaches[@]}"
do
    # Build extra args and results subdirectory based on approach
    extra_args=""
    if [[ "$approach" == "valinor_s" ]]; then
        extra_args="--samplingOnly"
        subdir="valinor_s/"
    elif [[ "$approach" == "valinor_a" ]]; then
        extra_args=""
        subdir="valinor_a/"
    else
        echo "Unknown approach: $approach (expected valinor_a or valinor_s)"
        continue
    fi

    for run in $(seq $run_start $run_end)
    do
        for scenario in "${scenarios[@]}"
        do
            for resolution in "${resolution_list[@]}"
            do
            results_dir="${results_base}/${scenario}/${subdir}"
            mkdir -p "$results_dir"
            for num_measures in "${num_measures_list[@]}"
            do
                for error_bound in "${error_bounds[@]}"
                do
                    # valinor_s only makes sense with errorBound > 0
                    if [[ "$approach" == "valinor_s" && "$error_bound" == "0" ]]; then
                        continue
                    fi

                    # Reduce combinations:
                    # - When varying measures: only fixed_error_bounds_for_measures
                    # - When varying error bounds: only fixed_measures_for_error_bounds
                    if ! contains "$num_measures" "${fixed_measures_for_error_bounds[@]}"; then
                        if ! contains "$error_bound" "${fixed_error_bounds_for_measures[@]}"; then
                            continue
                        fi
                    fi

                    # Build filename: include resolution and subtile ratio only if non-default
                    out_name="results_mcols${num_measures}_error${error_bound}"
                    out_name="${out_name}_res${resolution}_str${subtile_ratio}"
                    out_name="${out_name}_run${run}.csv"
                    out_file="${results_dir}${out_name}"
                    if [[ -f "$out_file" ]]; then
                        echo "Skipping existing result: $out_file"
                        continue
                    fi
                    echo "[$approach] Running scenario=$scenario mcols=$num_measures error=$error_bound res=$resolution str=$subtile_ratio run=$run..."
                    # Force cold disk reads for reproducible initialization timing
                    sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                    if sudo systemd-run --scope -p MemoryMax="$MEM_LIMIT" --quiet \
                        "$JAVA" -Xmx"$JVM_XMX" -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30 \
                        -Djava.library.path="$LIBPATH" $JVM_EXTRA_OPTS -jar target/experiments.jar \
                        -c timeApproximateQueries \
                        -scenario "$scenario" \
                        -configFile "$config_file" \
                        -initMode queryBiased \
                        -numMeasures $num_measures \
                        -errorBound $error_bound \
                        -resolution $resolution \
                        -subtileRatio $subtile_ratio \
                        -run $run \
                        $extra_args \
                        -out "$out_file"; then
                        if [[ -s "$out_file" ]]; then
                            echo "[$approach] Completed scenario=$scenario mcols=$num_measures error=$error_bound res=$resolution run=$run."
                        else
                            echo "[$approach] FAILED scenario=$scenario mcols=$num_measures error=$error_bound res=$resolution run=$run: output file missing or empty ($out_file)."
                            rm -f "$out_file"
                        fi
                    else
                        status=$?
                        echo "[$approach] FAILED scenario=$scenario mcols=$num_measures error=$error_bound res=$resolution run=$run with exit=$status."
                        rm -f "$out_file"
                    fi
                done
            done
            done  # resolution
        done
    done
done

echo "All experiments completed."