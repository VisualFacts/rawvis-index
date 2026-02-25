#!/bin/bash
# =============================================================================
# Experiment orchestration script — runs blocks sequentially.
# Each block calls exp_valinor.sh or exp_duckdb.sh with env-var overrides.
# Comment out blocks you don't want to run.
#
# Usage:
#   nohup ./experiments/run_experiments.sh >> experiments/run_experiments.log 2>&1 &
#   tail -f experiments/run_experiments.log
# =============================================================================

set -e   # abort on any non-zero exit
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

run_valinor() { "$SCRIPT_DIR/exp_valinor.sh"; }
run_duckdb()  { "$SCRIPT_DIR/exp_duckdb.sh";  }

# # =============================================================================
# # Block 1 — Selectivity sweep test (VALINOR-A, error=0, 1 measure, 1 run)
# # =============================================================================
# echo "===== Block 1: selectivity sweep test ====="
# SCENARIOS="synth50_pan_sel001 synth50_pan_sel01 synth50_pan_sel1 synth50_pan_sel5" \
# NUM_MEASURES="1" \
# ERROR_BOUNDS="0" \
# NUM_RUNS=1 \
# APPROACHES="valinor_a" \
# run_valinor

# =============================================================================
# Block 2 — synth10_500M pan: 2 selectivity levels, error bounds 0/0.01/0.5, 1 measure, 1 run
# =============================================================================
echo "===== Block 2: synth10_500M pan selectivity sweep ====="
SCENARIOS="synth10_500M_pan_sel01 synth10_500M_pan_sel1" \
NUM_MEASURES="1" \
ERROR_BOUNDS="0 0.01 0.5" \
NUM_RUNS=1 \
APPROACHES="valinor_a" \
run_valinor

# =============================================================================
# Block 3 — Selectivity sweep: full error bound × measure sweep (VALINOR-A only)
# =============================================================================
# echo "===== Block 3: selectivity sweep full run ====="
# SCENARIOS="synth50_pan_sel001 synth50_pan_sel01 synth50_pan_sel1 synth50_pan_sel5" \
# APPROACHES="valinor_a valinor_s" \
# ERROR_BOUNDS="0 0.01 0.02 0.05 0.1" \
# NUM_MEASURES="1 4 8" \
# NUM_RUNS=3 \
# run_valinor

# =============================================================================
# Block 4 — Main scenarios: VALINOR-A + VALINOR-S
# =============================================================================
# echo "===== Block 4: main scenarios ====="
# SCENARIOS="synth50_pan taxi_pan taxi_zoom gaia_dr3_shuffled_pan" \
# APPROACHES="valinor_a valinor_s" \
# NUM_RUNS=3 \
# run_valinor

# =============================================================================
# Block 5 — DuckDB baseline for selectivity scenarios
# =============================================================================
# echo "===== Block 5: DuckDB selectivity ====="
# SCENARIOS="synth50_pan_sel001 synth50_pan_sel01 synth50_pan_sel1 synth50_pan_sel5" \
# NUM_MEASURES="1" \
# NUM_RUNS=1 \
# run_duckdb

# =============================================================================
# Block 6 — DuckDB baseline for main scenarios
# =============================================================================
# echo "===== Block 6: DuckDB main scenarios ====="
# SCENARIOS="synth50_pan taxi_pan gaia_dr3_shuffled_pan" \
# NUM_RUNS=3 \
# run_duckdb

echo "===== All blocks completed ====="
