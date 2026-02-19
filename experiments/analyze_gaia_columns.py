#!/usr/bin/env python3
"""Quick statistical analysis of the Gaia DR3 benchmark CSV for column profiling.

Uses pandas chunked reading (C parser) for speed.
Phase 1: Full scan for exact min/max/null counts via chunked aggregation.
Phase 2: Reservoir sample of ~2M rows for percentile/distribution analysis.
"""
import sys
import time
import numpy as np
import pandas as pd

DATA = "/data-nonraid/maroulis/data/gaia/gaia_dr3_benchmark.csv"
COLS = [
    "ra", "dec", "phot_g_mean_mag", "phot_bp_mean_mag", "phot_rp_mean_mag",
    "parallax", "pmra", "pmdec", "ruwe",
    "phot_g_mean_flux", "phot_g_mean_flux_error",
    "phot_bp_mean_flux", "phot_rp_mean_flux", "astrometric_excess_noise"
]
CHUNK_SIZE = 5_000_000   # 5M rows per chunk

print(f"=== Single-pass analysis: exact min/max + sampled distributions (chunked pandas) ===", flush=True)
t0 = time.time()

global_min = pd.Series([np.inf] * len(COLS), index=COLS)
global_max = pd.Series([-np.inf] * len(COLS), index=COLS)
null_counts = pd.Series([0] * len(COLS), index=COLS, dtype='int64')
total_rows = 0
# Collect sampled rows (~0.6% → ~2M from 328M)
SAMPLE_FRAC = 0.006
sampled_chunks = []

reader = pd.read_csv(DATA, header=None, names=COLS, chunksize=CHUNK_SIZE,
                      dtype='float64', na_values=['', 'nan'])
for i, chunk in enumerate(reader):
    total_rows += len(chunk)
    null_counts += chunk.isna().sum()
    chunk_min = chunk.min(skipna=True)
    chunk_max = chunk.max(skipna=True)
    global_min = pd.concat([global_min, chunk_min], axis=1).min(axis=1)
    global_max = pd.concat([global_max, chunk_max], axis=1).max(axis=1)
    # Sample from this chunk
    sampled_chunks.append(chunk.sample(frac=SAMPLE_FRAC, random_state=42))
    elapsed = time.time() - t0
    print(f"  Chunk {i+1}: {total_rows:>12,} rows  ({elapsed:.0f}s)", flush=True)

non_null = total_rows - null_counts
null_pct = 100.0 * null_counts / total_rows
sampled_df = pd.concat(sampled_chunks, ignore_index=True)
print(f"\nDone: {total_rows:,} rows, {len(sampled_df):,} sampled, in {time.time()-t0:.0f}s\n", flush=True)

# Print per-column stats
print(f"{'Column':<28s} {'Non-null':>12s} {'Null%':>7s} {'EXACT Min':>16s} {'P1':>14s} "
      f"{'P5':>14s} {'Median':>14s} {'P95':>14s} {'P99':>14s} {'EXACT Max':>16s} {'Mean':>14s} {'Std':>14s}")
print("-" * 205)

for col in COLS:
    nn = int(non_null[col])
    np_val = null_pct[col]
    arr = sampled_df[col].dropna().values
    if len(arr) == 0:
        print(f"{col:<28s} {nn:>12,d} {np_val:>6.1f}%  (all null in sample)")
        continue
    p1, p5, med, p95, p99 = np.percentile(arr, [1, 5, 50, 95, 99])
    gmin = global_min[col]
    gmax = global_max[col]
    print(f"{col:<28s} {nn:>12,d} {np_val:>6.1f}% {gmin:>16.6f} {p1:>14.4f} "
          f"{p5:>14.4f} {med:>14.4f} {p95:>14.4f} {p99:>14.4f} {gmax:>16.6f} "
          f"{arr.mean():>14.4f} {arr.std():>14.4f}")

# Print extreme outlier info for key columns
print("\n\n=== Extreme Value Analysis (exact min/max + sampled tail shape) ===\n")
for col in COLS:
    arr = sampled_df[col].dropna().values
    if len(arr) == 0:
        continue
    p001 = np.percentile(arr, 0.01)
    p9999 = np.percentile(arr, 99.99)
    n_extreme_low = np.sum(arr < p001)
    n_extreme_high = np.sum(arr > p9999)
    n_inf = np.sum(~np.isfinite(arr))
    n_neg = np.sum(arr < 0)
    gmin = global_min[col]
    gmax = global_max[col]
    # How far are the true extremes from the sampled percentiles?
    spread = p9999 - p001 if p9999 != p001 else 1.0
    min_ratio = (p001 - gmin) / spread if gmin < p001 else 0.0
    max_ratio = (gmax - p9999) / spread if gmax > p9999 else 0.0
    flag = ""
    if min_ratio > 10 or max_ratio > 10:
        flag = " *** EXTREME OUTLIER ***"
    elif min_ratio > 2 or max_ratio > 2:
        flag = " ** outlier"
    print(f"{col:<28s}  P0.01={p001:>14.4f}  P99.99={p9999:>14.4f}  "
          f"exactMin={gmin:>14.4f}  exactMax={gmax:>14.4f}  "
          f"inf/nan={n_inf:>4d}  negative={n_neg:>8d}{flag}")

# RA/Dec range for bounding box
ra_arr = sampled_df['ra'].dropna().values
dec_arr = sampled_df['dec'].dropna().values
print(f"\n\n=== Coordinate Ranges (exact) ===")
print(f"RA:  [{global_min['ra']:.6f}, {global_max['ra']:.6f}]")
print(f"Dec: [{global_min['dec']:.6f}, {global_max['dec']:.6f}]")

# Dynamic range analysis (important for float32 precision)
print(f"\n\n=== Dynamic Range Analysis (max/min ratio, relevant for float32 precision) ===\n")
for col in COLS:
    arr = sampled_df[col].dropna().values
    pos = arr[arr > 0]
    if len(pos) > 0:
        ratio = global_max[col] / pos.min() if pos.min() > 0 else float('inf')
        print(f"{col:<28s}  max/min_positive = {ratio:>14.2f}  (float32 ok if < ~1e7)")
    else:
        print(f"{col:<28s}  no positive values")
