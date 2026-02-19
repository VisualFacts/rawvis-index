#!/usr/bin/env python3
"""
Download Gaia DR3 benchmark dataset as CSV via the ESA Gaia Archive TAP service.

Prerequisites:
  1. pip install astroquery astropy pandas
  2. No account needed (public data), but --login keeps async results longer.

Strategy:
  - Deterministic subsample via MOD(source_id, N) = 0.
  - Partitioned into 1° RA strips (360 async TAP jobs) to avoid timeouts.
  - Each strip saved as headerless, comma-delimited CSV chunk.
  - Chunks concatenated into final output file.
  - NULLs written as empty fields (parsed as Float.NaN by Valinor).

Columns (14, no source_id — source_id is a 19-digit long that doesn't fit float32):
  #  Column                     Description
  0  ra                         Right Ascension (degrees)
  1  dec                        Declination (degrees)
  2  phot_g_mean_mag            G-band magnitude        (~100% complete)
  3  phot_bp_mean_mag           Blue photometer mag      (~82% complete)
  4  phot_rp_mean_mag           Red photometer mag       (~87% complete)
  5  parallax                   Parallax in mas          (~82% complete)
  6  pmra                       Proper motion RA         (~82% complete)
  7  pmdec                      Proper motion Dec        (~82% complete)
  8  ruwe                       Astrometric quality      (~81% complete)
  9  phot_g_mean_flux           G-band flux              (~100% complete)
  10 phot_g_mean_flux_error     Flux uncertainty         (~100% complete)
  11 phot_bp_mean_flux          BP flux                  (~82% complete)
  12 phot_rp_mean_flux          RP flux                  (~87% complete)
  13 astrometric_excess_noise   Astrometric residual     (~100% complete)

Config: xColumn=0, yColumn=1, measureCols=[2..13], delimiter=','

Sampling rule: MOD(source_id, 10) = 0 → 10% sample (~180M rows)
  - Preserves spatial distribution (source_id encodes HEALPix in high bits)
  - Deterministic and reproducible for any Gaia DR3 mirror

Note: Gaia also provides a `random_index` column (uniform random permutation
of 0..N-1) designed for exactly this purpose. An alternative would be
`WHERE random_index < 181100000` for ~10%. We use MOD for simpler documentation.

Estimated CSV sizes (~160 bytes/row average):
  --sample-mod  5  (20%): ~362M rows → ~58 GB
  --sample-mod 10  (10%): ~181M rows → ~29 GB  ← default, fits in page cache
  --sample-mod 20   (5%):  ~90M rows → ~14.5 GB
  --sample-mod 50   (2%):  ~36M rows → ~5.8 GB
  --sample-mod 100  (1%):  ~18M rows → ~2.9 GB

Output: /data-nonraid/maroulis/data/gaia/gaia_dr3_benchmark.csv

Usage:
  python download_gaia_benchmark.py                   # 10% sample (default)
  python download_gaia_benchmark.py --sample-mod 20   # 5% sample
  python download_gaia_benchmark.py --sample-mod 100  # 1% sample
  python download_gaia_benchmark.py --login            # authenticated mode
"""

import os
import sys
import time
import argparse
import warnings
from pathlib import Path

try:
    from astroquery.gaia import Gaia
    import pandas as pd
except ImportError:
    print("ERROR: Required packages not found.")
    print("Install with:  pip install astroquery pandas")
    sys.exit(1)

# Suppress noisy astropy/astroquery warnings
warnings.filterwarnings('ignore', category=UserWarning, module='astropy')


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# 14 columns (no source_id). Order chosen so truncation from end is still valid.
# Column layout (0-indexed):
#   ra(0), dec(1), phot_g_mean_mag(2), phot_bp_mean_mag(3), phot_rp_mean_mag(4),
#   parallax(5), pmra(6), pmdec(7), ruwe(8), phot_g_mean_flux(9),
#   phot_g_mean_flux_error(10), phot_bp_mean_flux(11), phot_rp_mean_flux(12),
#   astrometric_excess_noise(13)
COLUMNS = (
    "ra, dec, "
    "phot_g_mean_mag, phot_bp_mean_mag, phot_rp_mean_mag, "
    "parallax, pmra, pmdec, ruwe, "
    "phot_g_mean_flux, phot_g_mean_flux_error, "
    "phot_bp_mean_flux, phot_rp_mean_flux, "
    "astrometric_excess_noise"
)

QUERY_TEMPLATE = (
    "SELECT {columns} "
    "FROM gaiadr3.gaia_source "
    "WHERE ra >= {ra_lo} AND ra < {ra_hi} "
    "AND MOD(source_id, {modulus}) = 0"
)


# ---------------------------------------------------------------------------
# Download logic
# ---------------------------------------------------------------------------

def submit_and_download_strip(ra_lo, ra_hi, modulus, output_path, max_retries=3):
    """Submit one RA-strip query as async TAP job and save as headerless CSV.

    Returns the number of rows written.
    """
    query = QUERY_TEMPLATE.format(
        columns=COLUMNS,
        ra_lo=ra_lo,
        ra_hi=ra_hi,
        modulus=modulus,
    )

    tbl = None
    for attempt in range(max_retries):
        try:
            job = Gaia.launch_job_async(query)
            tbl = job.get_results()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                print(f"\n    Attempt {attempt + 1} failed ({e}), "
                      f"retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise

    n_rows = len(tbl)
    if n_rows == 0:
        Path(output_path).touch()
        return 0

    # Convert to pandas: masked (NULL) values become NaN.
    # Write headerless CSV; NaNs written as empty fields.
    df = tbl.to_pandas()
    df.to_csv(output_path, index=False, header=False, na_rep='')
    return n_rows


def count_lines(filepath):
    """Count lines in a file efficiently."""
    count = 0
    with open(filepath, 'rb') as f:
        for _ in f:
            count += 1
    return count


def concatenate_chunks(chunk_dir, output_file):
    """Concatenate all chunk CSVs into a single file."""
    chunk_files = sorted(chunk_dir.glob("strip_*.csv"))
    print(f"\nConcatenating {len(chunk_files)} chunks into {output_file} ...")
    total_rows = 0
    with open(output_file, 'wb') as out:
        for cf in chunk_files:
            if cf.stat().st_size > 0:
                with open(cf, 'rb') as inp:
                    out.write(inp.read())
                total_rows += count_lines(cf)
    total_size = output_file.stat().st_size
    print(f"Done. {total_rows:,} rows, {total_size / 1e9:.1f} GB")
    return total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download Gaia DR3 benchmark dataset via TAP")
    parser.add_argument(
        "--ra-step", type=float, default=1.0,
        help="RA strip width in degrees (default: 1.0)")
    parser.add_argument(
        "--output-dir", type=str,
        default="/data-nonraid/maroulis/data/gaia/chunks",
        help="Directory for intermediate chunk files")
    parser.add_argument(
        "--output-file", type=str,
        default="/data-nonraid/maroulis/data/gaia/gaia_dr3_benchmark.csv",
        help="Final concatenated CSV path")
    parser.add_argument(
        "--sample-mod", type=int, default=10,
        help="Modulus N for MOD(source_id, N)=0 sampling "
             "(10→10%%, 20→5%%, 100→1%%; default: 10)")
    parser.add_argument(
        "--login", action="store_true",
        help="Login to Gaia archive (keeps async results forever)")
    parser.add_argument(
        "--resume-from", type=float, default=0.0,
        help="Resume from this RA value (skip earlier strips)")
    args = parser.parse_args()

    chunk_dir = Path(args.output_dir)
    chunk_dir.mkdir(parents=True, exist_ok=True)

    # Remove default row limit (astroquery caps at 50 by default for sync)
    Gaia.ROW_LIMIT = -1

    if args.login:
        Gaia.login()
        print("Authenticated to Gaia archive.")
    else:
        print("Using public (anonymous) access. "
              "Add --login for persistent server-side results.")

    sample_pct = 100.0 / args.sample_mod
    print(f"\nDownloading gaiadr3.gaia_source "
          f"({sample_pct:.1f}% sample, MOD(source_id, {args.sample_mod}) = 0)")
    print(f"RA strip width: {args.ra_step}°  |  Chunks → {chunk_dir}/\n")

    ra = args.resume_from
    session_rows = 0
    skipped = 0
    strip_idx = int(ra / args.ra_step)

    while ra < 360.0:
        ra_lo = ra
        ra_hi = min(ra + args.ra_step, 360.0)
        chunk_path = (chunk_dir /
                      f"strip_{strip_idx:04d}_ra{ra_lo:.1f}-{ra_hi:.1f}.csv")

        # Skip if already downloaded (resumability)
        if chunk_path.exists() and chunk_path.stat().st_size > 100:
            skipped += 1
            print(f"  [{strip_idx:4d}] RA [{ra_lo:6.1f}, {ra_hi:6.1f}) "
                  f"— exists, skipping")
            ra = ra_hi
            strip_idx += 1
            continue

        print(f"  [{strip_idx:4d}] RA [{ra_lo:6.1f}, {ra_hi:6.1f}) ",
              end="", flush=True)
        try:
            t0 = time.time()
            n = submit_and_download_strip(
                ra_lo, ra_hi, args.sample_mod, chunk_path)
            elapsed = time.time() - t0
            session_rows += n
            print(f"→ {n:,} rows in {elapsed:.0f}s  "
                  f"(session: {session_rows:,})")
        except Exception as e:
            print(f"ERROR: {e}")
            print(f"    Resume with: --resume-from {ra_lo}")
            sys.exit(1)

        ra = ra_hi
        strip_idx += 1

        # Be polite to ESA servers
        time.sleep(2)

    print(f"\nAll strips downloaded. "
          f"Session: {session_rows:,} new rows, {skipped} skipped.")
    total = concatenate_chunks(chunk_dir, Path(args.output_file))
    print(f"\nFinal dataset: {total:,} rows")


if __name__ == "__main__":
    main()
