#!/usr/bin/env python3
"""
Detailed analysis of the filtered US eBird dataset (2023-2026).
Focuses on: lat/lon density & bounds, date range confirmation,
measure column statistics including CV (coefficient of variation).
"""

import duckdb
import sys
import math

FILE = "/data/smaroulis/ebd_US_2023_2026.txt"

def main():
    con = duckdb.connect()
    # Increase memory limit for large aggregations
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET threads = 10")

    print(f"eBird US 2023-2026 Detailed Analysis: {FILE}")
    print("=" * 80)

    # ── Total rows ──
    row_count = con.execute(f"""
        SELECT COUNT(*) FROM read_csv('{FILE}', delim='\t', header=true,
            nullstr='', ignore_errors=true, parallel=true)
    """).fetchone()[0]
    print(f"\nTOTAL ROWS: {row_count:,}")

    # ── Date range confirmation ──
    print("\n" + "─" * 80)
    print("DATE RANGE CONFIRMATION")
    print("─" * 80)
    date_stats = con.execute(f"""
        SELECT
            MIN("OBSERVATION DATE") AS min_date,
            MAX("OBSERVATION DATE") AS max_date,
            COUNT(DISTINCT EXTRACT(YEAR FROM "OBSERVATION DATE"::DATE)) AS n_years,
            SUM(CASE WHEN "OBSERVATION DATE" IS NULL THEN 1 ELSE 0 END) AS null_dates
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
    """).fetchone()
    print(f"  Min date:      {date_stats[0]}")
    print(f"  Max date:      {date_stats[1]}")
    print(f"  Distinct years: {date_stats[2]}")
    print(f"  Null/empty dates: {date_stats[3]:,}")

    # Rows per year
    print("\n  Rows per year:")
    years = con.execute(f"""
        SELECT EXTRACT(YEAR FROM "OBSERVATION DATE"::DATE) AS yr, COUNT(*) AS cnt
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        GROUP BY yr ORDER BY yr
    """).fetchall()
    for yr, cnt in years:
        pct = cnt / row_count * 100
        print(f"    {int(yr):>6}  {cnt:>15,}  ({pct:5.2f}%)")

    # Rows per month (aggregated across years)
    print("\n  Rows per month (all years):")
    months = con.execute(f"""
        SELECT EXTRACT(MONTH FROM "OBSERVATION DATE"::DATE) AS mo, COUNT(*) AS cnt
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        GROUP BY mo ORDER BY mo
    """).fetchall()
    for mo, cnt in months:
        pct = cnt / row_count * 100
        bar = "█" * int(pct / 2)
        print(f"    {int(mo):>2}  {cnt:>15,}  ({pct:5.2f}%) {bar}")

    # ── Geographic bounds ──
    print("\n" + "─" * 80)
    print("GEOGRAPHIC BOUNDS")
    print("─" * 80)
    geo = con.execute(f"""
        SELECT
            MIN(LATITUDE) AS lat_min, MAX(LATITUDE) AS lat_max,
            AVG(LATITUDE) AS lat_avg, STDDEV_SAMP(LATITUDE) AS lat_std,
            MIN(LONGITUDE) AS lon_min, MAX(LONGITUDE) AS lon_max,
            AVG(LONGITUDE) AS lon_avg, STDDEV_SAMP(LONGITUDE) AS lon_std,
            SUM(CASE WHEN LATITUDE IS NULL THEN 1 ELSE 0 END) AS lat_nulls,
            SUM(CASE WHEN LONGITUDE IS NULL THEN 1 ELSE 0 END) AS lon_nulls
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
    """).fetchone()
    print(f"  Latitude:   min={geo[0]:.6f}  max={geo[1]:.6f}  avg={geo[2]:.6f}  std={geo[3]:.6f}  nulls={geo[8]:,}")
    print(f"  Longitude:  min={geo[4]:.6f}  max={geo[5]:.6f}  avg={geo[6]:.6f}  std={geo[7]:.6f}  nulls={geo[9]:,}")

    # ── Lat/Lon density: grid-based histogram ──
    print("\n" + "─" * 80)
    print("LAT/LON DENSITY: 5° x 5° GRID CELLS (Continental US focus)")
    print("─" * 80)
    # Continental US roughly: lat 24-50, lon -126 to -66
    grid = con.execute(f"""
        SELECT
            FLOOR(LATITUDE / 5) * 5 AS lat_bin,
            FLOOR(LONGITUDE / 5) * 5 AS lon_bin,
            COUNT(*) AS cnt
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        WHERE LATITUDE BETWEEN 24 AND 50 AND LONGITUDE BETWEEN -126 AND -66
        GROUP BY lat_bin, lon_bin
        ORDER BY cnt DESC
    """).fetchall()
    total_conus = sum(r[2] for r in grid)
    print(f"  Continental US rows (lat 24-50, lon -126 to -66): {total_conus:,}")
    print(f"  {'Lat Bin':>10}  {'Lon Bin':>10}  {'Count':>15}  {'% of CONUS':>10}")
    print(f"  {'─'*10}  {'─'*10}  {'─'*15}  {'─'*10}")
    for lat_bin, lon_bin, cnt in grid[:30]:
        pct = cnt / total_conus * 100
        print(f"  {lat_bin:>10.0f}  {lon_bin:>10.0f}  {cnt:>15,}  {pct:>9.2f}%")
    if len(grid) > 30:
        print(f"  ... ({len(grid) - 30} more cells)")

    # ── Density by US state (top 20) ──
    print("\n" + "─" * 80)
    print("ROWS BY US STATE (Top 30)")
    print("─" * 80)
    states = con.execute(f"""
        SELECT "STATE" AS state, "STATE CODE" AS code, COUNT(*) AS cnt
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        GROUP BY state, code
        ORDER BY cnt DESC
        LIMIT 30
    """).fetchall()
    print(f"  {'State':<30}  {'Code':<8}  {'Count':>15}  {'%':>7}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*15}  {'─'*7}")
    for state, code, cnt in states:
        pct = cnt / row_count * 100
        print(f"  {state:<30}  {code:<8}  {cnt:>15,}  {pct:>6.2f}%")

    # ── Lat/Lon density: 1° grid for hotspot detection ──
    print("\n" + "─" * 80)
    print("TOP 20 HOTSPOT 1° CELLS (highest density)")
    print("─" * 80)
    hotspots = con.execute(f"""
        SELECT
            FLOOR(LATITUDE) AS lat_deg,
            FLOOR(LONGITUDE) AS lon_deg,
            COUNT(*) AS cnt
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        GROUP BY lat_deg, lon_deg
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    print(f"  {'Lat°':>6}  {'Lon°':>7}  {'Count':>15}  {'% of total':>10}")
    print(f"  {'─'*6}  {'─'*7}  {'─'*15}  {'─'*10}")
    for lat, lon, cnt in hotspots:
        pct = cnt / row_count * 100
        print(f"  {lat:>6.0f}  {lon:>7.0f}  {cnt:>15,}  {pct:>9.2f}%")

    # ── Non-continental outliers ──
    print("\n" + "─" * 80)
    print("NON-CONTINENTAL US OBSERVATIONS")
    print("─" * 80)
    regions = con.execute(f"""
        SELECT
            CASE
                WHEN LATITUDE > 50 AND LONGITUDE < -125 THEN 'Alaska'
                WHEN LATITUDE BETWEEN 18 AND 23 AND LONGITUDE BETWEEN -162 AND -154 THEN 'Hawaii'
                WHEN LATITUDE BETWEEN 17 AND 19 AND LONGITUDE BETWEEN -68 AND -64 THEN 'Puerto Rico/USVI'
                WHEN LATITUDE BETWEEN 13 AND 16 AND LONGITUDE BETWEEN -68 AND -64 THEN 'Caribbean territories'
                WHEN LATITUDE < 24 AND LONGITUDE BETWEEN -126 AND -66 THEN 'South of CONUS'
                WHEN LATITUDE > 50 AND LONGITUDE > -125 THEN 'Northern territories'
                WHEN LATITUDE BETWEEN 24 AND 50 AND LONGITUDE BETWEEN -126 AND -66 THEN 'Continental US'
                ELSE 'Other/Overseas'
            END AS region,
            COUNT(*) AS cnt,
            MIN(LATITUDE) AS lat_min, MAX(LATITUDE) AS lat_max,
            MIN(LONGITUDE) AS lon_min, MAX(LONGITUDE) AS lon_max
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        GROUP BY region ORDER BY cnt DESC
    """).fetchall()
    print(f"  {'Region':<25}  {'Count':>15}  {'%':>7}  {'Lat Range':>20}  {'Lon Range':>22}")
    print(f"  {'─'*25}  {'─'*15}  {'─'*7}  {'─'*20}  {'─'*22}")
    for region, cnt, lat_min, lat_max, lon_min, lon_max in regions:
        pct = cnt / row_count * 100
        print(f"  {region:<25}  {cnt:>15,}  {pct:>6.2f}%  [{lat_min:>8.4f}, {lat_max:>8.4f}]  [{lon_min:>9.4f}, {lon_max:>9.4f}]")

    # ── Measure column statistics with CV ──
    print("\n" + "─" * 80)
    print("MEASURE COLUMN STATISTICS (with Coefficient of Variation)")
    print("─" * 80)

    # OBSERVATION COUNT — varchar column, may contain 'X'
    print("\n  OBSERVATION COUNT")
    obs = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN "OBSERVATION COUNT" = 'X' THEN 1 ELSE 0 END) AS x_count,
            SUM(CASE WHEN "OBSERVATION COUNT" IS NULL THEN 1 ELSE 0 END) AS null_count
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
    """).fetchone()
    print(f"    Total: {obs[0]:,}  |  'X' (presence-only): {obs[1]:,} ({obs[1]/obs[0]*100:.2f}%)  |  Null/empty: {obs[2]:,}")

    obs_stats = con.execute(f"""
        SELECT
            COUNT(val) AS n,
            MIN(val) AS min_v, MAX(val) AS max_v,
            AVG(val) AS avg_v, STDDEV_SAMP(val) AS std_v,
            MEDIAN(val) AS median_v,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) AS p75,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY val) AS p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY val) AS p99,
            SUM(CASE WHEN val = 0 THEN 1 ELSE 0 END) AS zeros
        FROM (
            SELECT TRY_CAST("OBSERVATION COUNT" AS DOUBLE) AS val
            FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
            WHERE "OBSERVATION COUNT" != 'X' AND "OBSERVATION COUNT" IS NOT NULL
        )
    """).fetchone()
    n, mn, mx, avg, std, med, p25, p75, p95, p99, zeros = obs_stats
    cv = (std / avg * 100) if avg and avg != 0 else float('nan')
    print(f"    Numeric rows: {n:,}  |  Zeros: {zeros:,}")
    print(f"    Min: {mn:.2f}  Max: {mx:.2f}  Mean: {avg:.2f}  Std: {std:.2f}  CV: {cv:.1f}%")
    print(f"    Median: {med:.1f}  P25: {p25:.1f}  P75: {p75:.1f}  P95: {p95:.1f}  P99: {p99:.1f}")

    # Remaining numeric measure columns
    measure_cols = [
        ("DURATION MINUTES", "BIGINT"),
        ("EFFORT DISTANCE KM", "DOUBLE"),
        ("EFFORT AREA HA", "DOUBLE"),
        ("NUMBER OBSERVERS", "BIGINT"),
    ]

    for col_name, col_type in measure_cols:
        print(f"\n  {col_name}")
        stats = con.execute(f"""
            SELECT
                COUNT("{col_name}") AS n,
                SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) AS nulls,
                MIN("{col_name}") AS min_v,
                MAX("{col_name}") AS max_v,
                AVG("{col_name}"::DOUBLE) AS avg_v,
                STDDEV_SAMP("{col_name}"::DOUBLE) AS std_v,
                MEDIAN("{col_name}"::DOUBLE) AS median_v,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col_name}"::DOUBLE) AS p25,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col_name}"::DOUBLE) AS p75,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "{col_name}"::DOUBLE) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "{col_name}"::DOUBLE) AS p99,
                SUM(CASE WHEN "{col_name}" = 0 THEN 1 ELSE 0 END) AS zeros
            FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        """).fetchone()
        n, nulls, mn, mx, avg, std, med, p25, p75, p95, p99, zeros = stats
        null_pct = nulls / row_count * 100
        cv = (std / avg * 100) if avg and avg != 0 else float('nan')
        print(f"    Non-null: {n:,}  |  Null: {nulls:,} ({null_pct:.2f}%)  |  Zeros: {zeros:,}")
        print(f"    Min: {mn}  Max: {mx}  Mean: {avg:.4f}  Std: {std:.4f}  CV: {cv:.1f}%")
        print(f"    Median: {med:.2f}  P25: {p25:.2f}  P75: {p75:.2f}  P95: {p95:.2f}  P99: {p99:.2f}")

    # ── OBSERVATION COUNT distribution buckets ──
    print("\n" + "─" * 80)
    print("OBSERVATION COUNT DISTRIBUTION (log-scale buckets)")
    print("─" * 80)
    buckets = con.execute(f"""
        WITH parsed AS (
            SELECT TRY_CAST("OBSERVATION COUNT" AS DOUBLE) AS val
            FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
            WHERE "OBSERVATION COUNT" != 'X' AND "OBSERVATION COUNT" IS NOT NULL
        )
        SELECT
            CASE
                WHEN val = 1 THEN '1'
                WHEN val BETWEEN 2 AND 5 THEN '2-5'
                WHEN val BETWEEN 6 AND 10 THEN '6-10'
                WHEN val BETWEEN 11 AND 50 THEN '11-50'
                WHEN val BETWEEN 51 AND 100 THEN '51-100'
                WHEN val BETWEEN 101 AND 500 THEN '101-500'
                WHEN val BETWEEN 501 AND 1000 THEN '501-1K'
                WHEN val BETWEEN 1001 AND 10000 THEN '1K-10K'
                WHEN val > 10000 THEN '>10K'
                ELSE 'other'
            END AS bucket,
            COUNT(*) AS cnt
        FROM parsed
        GROUP BY bucket
        ORDER BY MIN(val)
    """).fetchall()
    total_numeric = sum(r[1] for r in buckets)
    print(f"  {'Bucket':<12}  {'Count':>15}  {'%':>7}")
    print(f"  {'─'*12}  {'─'*15}  {'─'*7}")
    for bucket, cnt in buckets:
        pct = cnt / total_numeric * 100
        print(f"  {bucket:<12}  {cnt:>15,}  {pct:>6.2f}%")

    # ── DURATION MINUTES distribution ──
    print("\n" + "─" * 80)
    print("DURATION MINUTES DISTRIBUTION")
    print("─" * 80)
    dur_buckets = con.execute(f"""
        SELECT
            CASE
                WHEN "DURATION MINUTES" IS NULL THEN 'null'
                WHEN "DURATION MINUTES" = 0 THEN '0'
                WHEN "DURATION MINUTES" BETWEEN 1 AND 15 THEN '1-15'
                WHEN "DURATION MINUTES" BETWEEN 16 AND 30 THEN '16-30'
                WHEN "DURATION MINUTES" BETWEEN 31 AND 60 THEN '31-60'
                WHEN "DURATION MINUTES" BETWEEN 61 AND 120 THEN '61-120'
                WHEN "DURATION MINUTES" BETWEEN 121 AND 240 THEN '121-240'
                WHEN "DURATION MINUTES" BETWEEN 241 AND 480 THEN '241-480'
                WHEN "DURATION MINUTES" > 480 THEN '>480'
            END AS bucket,
            COUNT(*) AS cnt
        FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        GROUP BY bucket
        ORDER BY MIN("DURATION MINUTES") NULLS FIRST
    """).fetchall()
    print(f"  {'Bucket':<12}  {'Count':>15}  {'%':>7}")
    print(f"  {'─'*12}  {'─'*15}  {'─'*7}")
    for bucket, cnt in dur_buckets:
        pct = cnt / row_count * 100
        print(f"  {bucket:<12}  {cnt:>15,}  {pct:>6.2f}%")

    # ── Spatial coverage: percentage of 0.1° cells with observations ──
    print("\n" + "─" * 80)
    print("SPATIAL COVERAGE (0.1° grid cells with ≥1 observation)")
    print("─" * 80)
    # Continental US: lat 24-50 (260 bins), lon -126 to -66 (600 bins) = 156,000 possible cells
    coverage = con.execute(f"""
        WITH cells AS (
            SELECT DISTINCT
                FLOOR(LATITUDE * 10) AS lat10,
                FLOOR(LONGITUDE * 10) AS lon10
            FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
            WHERE LATITUDE BETWEEN 24 AND 50 AND LONGITUDE BETWEEN -126 AND -66
        )
        SELECT COUNT(*) FROM cells
    """).fetchone()[0]
    possible = 260 * 600  # approximate
    print(f"  CONUS 0.1° cells with data: {coverage:,} / ~{possible:,} ({coverage/possible*100:.1f}%)")

    # Full US (including AK/HI)
    coverage_all = con.execute(f"""
        WITH cells AS (
            SELECT DISTINCT
                FLOOR(LATITUDE * 10) AS lat10,
                FLOOR(LONGITUDE * 10) AS lon10
            FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        )
        SELECT COUNT(*) FROM cells
    """).fetchone()[0]
    print(f"  All US 0.1° cells with data: {coverage_all:,}")

    # ── Measure CV comparison table ──
    print("\n" + "─" * 80)
    print("MEASURE CV SUMMARY")
    print("─" * 80)
    print(f"  {'Measure':<25}  {'Mean':>12}  {'Std':>12}  {'CV%':>8}  {'Median':>10}  {'IQR':>12}  {'Non-null':>15}")
    print(f"  {'─'*25}  {'─'*12}  {'─'*12}  {'─'*8}  {'─'*10}  {'─'*12}  {'─'*15}")

    # Re-query all measures together for the summary
    summary = con.execute(f"""
        WITH raw AS (
            SELECT
                TRY_CAST("OBSERVATION COUNT" AS DOUBLE) AS obs_count,
                "DURATION MINUTES"::DOUBLE AS duration,
                "EFFORT DISTANCE KM"::DOUBLE AS distance,
                "NUMBER OBSERVERS"::DOUBLE AS observers
            FROM read_csv('{FILE}', delim='\t', header=true, nullstr='', ignore_errors=true, parallel=true)
        )
        SELECT
            -- obs_count
            AVG(obs_count), STDDEV_SAMP(obs_count),
            MEDIAN(obs_count),
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY obs_count),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY obs_count),
            COUNT(obs_count),
            -- duration
            AVG(duration), STDDEV_SAMP(duration),
            MEDIAN(duration),
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY duration),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duration),
            COUNT(duration),
            -- distance
            AVG(distance), STDDEV_SAMP(distance),
            MEDIAN(distance),
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY distance),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY distance),
            COUNT(distance),
            -- observers
            AVG(observers), STDDEV_SAMP(observers),
            MEDIAN(observers),
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY observers),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY observers),
            COUNT(observers)
        FROM raw
    """).fetchone()

    measures = [
        ("OBSERVATION COUNT", 0),
        ("DURATION MINUTES", 6),
        ("EFFORT DISTANCE KM", 12),
        ("NUMBER OBSERVERS", 18),
    ]
    for name, offset in measures:
        avg = summary[offset]
        std = summary[offset + 1]
        med = summary[offset + 2]
        p25 = summary[offset + 3]
        p75 = summary[offset + 4]
        nn = summary[offset + 5]
        cv = (std / avg * 100) if avg and avg != 0 else float('nan')
        iqr = (p75 - p25) if p25 is not None and p75 is not None else float('nan')
        avg_s = f"{avg:.2f}" if avg is not None else "N/A"
        std_s = f"{std:.2f}" if std is not None else "N/A"
        cv_s = f"{cv:.1f}" if not math.isnan(cv) else "N/A"
        med_s = f"{med:.2f}" if med is not None else "N/A"
        iqr_s = f"{iqr:.2f}" if not math.isnan(iqr) else "N/A"
        print(f"  {name:<25}  {avg_s:>12}  {std_s:>12}  {cv_s:>8}  {med_s:>10}  {iqr_s:>12}  {nn:>15,}")

    print("\n" + "=" * 80)
    print("Analysis complete.")
    con.close()


if __name__ == "__main__":
    main()
