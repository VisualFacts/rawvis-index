#!/usr/bin/env bash
# Filter US eBird data to keep only continental US (CONUS) rows from 2023-2026.
# Excludes Alaska, Hawaii, and overseas territories via lat/lon bounds.
#
# Columns (1-indexed, tab-delimited):
#   29 = LATITUDE
#   30 = LONGITUDE
#   31 = OBSERVATION DATE (YYYY-MM-DD)
#
# CONUS bounds (generous):
#   Latitude:  24.0 – 50.0   (southern tip of FL Keys to Canadian border)
#   Longitude: -130.0 – -60.0 (Pacific coast to Atlantic coast + margin)
#
# Usage: bash experiments/filter_ebird_us.sh
#
# Input:  /media/wd-hdd/nbikakis/ebirds/ebd_US_relFeb-2026.txt  (~460 GB, 1.25B rows)
# Output: /data/smaroulis/ebd_US_2023_2026.txt                   (~155 GB, ~420M rows)

set -euo pipefail

INPUT="/media/wd-hdd/nbikakis/ebirds/ebd_US_relFeb-2026.txt"
OUTPUT="/data/smaroulis/ebd_US_2023_2026.txt"

if [[ ! -f "$INPUT" ]]; then
    echo "ERROR: Input file not found: $INPUT"
    exit 1
fi

echo "Filtering US eBird: $INPUT"
echo "Output: $OUTPUT"
echo "Keeping: years 2023-2026, CONUS only (lat 24-50, lon -130 to -60)"
echo "Started: $(date)"

# Header line
head -1 "$INPUT" > "$OUTPUT"

# Filter: year 2023-2026 AND CONUS bounding box.
# awk columns (1-indexed): $29=LATITUDE, $30=LONGITUDE, $31=OBSERVATION DATE
tail -n +2 "$INPUT" | awk -F'\t' '
    $31 ~ /^202[3-6]/ && $29+0 >= 24 && $29+0 <= 50 && $30+0 >= -130 && $30+0 <= -60
' >> "$OUTPUT"

ROWS=$(wc -l < "$OUTPUT")
SIZE=$(du -h "$OUTPUT" | cut -f1)
echo "Done: $(date)"
echo "Output rows: $ROWS (including header)"
echo "Output size: $SIZE"
