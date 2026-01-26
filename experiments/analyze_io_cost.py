#!/usr/bin/env python3
"""
Analyze average time cost per I/O operation across experiment result files.
"""

import csv
import sys
from pathlib import Path
from collections import defaultdict


def analyze_file(filepath: str) -> dict:
    """Analyze a single result file and return IO cost statistics."""
    total_ios = 0
    total_time = 0.0
    query_count = 0
    
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ios = int(row['I/Os'])
            time_sec = float(row['Time (sec)'])
            
            # Skip initialization query (i=0) and queries with 0 IOs
            if int(row['i']) == 0 or ios == 0:
                continue
            
            total_ios += ios
            total_time += time_sec
            query_count += 1
    
    if total_ios == 0:
        return None
    
    avg_time_per_io_us = (total_time / total_ios) * 1_000_000  # microseconds
    
    # Extract dataset from path (e.g., "taxi_pan", "synth50_pan")
    path_parts = Path(filepath).parts
    dataset = 'unknown'
    for part in path_parts:
        if 'taxi' in part.lower() or 'synth' in part.lower():
            dataset = part
            break
    
    return {
        'file': filepath,
        'dataset': dataset,
        'queries': query_count,
        'total_ios': total_ios,
        'total_time_sec': total_time,
        'avg_time_per_io_us': avg_time_per_io_us,
        'avg_ios_per_query': total_ios / query_count if query_count > 0 else 0,
        'avg_time_per_query_sec': total_time / query_count if query_count > 0 else 0,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_io_cost.py <file1.csv> [file2.csv] ...")
        print("       python analyze_io_cost.py results/taxi_pan/*.csv")
        sys.exit(1)
    
    files = sys.argv[1:]
    results = []
    
    for filepath in files:
        try:
            stats = analyze_file(filepath)
            if stats:
                results.append(stats)
        except Exception as e:
            print(f"Error processing {filepath}: {e}", file=sys.stderr)
    
    if not results:
        print("No valid results found.")
        sys.exit(1)
    
    # Sort by filename for consistent output
    results.sort(key=lambda x: x['file'])
    
    # Print header
    print(f"{'Dataset':<15} {'File':<40} {'Queries':>6} {'Total IOs':>12} {'Time (s)':>10} {'µs/IO':>10} {'IOs/Query':>10} {'sec/Query':>10}")
    print("-" * 120)
    
    # Group by dataset prefix for summary
    dataset_stats = defaultdict(lambda: {'total_ios': 0, 'total_time': 0.0, 'queries': 0})
    
    for r in results:
        short_name = Path(r['file']).name
        print(f"{r['dataset']:<15} {short_name:<40} {r['queries']:>6} {r['total_ios']:>12,} {r['total_time_sec']:>10.2f} {r['avg_time_per_io_us']:>10.2f} {r['avg_ios_per_query']:>10.1f} {r['avg_time_per_query_sec']:>10.3f}")
        
        # Group by dataset
        prefix = r['dataset']
        
        dataset_stats[prefix]['total_ios'] += r['total_ios']
        dataset_stats[prefix]['total_time'] += r['total_time_sec']
        dataset_stats[prefix]['queries'] += r['queries']
    
    # Print summary by dataset
    print("\n" + "=" * 80)
    print("SUMMARY BY DATASET")
    print("=" * 80)
    print(f"{'Dataset':<15} {'Total Queries':>15} {'Total IOs':>15} {'Total Time (s)':>15} {'µs/IO':>12}")
    print("-" * 80)
    
    for dataset, stats in sorted(dataset_stats.items()):
        if stats['total_ios'] > 0:
            us_per_io = (stats['total_time'] / stats['total_ios']) * 1_000_000
            print(f"{dataset:<15} {stats['queries']:>15,} {stats['total_ios']:>15,} {stats['total_time']:>15.2f} {us_per_io:>12.2f}")


if __name__ == '__main__':
    main()
