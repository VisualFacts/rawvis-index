"""
Reusable plotting utilities for experiment visualization.
Import this module in notebooks or scripts to generate consistent plots.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from plot_config import (
    COLORS, FONTSIZE, FONTSIZE_SMALL, FIGSIZE_SINGLE, FIGSIZE_WIDE, FIGSIZE_DOUBLE,
    SELECTED_ERROR_BOUNDS, SELECTED_MEASURE_COLS,
    PAPER_SETTINGS, PLOTS_DIR, get_label,
    DATASETS, get_experiment_config
)


# =============================================================================
# DATA LOADING
# =============================================================================

def load_experimental_data(
    directory: str,
    selected_error_bounds: List[float] = None,
    selected_measure_cols: List[int] = None,
) -> pd.DataFrame:
    """
    Load all CSV result files from a directory.
    
    Args:
        directory: Path to results directory
        selected_error_bounds: Filter to these error bounds (None = use config default)
        selected_measure_cols: Filter to these measure cols (None = use config default)
    
    Returns:
        Combined DataFrame with all results
    """
    if selected_error_bounds is None:
        selected_error_bounds = SELECTED_ERROR_BOUNDS
    if selected_measure_cols is None:
        selected_measure_cols = SELECTED_MEASURE_COLS
    
    all_data = []
    dir_path = Path(directory)
    
    for file_path in dir_path.glob('*.csv'):
        stem = file_path.stem
        mcols = None
        error_bound = None
        run = None
        
        try:
            parts = stem.split('_')
            for part in parts:
                if part.startswith('mcols'):
                    mcols = int(part.replace('mcols', ''))
                elif part.startswith('error'):
                    error_bound = float(part.replace('error', ''))
                elif part.startswith('run'):
                    run = int(part.replace('run', ''))
        except Exception as e:
            print(f"Warning: Could not parse filename {stem}: {e}")
            continue
        
        # Apply filters
        if mcols not in selected_measure_cols:
            continue
        if error_bound not in selected_error_bounds:
            continue
        
        df = pd.read_csv(file_path)
        df['measure_cols'] = mcols
        df['errorBound'] = error_bound
        df['run'] = run
        all_data.append(df)
    
    if not all_data:
        raise ValueError(f"No matching CSV files found in {directory}")
    
    return pd.concat(all_data, ignore_index=True)


# =============================================================================
# CONFIDENCE INTERVAL PARSING HELPERS
# =============================================================================

def parse_ci_dict(ci_str: str) -> Dict[int, Tuple[float, float]]:
    """
    Parse a confidence interval string like "{0=[lb, ub], 1=[lb, ub]}" 
    into a dict mapping measure_id -> (lower_bound, upper_bound).
    
    Returns empty dict if parsing fails or string is null/empty.
    """
    if pd.isna(ci_str) or ci_str == 'null' or not ci_str:
        return {}
    
    try:
        # Remove outer braces
        ci_str = ci_str.strip('{}')
        if not ci_str:
            return {}
        
        result = {}
        # Split by "], " to get each measure's entry
        # Pattern: "0=[1.5, 2.5], 1=[3.5, 4.5]"
        import re
        pattern = r'(\d+)=\[([-\d.eE+]+),\s*([-\d.eE+]+)\]'
        matches = re.findall(pattern, ci_str)
        
        for match in matches:
            measure_id = int(match[0])
            lb = float(match[1])
            ub = float(match[2])
            result[measure_id] = (lb, ub)
        
        return result
    except Exception:
        return {}


def parse_error_dict(error_str: str) -> Dict[int, float]:
    """
    Parse an error bound string like "{0=0.01, 1=0.02}" 
    into a dict mapping measure_id -> error_value.
    
    Returns empty dict if parsing fails or string is null/empty.
    """
    if pd.isna(error_str) or error_str == 'null' or not error_str:
        return {}
    
    try:
        # Remove outer braces
        error_str = error_str.strip('{}')
        if not error_str:
            return {}
        
        result = {}
        import re
        pattern = r'(\d+)=([-\d.eE+]+)'
        matches = re.findall(pattern, error_str)
        
        for match in matches:
            measure_id = int(match[0])
            value = float(match[1])
            result[measure_id] = value
        
        return result
    except Exception:
        return {}


def get_available_measures(directory: str) -> List[int]:
    """
    Get list of measure IDs available in a dataset by parsing CI column.
    """
    df = load_experimental_data(directory)
    # Find a row with CI data
    for ci_str in df['Confidence Interval'].dropna():
        if ci_str and ci_str != 'null':
            parsed = parse_ci_dict(ci_str)
            if parsed:
                return sorted(parsed.keys())
    return []


# =============================================================================
# PLOT FORMATTING HELPERS
# =============================================================================

def format_error_bound_label(value: float) -> str:
    """Format error bound value for display."""
    if value == 0:
        return "Exact"
    return f"{value * 100:.0f}%"


def get_color_palette(values: List[Any]) -> Dict[Any, str]:
    """Create a color palette mapping values to colors."""
    return {v: COLORS[i % len(COLORS)] for i, v in enumerate(values)}


def save_figure(fig, filename: str, experiment_name: str = None):
    """Save figure to the plots directory."""
    Path(PLOTS_DIR).mkdir(exist_ok=True)
    
    if experiment_name:
        filename = f"{experiment_name}_{filename}"
    
    filepath = Path(PLOTS_DIR) / filename
    fig.savefig(filepath, **PAPER_SETTINGS)
    print(f"Saved: {filepath}")


def apply_style(ax, xlabel: str, ylabel: str, legend: bool = True):
    """Apply consistent styling to an axis."""
    ax.set_xlabel(xlabel, fontsize=FONTSIZE, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=FONTSIZE, fontweight='bold')
    ax.tick_params(axis='both', labelsize=FONTSIZE)
    ax.grid(False)
    if legend:
        ax.legend(fontsize=FONTSIZE, frameon=False)


# =============================================================================
# MAIN PLOTTING FUNCTIONS
# =============================================================================

def plot_metric_over_queries(
    directory: str,
    y_col: str,
    group_by: str,
    fixed_params: Dict[str, Any] = None,
    include_initialization: bool = False,
    query_col: str = 'i',
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot a metric over query sequence, grouped by a parameter.
    
    Args:
        directory: Path to results directory
        y_col: Column to plot on y-axis (e.g., 'Time (sec)')
        group_by: Column to group by (e.g., 'errorBound', 'measure_cols')
        fixed_params: Dict of {column: value} to filter data
        include_initialization: Include first query (usually initialization)
        query_col: Column containing query index
        title: Optional plot title
        save_as: Filename to save (without extension)
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    df = load_experimental_data(directory)
    
    # Apply fixed filters
    fixed_params = fixed_params or {}
    for col, val in fixed_params.items():
        df = df[df[col] == val]
    
    # Aggregate across runs
    agg_df = (
        df.groupby([group_by, query_col])
        .agg(mean_val=(y_col, 'mean'), std_val=(y_col, 'std'))
        .reset_index()
    )
    
    if not include_initialization:
        agg_df = agg_df[agg_df[query_col] != 0]
    
    group_values = sorted(agg_df[group_by].unique())
    color_palette = get_color_palette(group_values)
    
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    
    for v in group_values:
        data = agg_df[agg_df[group_by] == v]
        
        if group_by == 'errorBound':
            label = format_error_bound_label(v)
        else:
            label = str(v)
        
        ax.plot(
            data[query_col],
            data['mean_val'],
            label=label,
            linewidth=2,
            color=color_palette[v],
        )
    
    if not include_initialization:
        ax.set_xlim(left=1)
    
    apply_style(ax, 'Query Sequence', get_label(y_col))
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_metric_with_bars(
    directory: str,
    y_col: str,
    group_by: str,
    fixed_params: Dict[str, Any] = None,
    include_initialization: bool = False,
    query_col: str = 'i',
    group_label: str = None,
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot a metric over query sequence with a bar chart showing totals side by side.
    
    Args:
        directory: Path to results directory
        y_col: Column to plot on y-axis (e.g., 'Time (sec)')
        group_by: Column to group by (e.g., 'errorBound', 'measure_cols')
        fixed_params: Dict of {column: value} to filter data
        include_initialization: Include first query (usually initialization)
        query_col: Column containing query index
        group_label: Label for the x-axis of bar chart
        title: Optional plot title
        save_as: Filename to save (without extension)
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    df = load_experimental_data(directory)
    
    # Apply fixed filters
    fixed_params = fixed_params or {}
    for col, val in fixed_params.items():
        df = df[df[col] == val]
    
    # Aggregate across runs
    agg_df = (
        df.groupby([group_by, query_col])
        .agg(mean_val=(y_col, 'mean'), std_val=(y_col, 'std'))
        .reset_index()
    )
    
    if not include_initialization:
        agg_df = agg_df[agg_df[query_col] != 0]
    
    group_values = sorted(agg_df[group_by].unique())
    color_palette = get_color_palette(group_values)
    
    # Create figure with two subplots
    fig = plt.figure(figsize=(15, 5))
    gs = fig.add_gridspec(1, 2, width_ratios=[3, 1])
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])
    
    # Line plot on left
    for v in group_values:
        data = agg_df[agg_df[group_by] == v]
        
        if group_by == 'errorBound':
            label = format_error_bound_label(v)
        else:
            label = str(v)
        
        ax1.plot(
            data[query_col],
            data['mean_val'],
            label=label,
            linewidth=2,
            color=color_palette[v],
        )
    
    if not include_initialization:
        ax1.set_xlim(left=1)
    
    apply_style(ax1, 'Query Sequence', get_label(y_col))
    
    # Bar chart on right - sum of mean values per group
    total_df = agg_df.groupby(group_by)['mean_val'].sum().reset_index()
    total_df = total_df.sort_values(group_by)
    
    if group_by == 'errorBound':
        labels = [format_error_bound_label(v) for v in total_df[group_by]]
    else:
        labels = [str(v) for v in total_df[group_by]]
    
    bars = ax2.bar(labels, total_df['mean_val'], alpha=0.8)
    for bar, v in zip(bars, total_df[group_by]):
        bar.set_color(color_palette[v])
    
    if group_label is None:
        group_label = get_label(group_by)
    
    apply_style(ax2, group_label, f"Total {get_label(y_col)}", legend=False)
    
    if title:
        fig.suptitle(title, fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_total_metric_bars(
    directory: str,
    y_col: str,
    group_by: str,
    fixed_params: Dict[str, Any] = None,
    include_initialization: bool = False,
    query_col: str = 'i',
    group_label: str = None,
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot total (sum) of a metric as a bar chart, grouped by a parameter.
    
    Args:
        directory: Path to results directory
        y_col: Column to sum (e.g., 'Time (sec)')
        group_by: Column to group by (e.g., 'errorBound')
        fixed_params: Dict of {column: value} to filter data
        include_initialization: Include first query
        query_col: Column containing query index
        group_label: Label for x-axis
        title: Optional plot title
        save_as: Filename to save
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    df = load_experimental_data(directory)
    
    fixed_params = fixed_params or {}
    for col, val in fixed_params.items():
        df = df[df[col] == val]
    
    agg_df = (
        df.groupby([group_by, query_col])
        .agg(mean_val=(y_col, 'mean'))
        .reset_index()
    )
    
    if not include_initialization:
        agg_df = agg_df[agg_df[query_col] != 0]
    
    # Sum across all queries per group
    total_df = agg_df.groupby(group_by)['mean_val'].sum().reset_index()
    total_df = total_df.sort_values(group_by)
    
    group_values = total_df[group_by].tolist()
    color_palette = get_color_palette(group_values)
    
    if group_by == 'errorBound':
        labels = [format_error_bound_label(v) for v in group_values]
    else:
        labels = [str(v) for v in group_values]
    
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    
    bars = ax.bar(labels, total_df['mean_val'], alpha=0.8)
    for bar, v in zip(bars, group_values):
        bar.set_color(color_palette[v])
    
    if group_label is None:
        group_label = get_label(group_by)
    
    apply_style(ax, group_label, f"Total {get_label(y_col)}", legend=False)
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_confidence_intervals(
    directory: str,
    error_bounds: List[float] = None,
    include_initialization: bool = False,
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot exact results vs confidence intervals for different error bounds.
    
    Args:
        directory: Path to results directory
        error_bounds: List of error bounds to plot (None = all non-zero)
        include_initialization: Include first query
        title: Optional plot title
        save_as: Filename to save
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    df = load_experimental_data(directory)
    df = df[df['run'] == 1]  # Use first run only
    
    agg_df = df.groupby(['errorBound', 'i']).agg(
        conf_lb=('Confidence Interval LB', 'mean'),
        conf_ub=('Confidence Interval UB', 'mean'),
        val=('Query Result Sum', 'mean'),
    ).reset_index()
    
    if not include_initialization:
        agg_df = agg_df[agg_df['i'] != 0]
    
    exact_df = agg_df[agg_df['errorBound'] == 0]
    
    if error_bounds is None:
        error_bounds = sorted([eb for eb in agg_df['errorBound'].unique() if eb != 0])
    
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    
    # Plot exact values
    ax.plot(exact_df['i'], exact_df['val'], label='Exact', linewidth=2, color='black')
    
    # Plot confidence intervals for each error bound
    for i, eb in enumerate(error_bounds):
        approx_df = agg_df[agg_df['errorBound'] == eb]
        ax.fill_between(
            approx_df['i'],
            approx_df['conf_lb'],
            approx_df['conf_ub'],
            alpha=0.2,
            color=COLORS[(i + 1) % len(COLORS)],
            label=format_error_bound_label(eb),
        )
    
    if not include_initialization:
        ax.set_xlim(left=1)
    
    apply_style(ax, 'Query Sequence', 'Query Result Sum')
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_relative_error_for_measure(
    directory: str,
    measure_id: int,
    fixed_params: Dict[str, Any] = None,
    include_initialization: bool = False,
    query_col: str = 'i',
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot relative error over query sequence for a specific measure.
    
    Relative error = (CI_upper - CI_lower) / 2 / exact_value
    
    Args:
        directory: Path to results directory
        measure_id: The measure column ID to plot (e.g., 0, 1, 8, 9)
        fixed_params: Dict of {column: value} to filter data (e.g., {'measure_cols': 4})
        include_initialization: Include first query
        query_col: Column containing query index
        title: Optional plot title
        save_as: Filename to save (without extension)
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    df = load_experimental_data(directory)
    
    # Apply fixed filters
    fixed_params = fixed_params or {}
    for col, val in fixed_params.items():
        df = df[df[col] == val]
    
    # Use first run only
    df = df[df['run'] == 1]
    
    if not include_initialization:
        df = df[df[query_col] != 0]
    
    # Get exact values from errorBound=0
    exact_df = df[df['errorBound'] == 0].copy()
    
    # Parse exact query results to get ground truth per measure
    # For exact runs, we need Query Result or Query Result Sum
    # The exact value for this measure should be derived from exact execution
    
    # Get non-zero error bounds
    error_bounds = sorted([eb for eb in df['errorBound'].unique() if eb != 0])
    
    if not error_bounds:
        raise ValueError("No approximate results found (all errorBound=0)")
    
    # Build a mapping of query_index -> exact_value for this measure
    # For now, we estimate from CI midpoint of smallest error bound as proxy for exact
    # Or use the CI to compute relative error = (ub - lb) / 2 / midpoint
    
    color_palette = get_color_palette(error_bounds)
    
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    
    for eb in error_bounds:
        eb_df = df[df['errorBound'] == eb].copy()
        
        # Parse CI for this measure
        relative_errors = []
        query_indices = []
        
        for _, row in eb_df.iterrows():
            ci_dict = parse_ci_dict(row.get('Confidence Interval', ''))
            
            if measure_id in ci_dict:
                lb, ub = ci_dict[measure_id]
                midpoint = (lb + ub) / 2
                if midpoint != 0:
                    rel_error = (ub - lb) / 2 / abs(midpoint)
                else:
                    rel_error = 0
                relative_errors.append(rel_error)
                query_indices.append(row[query_col])
        
        if query_indices:
            # Sort by query index
            sorted_pairs = sorted(zip(query_indices, relative_errors))
            query_indices, relative_errors = zip(*sorted_pairs)
            
            label = format_error_bound_label(eb)
            ax.plot(
                query_indices,
                relative_errors,
                label=label,
                linewidth=2,
                color=color_palette[eb],
            )
    
    if not include_initialization:
        ax.set_xlim(left=1)
    
    apply_style(ax, 'Query Sequence', 'Relative Error')
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    else:
        ax.set_title(f'Relative Error for Measure {measure_id}', fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_ci_vs_exact_for_measure(
    directory: str,
    measure_id: int,
    error_bounds: List[float] = None,
    fixed_params: Dict[str, Any] = None,
    include_initialization: bool = False,
    query_col: str = 'i',
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot exact results vs confidence intervals for a specific measure.
    
    Shows the exact value line with CI bands overlaid for each error bound.
    
    Args:
        directory: Path to results directory
        measure_id: The measure column ID to plot
        error_bounds: List of error bounds to plot (None = all non-zero)
        fixed_params: Dict of {column: value} to filter data
        include_initialization: Include first query
        query_col: Column containing query index
        title: Optional plot title
        save_as: Filename to save
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    df = load_experimental_data(directory)
    
    # Apply fixed filters
    fixed_params = fixed_params or {}
    for col, val in fixed_params.items():
        df = df[df[col] == val]
    
    # Use first run only
    df = df[df['run'] == 1]
    
    if not include_initialization:
        df = df[df[query_col] != 0]
    
    # Get exact values (errorBound=0) - use CI midpoint as estimate
    exact_df = df[df['errorBound'] == 0].copy()
    
    # For exact runs, CI might be null, so we need to get value differently
    # Try to parse CI, or use the midpoint from the smallest error bound
    
    # Get available error bounds
    all_error_bounds = sorted([eb for eb in df['errorBound'].unique() if eb != 0])
    
    if error_bounds is None:
        error_bounds = all_error_bounds
    
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    
    # Get exact values from smallest error bound CI midpoint as approximation
    # (or could be retrieved from a different column if available)
    smallest_eb = min(all_error_bounds) if all_error_bounds else None
    
    if smallest_eb:
        exact_proxy_df = df[df['errorBound'] == smallest_eb]
        exact_vals = []
        exact_queries = []
        
        for _, row in exact_proxy_df.iterrows():
            ci_dict = parse_ci_dict(row.get('Confidence Interval', ''))
            if measure_id in ci_dict:
                lb, ub = ci_dict[measure_id]
                exact_vals.append((lb + ub) / 2)
                exact_queries.append(row[query_col])
        
        if exact_queries:
            sorted_pairs = sorted(zip(exact_queries, exact_vals))
            exact_queries, exact_vals = zip(*sorted_pairs)
            ax.plot(exact_queries, exact_vals, label='Estimate', linewidth=2, color='black')
    
    # Plot CI bands for each error bound
    for i, eb in enumerate(error_bounds):
        eb_df = df[df['errorBound'] == eb]
        
        lbs, ubs, queries = [], [], []
        for _, row in eb_df.iterrows():
            ci_dict = parse_ci_dict(row.get('Confidence Interval', ''))
            if measure_id in ci_dict:
                lb, ub = ci_dict[measure_id]
                lbs.append(lb)
                ubs.append(ub)
                queries.append(row[query_col])
        
        if queries:
            sorted_data = sorted(zip(queries, lbs, ubs))
            queries, lbs, ubs = zip(*sorted_data)
            
            ax.fill_between(
                queries, lbs, ubs,
                alpha=0.2,
                color=COLORS[(i + 1) % len(COLORS)],
                label=format_error_bound_label(eb),
            )
    
    if not include_initialization:
        ax.set_xlim(left=1)
    
    apply_style(ax, 'Query Sequence', f'Measure {measure_id} Value')
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def compute_accuracy_summary(
    directory: str,
    include_initialization: bool = False,
) -> pd.DataFrame:
    """
    Compute summary statistics for each error bound.
    
    Args:
        directory: Path to results directory
        include_initialization: Include first query
    
    Returns:
        DataFrame with summary statistics per error bound
    """
    df = load_experimental_data(directory)
    
    if not include_initialization:
        df = df[df['i'] != 0]
    
    # Aggregate by error bound across all runs
    summary = df.groupby('errorBound').agg(
        total_time=('Time (sec)', 'sum'),
        avg_time=('Time (sec)', 'mean'),
        total_io=('I/Os', 'sum'),
        avg_io=('I/Os', 'mean'),
        num_queries=('i', 'count'),
    ).reset_index()
    
    # Compute speedup relative to exact (errorBound=0)
    exact_time = summary[summary['errorBound'] == 0]['total_time'].values
    if len(exact_time) > 0:
        summary['speedup'] = exact_time[0] / summary['total_time']
    else:
        summary['speedup'] = 1.0
    
    # Format error bound for display
    summary['error_bound'] = summary['errorBound'].apply(
        lambda x: 'Exact' if x == 0 else f'{x*100:.0f}%'
    )
    
    return summary[['error_bound', 'total_time', 'avg_time', 'total_io', 'avg_io', 'speedup']].round(4)


# =============================================================================
# COMPETITOR COMPARISON FUNCTIONS
# =============================================================================

def load_competitor_data(
    base_dir: str,
    competitor: str,
    measure_cols: int = 4,
    error_bound: float = None,
) -> pd.DataFrame:
    """
    Load experimental data for a specific competitor.
    
    Args:
        base_dir: Base directory for the dataset/scenario
        competitor: Competitor key from COMPETITORS config
        measure_cols: Number of measure columns
        error_bound: Error bound (only for methods with has_error_bound=True)
    
    Returns:
        DataFrame with competitor results
    """
    from plot_config import COMPETITORS, get_competitor_directory
    import glob
    
    comp = COMPETITORS[competitor]
    comp_dir = get_competitor_directory(base_dir, competitor)
    
    # Build file pattern
    pattern = comp['file_pattern']
    pattern = pattern.replace('{mcols}', str(measure_cols))
    
    if comp['has_error_bound'] and error_bound is not None:
        pattern = pattern.replace('{error}', str(error_bound))
    elif '{error}' in pattern:
        # For exact runs or methods without error bounds
        pattern = pattern.replace('{error}', '0')
    
    full_pattern = f"{comp_dir.rstrip('/')}/{pattern}"
    files = glob.glob(full_pattern)
    
    if not files:
        raise FileNotFoundError(f"No files found for {competitor} with pattern: {full_pattern}")
    
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Extract run number from filename
            import re
            run_match = re.search(r'run(\d+)', f)
            df['run'] = int(run_match.group(1)) if run_match else 1
            df['competitor'] = competitor
            df['competitor_label'] = comp['label']
            
            # Normalize column names if needed
            if 'measure_cols' not in df.columns:
                df['measure_cols'] = measure_cols
            if 'errorBound' not in df.columns:
                df['errorBound'] = error_bound if error_bound else 0
                
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not load {f}: {e}")
            continue
    
    if not dfs:
        raise ValueError(f"No valid data loaded for {competitor}")
    
    return pd.concat(dfs, ignore_index=True)


def load_all_competitors_data(
    base_dir: str,
    competitors: List[str],
    measure_cols: int = 4,
    error_bound: float = 0.01,
) -> pd.DataFrame:
    """
    Load data for multiple competitors.
    
    Args:
        base_dir: Base directory for the dataset/scenario
        competitors: List of competitor keys to load
        measure_cols: Number of measure columns
        error_bound: Error bound for approximate methods
    
    Returns:
        Combined DataFrame with all competitors
    """
    all_data = []
    
    for comp in competitors:
        try:
            df = load_competitor_data(base_dir, comp, measure_cols, error_bound)
            all_data.append(df)
        except (FileNotFoundError, ValueError) as e:
            print(f"Warning: Skipping {comp}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No competitor data could be loaded")
    
    return pd.concat(all_data, ignore_index=True)


def plot_competitors_comparison(
    base_dir: str,
    competitors: List[str] = None,
    y_col: str = 'Time (sec)',
    measure_cols: int = 4,
    error_bound: float = 0.01,
    include_initialization: bool = False,
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot line chart comparing multiple competitors over query sequence.
    
    Args:
        base_dir: Base directory for the dataset/scenario
        competitors: List of competitor keys (None = use DEFAULT_COMPETITORS)
        y_col: Column to plot (e.g., 'Time (sec)', 'I/Os')
        measure_cols: Number of measure columns
        error_bound: Error bound for approximate methods
        include_initialization: Include first query
        title: Plot title
        save_as: Filename to save
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    from plot_config import COMPETITORS, DEFAULT_COMPETITORS
    
    if competitors is None:
        competitors = DEFAULT_COMPETITORS
    
    # Load data for all competitors
    df = load_all_competitors_data(base_dir, competitors, measure_cols, error_bound)
    
    if not include_initialization:
        df = df[df['i'] != 0]
    
    # Use first run only and aggregate by query
    df = df[df['run'] == 1]
    
    # Build color palette
    color_palette = {COMPETITORS[c]['label']: COMPETITORS[c]['color'] for c in competitors}
    
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    
    for comp_label in df['competitor_label'].unique():
        comp_df = df[df['competitor_label'] == comp_label].sort_values('i')
        ax.plot(
            comp_df['i'],
            comp_df[y_col],
            label=comp_label,
            linewidth=2,
            color=color_palette.get(comp_label, 'black'),
        )
    
    apply_style(ax, 'Query Sequence', y_col)
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    
    if not include_initialization:
        ax.set_xlim(left=1)
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_competitors_with_bars(
    base_dir: str,
    competitors: List[str] = None,
    y_col: str = 'Time (sec)',
    measure_cols: int = 4,
    error_bound: float = 0.01,
    include_initialization: bool = False,
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot competitors comparison with line chart + bar chart showing totals.
    
    Args:
        base_dir: Base directory for the dataset/scenario
        competitors: List of competitor keys (None = use DEFAULT_COMPETITORS)
        y_col: Column to plot
        measure_cols: Number of measure columns
        error_bound: Error bound for approximate methods
        include_initialization: Include first query
        title: Plot title
        save_as: Filename to save
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    from plot_config import COMPETITORS, DEFAULT_COMPETITORS
    
    if competitors is None:
        competitors = DEFAULT_COMPETITORS
    
    # Load data for all competitors
    df = load_all_competitors_data(base_dir, competitors, measure_cols, error_bound)
    
    if not include_initialization:
        df = df[df['i'] != 0]
    
    # Use first run only
    df = df[df['run'] == 1]
    
    # Build color palette
    color_palette = {COMPETITORS[c]['label']: COMPETITORS[c]['color'] for c in competitors}
    
    # Create figure with gridspec
    fig = plt.figure(figsize=FIGSIZE_DOUBLE)
    gs = fig.add_gridspec(1, 2, width_ratios=[3, 1])
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])
    
    # Line plot on left
    for comp_label in df['competitor_label'].unique():
        comp_df = df[df['competitor_label'] == comp_label].sort_values('i')
        ax1.plot(
            comp_df['i'],
            comp_df[y_col],
            label=comp_label,
            linewidth=2,
            color=color_palette.get(comp_label, 'black'),
        )
    
    apply_style(ax1, 'Query Sequence', y_col)
    
    if not include_initialization:
        ax1.set_xlim(left=1)
    
    # Bar chart on right
    totals = df.groupby('competitor_label')[y_col].sum().sort_values()
    bars = ax2.bar(totals.index, totals.values, alpha=0.8)
    
    for bar, label in zip(bars, totals.index):
        bar.set_color(color_palette.get(label, 'gray'))
    
    ax2.set_xlabel('Method', fontsize=FONTSIZE, fontweight='bold')
    ax2.set_ylabel(f'Total {y_col}', fontsize=FONTSIZE, fontweight='bold')
    ax2.tick_params(axis='both', labelsize=FONTSIZE_SMALL)
    plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
    
    if title:
        fig.suptitle(title, fontsize=FONTSIZE, fontweight='bold', y=1.02)
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


def plot_competitors_bars_only(
    base_dir: str,
    competitors: List[str] = None,
    y_col: str = 'Time (sec)',
    measure_cols: int = 4,
    error_bound: float = 0.01,
    include_initialization: bool = False,
    title: str = None,
    save_as: str = None,
    experiment_name: str = None,
) -> plt.Figure:
    """
    Plot bar chart comparing total metric across competitors.
    
    Args:
        base_dir: Base directory for the dataset/scenario
        competitors: List of competitor keys
        y_col: Column to plot
        measure_cols: Number of measure columns
        error_bound: Error bound for approximate methods
        include_initialization: Include first query
        title: Plot title
        save_as: Filename to save
        experiment_name: Experiment name for filename prefix
    
    Returns:
        matplotlib Figure
    """
    from plot_config import COMPETITORS, DEFAULT_COMPETITORS
    
    if competitors is None:
        competitors = DEFAULT_COMPETITORS
    
    # Load data for all competitors
    df = load_all_competitors_data(base_dir, competitors, measure_cols, error_bound)
    
    if not include_initialization:
        df = df[df['i'] != 0]
    
    # Use first run only
    df = df[df['run'] == 1]
    
    # Build color palette
    color_palette = {COMPETITORS[c]['label']: COMPETITORS[c]['color'] for c in competitors}
    
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    
    # Bar chart
    totals = df.groupby('competitor_label')[y_col].sum().sort_values()
    bars = ax.bar(totals.index, totals.values, alpha=0.8)
    
    for bar, label in zip(bars, totals.index):
        bar.set_color(color_palette.get(label, 'gray'))
    
    ax.set_xlabel('Method', fontsize=FONTSIZE, fontweight='bold')
    ax.set_ylabel(f'Total {y_col}', fontsize=FONTSIZE, fontweight='bold')
    ax.tick_params(axis='both', labelsize=FONTSIZE)
    
    if title:
        ax.set_title(title, fontsize=FONTSIZE, fontweight='bold')
    
    plt.tight_layout()
    
    if save_as:
        save_figure(fig, f"{save_as}.pdf", experiment_name)
    
    return fig


# =============================================================================
# BATCH GENERATION
# =============================================================================

def generate_all_plots(
    datasets: Dict[str, Dict] = None,
    scenarios: List[str] = None,
    default_measure_cols: int = 4,
    default_error_bound: float = 0.01,
):
    """
    Generate all standard plots for datasets and scenarios.
    
    Args:
        datasets: Dict of dataset configs (from plot_config.DATASETS), or None for all
        scenarios: List of scenario names to process, or None for all available
        default_measure_cols: Default measure columns for error bound plots
        default_error_bound: Default error bound for measure columns plots
    """
    if datasets is None:
        datasets = DATASETS
    
    for ds_name, ds_config in datasets.items():
        label = ds_config['label']
        
        for sc_name, sc_config in ds_config['scenarios'].items():
            # Skip if scenario filter provided and this scenario not in it
            if scenarios is not None and sc_name not in scenarios:
                continue
            
            directory = sc_config['dir']
            exp_name = f"{ds_name}_{sc_name}"
        
            print(f"\n{'='*50}")
            print(f"Generating plots for: {label} / {sc_name}")
            print(f"{'='*50}")
            
            try:
                # Time vs Error Bound
                plot_total_metric_bars(
                    directory, 'Time (sec)', 'errorBound',
                    fixed_params={'measure_cols': default_measure_cols},
                    save_as='time_vs_error', experiment_name=exp_name
                )
                plt.close()
                
                # Time vs Measure Columns  
                plot_total_metric_bars(
                    directory, 'Time (sec)', 'measure_cols',
                    fixed_params={'errorBound': default_error_bound},
                    save_as='time_vs_measures', experiment_name=exp_name
                )
                plt.close()
                
                # Time over queries
                plot_metric_over_queries(
                    directory, 'Time (sec)', 'errorBound',
                    fixed_params={'measure_cols': default_measure_cols},
                    save_as='time_queries', experiment_name=exp_name
                )
                plt.close()
                
                # Confidence intervals
                plot_confidence_intervals(
                    directory,
                    save_as='confidence_intervals', experiment_name=exp_name
                )
                plt.close()
                
                # Accuracy summary
                summary = compute_accuracy_summary(directory)
                print(f"\nAccuracy Summary for {label} / {sc_name}:")
                print(summary.to_string(index=False))
                
            except Exception as e:
                print(f"Error processing {exp_name}: {e}")
                continue
    
    print(f"\n{'='*50}")
    print(f"All plots saved to: {PLOTS_DIR}")
    print(f"{'='*50}")
