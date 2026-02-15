"""
Configuration for experiment plots.
Define all datasets, scenarios, style settings, and default parameters here.
"""

# =============================================================================
# DATASET & SCENARIO DEFINITIONS
# =============================================================================
# Hierarchical structure: Dataset → Scenario → Results
# Paths are relative to experiments/ directory (parent of plotting/)

DATASETS = {
    'sdss_10cols': {
        'name': 'SDSS 10 Columns',
        'label': 'SDSS-10',
        'description': 'SDSS dataset with 10 measure columns',
        'scenarios': {
            'pan': {
                'dir': '../results/sdss_10cols_pan/',
                'description': 'Panning exploration pattern',
            }
        },
    },
    'sdss_100cols': {
        'name': 'SDSS 100 Columns',
        'label': 'SDSS-100',
        'description': 'SDSS dataset with 100 measure columns',
        'scenarios': {
            'pan': {
                'dir': '../results/sdss_100cols_pan/',
                'description': 'Panning exploration pattern',
            },
        },
    },
    'taxi': {
        'name': 'NYC Taxi',
        'label': 'Taxi',
        'description': 'NYC Taxi trip dataset',
        'default_measure_cols': 1,
        'scenarios': {
            'pan': {
                'dir': '../results/taxi_pan/',
                'description': 'Panning exploration pattern',
            },
            'zoom': {
                'dir': '../results/taxi_zoom/',
                'description': 'Zooming exploration pattern'
            },
        },
    },
    'synth10': {
        'name': 'Synthetic 10M',
        'label': 'Synth-10M',
        'description': 'Synthetic dataset with 10M rows',
        'scenarios': {
            'pan': {
                'dir': '../results/synth10_pan/',
                'description': 'Panning exploration pattern',
            },
        },
    },
    'synth50': {
        'name': 'Synthetic 50M',
        'label': 'Synth-50M',
        'description': 'Synthetic dataset with 50M rows',
        'scenarios': {
            'pan': {
                'dir': '../results/synth50_pan/',
                'description': 'Panning exploration pattern',
            },
        },
    },
}


def get_experiment_config(dataset: str, scenario: str) -> dict:
    """
    Get configuration for a specific dataset + scenario combination.
    
    Returns dict with: dir, label, dataset_name, scenario_name, default_measure_cols
    """
    if dataset not in DATASETS:
        raise ValueError(f"Unknown dataset: {dataset}. Available: {list(DATASETS.keys())}")
    
    ds = DATASETS[dataset]
    if scenario not in ds['scenarios']:
        raise ValueError(f"Unknown scenario '{scenario}' for dataset '{dataset}'. "
                        f"Available: {list(ds['scenarios'].keys())}")
    
    sc = ds['scenarios'][scenario]
    return {
        'dir': sc['dir'],
        'label': ds['label'],
        'dataset_name': ds['name'],
        'scenario_name': scenario,
        'description': f"{ds['description']} - {sc['description']}",
        'default_measure_cols': ds.get('default_measure_cols', DEFAULT_MEASURE_COLS),
    }

# =============================================================================
# PARAMETER SETTINGS
# =============================================================================

# Error bounds to include in analysis (0 = exact)
SELECTED_ERROR_BOUNDS = [0, 0.01, 0.02, 0.05, 0.1]

# Number of measure columns to include
SELECTED_MEASURE_COLS = [1, 2, 4, 6, 8]

# Default values when varying the other dimension
DEFAULT_MEASURE_COLS = 4
DEFAULT_ERROR_BOUND = 0.01

# =============================================================================
# PLOT STYLE SETTINGS
# =============================================================================

# Color palette for different series
COLORS = ['black', '#2ca02c', '#ff7f0e', '#0000FF', '#d62728', '#00008B']
COLORS_COMPETITORS = ['#0000FF', '#d62728', '#2ca02c']

# Font sizes
FONTSIZE = 18
FONTSIZE_SMALL = 14
FONTSIZE_TITLE = 20

# Figure sizes
FIGSIZE_SINGLE = (8, 5)
FIGSIZE_WIDE = (12, 5)
FIGSIZE_DOUBLE = (15, 5)

# Paper-ready output settings
PAPER_SETTINGS = {
    'dpi': 300,
    'format': 'pdf',
    'bbox_inches': 'tight',
    'transparent': False,
}

# Output directory for saved plots (relative to experiments/)
PLOTS_DIR = '../plots/'

# =============================================================================
# COLUMN MAPPINGS
# =============================================================================
# Map between CSV column names and display names

# Actual column names in CSV files
CSV_COLUMNS = {
    'time': 'Time (sec)',
    'io': 'I/Os',
    'query_result': 'Query Result',
    'query_result_sum': 'Query Result Sum',
    'confidence_interval': 'Confidence Interval',
    'error_bound': 'Error Bound',
    'query_index': 'i',
}

COLUMN_LABELS = {
    'Time (sec)': 'Response Time (s)',
    'I/Os': 'I/O Count',
    'Query Result Sum': 'Aggregate Result',
    'Error Bound': 'Relative Error',
    'Confidence Interval': 'Confidence Interval',
    'errorBound': 'Error Bound',
    'measure_cols': 'Measure Columns',
}

def get_label(col_name):
    """Get display label for a column name."""
    return COLUMN_LABELS.get(col_name, col_name)


# =============================================================================
# COMPETITOR DEFINITIONS
# =============================================================================
# Define baseline methods to compare against Valinor
# Each competitor has:
#   - label: Display name in plots
#   - color: Plot color
#   - subdir: Relative path from dataset/scenario directory (None = root for Valinor)
#   - has_error_bound: Whether results have error bounds in filenames
#   - file_pattern: How to find CSV files (uses {mcols} and optionally {error} placeholders)

COMPETITORS = {
    'valinor': {
        'label': 'VALINOR-A',
        'color': '#2ca02c',  # Green
        'subdir': 'valinor_a',
        'has_error_bound': True,
        'file_pattern': 'results_mcols{mcols}_error{error}_run*.csv',
    },
    'valinor_exact': {
        'label': 'VALINOR (Exact)',
        'color': '#d62728',  # Red
        'subdir': 'valinor_a',
        'has_error_bound': True,
        'file_pattern': 'results_mcols{mcols}_error0_run*.csv',
    },
    'duckdb_csv': {
        'label': 'DuckDB (CSV)',
        'color': '#ff7f0e',  # Orange
        'subdir': 'duckdb/directCSV',
        'has_error_bound': False,
        'file_pattern': 'results_mcols{mcols}_run*.csv',
    },
    'duckdb_table': {
        'label': 'DuckDB (Table)',
        'color': '#9467bd',  # Purple
        'subdir': 'duckdb/table',
        'has_error_bound': False,
        'file_pattern': 'results_mcols{mcols}_run*.csv',
    },
    'pilotdb': {
        'label': 'PilotDB',
        'color': '#e377c2',  # Pink
        'subdir': 'pilotdb',
        'has_error_bound': True,
        'file_pattern': 'results_mcols{mcols}_error{error}_run*.csv',
    },
    'valinor_s': {
        'label': 'VALINOR-S',
        'color': '#17becf',  # Cyan
        'subdir': 'valinor_s',
        'has_error_bound': True,
        'file_pattern': 'results_mcols{mcols}_error{error}_run*.csv',
    },
}

# Default competitors to compare in plots
DEFAULT_COMPETITORS = ['valinor', 'valinor_s', 'duckdb_csv', 'duckdb_table', 'pilotdb']


def get_competitor_config(competitor: str) -> dict:
    """Get configuration for a specific competitor."""
    if competitor not in COMPETITORS:
        raise ValueError(f"Unknown competitor: {competitor}. Available: {list(COMPETITORS.keys())}")
    return COMPETITORS[competitor]


def get_competitor_directory(base_dir: str, competitor: str) -> str:
    """
    Get the full directory path for a competitor's results.
    
    Args:
        base_dir: The base dataset/scenario directory (e.g., '../results/synth10_pan/')
        competitor: Competitor key (e.g., 'duckdb_csv')
    
    Returns:
        Full path to competitor results directory
    """
    comp = COMPETITORS[competitor]
    if comp['subdir'] is None:
        return base_dir
    return f"{base_dir.rstrip('/')}/{comp['subdir']}/"
