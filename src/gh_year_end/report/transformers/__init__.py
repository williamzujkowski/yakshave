"""Data transformation functions for report generation.

This package provides modular transformation functions for converting raw metrics
into report-ready data structures for visualization and analysis.

Modules:
    leaderboards: Awards and leaderboard data transformation
    timeseries: Time series and activity timeline transformation
    highlights: Highlights and fun facts calculation
    charts: Chart data generation for D3.js visualization
    analysis: Insights and risk analysis
"""

from .analysis import calculate_insights, calculate_risks
from .charts import generate_chart_data, generate_engineer_charts
from .highlights import calculate_fun_facts, calculate_highlights
from .leaderboards import transform_awards_data, transform_leaderboards
from .timeseries import transform_activity_timeline

__all__ = [
    "calculate_fun_facts",
    "calculate_highlights",
    "calculate_insights",
    "calculate_risks",
    "generate_chart_data",
    "generate_engineer_charts",
    "transform_activity_timeline",
    "transform_awards_data",
    "transform_leaderboards",
]
