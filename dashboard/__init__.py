"""Excel Dashboard for Concurrentiecheck Westerbergen.

Usage:
    from dashboard import generate_dashboard
    path = generate_dashboard(analytics_result, config)
"""

import glob
import os
from datetime import datetime

from dashboard.excel_generator import ExcelDashboard


def generate_dashboard(analytics_result: dict, config: dict = None,
                       output_path: str = None) -> str:
    """Generate Excel dashboard from analytics results.

    Args:
        analytics_result: dict returned by run_analytics()
        config: dashboard section of settings.yaml (optional)
        output_path: custom output path (optional, overrides config)

    Returns:
        Path to generated Excel file.
    """
    config = config or {}

    if output_path is None:
        output_path = _build_output_path(
            config, analytics_result.get("metadata", {}).get("scrape_date")
        )

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    # Generate
    dashboard = ExcelDashboard(analytics_result, config)
    dashboard.generate(output_path)

    # Cleanup old files
    keep_last = config.get("keep_last", 30)
    output_dir = config.get("output_dir", "data")
    _cleanup_old_dashboards(output_dir, keep_last)

    return output_path


def _build_output_path(config: dict, scrape_date: str = None) -> str:
    """Build the output file path from config template."""
    output_dir = config.get("output_dir", "data")
    template = config.get("filename_template", "concurrentiecheck_{date}.xlsx")
    date_str = scrape_date or datetime.now().strftime("%Y-%m-%d")
    filename = template.format(date=date_str)
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, filename)


def _cleanup_old_dashboards(output_dir: str, keep_last: int = 30):
    """Remove old dashboard files, keeping the N most recent."""
    pattern = os.path.join(output_dir, "concurrentiecheck_*.xlsx")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    for old_file in files[keep_last:]:
        try:
            os.remove(old_file)
        except OSError:
            pass
