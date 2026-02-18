"""Yield Management Analytics for Concurrentiecheck Westerbergen.

Usage:
    from analytics import run_analytics
    result = run_analytics(db_path="data/concurrentiecheck.db")
"""

from datetime import datetime

from analytics.data_prep import load_comparison_data
from analytics.kpi_engine import (
    compute_price_index,
    compute_price_per_night,
    compute_competitive_position,
    compute_availability_gaps,
    compute_seasonal_patterns,
    compute_price_changes,
    compute_recommendations,
)
from analytics.report import print_report


def run_analytics(db_path: str = "data/concurrentiecheck.db",
                  scrape_date: str = None,
                  print_to_console: bool = True) -> dict:
    """Run all analytics and return structured results.

    Returns dict with keys matching future Excel worksheet tabs.
    """
    from database import Database

    db = Database(db_path)

    # Step 1: Load and normalize data
    comparison_data = load_comparison_data(db, scrape_date)

    if not comparison_data:
        print("Geen vergelijkbare data gevonden.")
        return {
            "metadata": {"comparison_count": 0, "competitors": []},
            "comparison_data": [],
            "price_index": [],
            "price_per_night": [],
            "competitive_position": [],
            "availability_gaps": {},
            "seasonal_patterns": {},
            "price_changes": {"status": "onvoldoende_data", "changes": []},
            "recommendations": [],
        }

    # Determine actual scrape_date used
    actual_date = scrape_date or db.get_latest_scrape_date()

    # Step 2: Compute all KPIs
    price_index = compute_price_index(comparison_data)
    ppn = compute_price_per_night(comparison_data)
    position = compute_competitive_position(comparison_data)
    gaps = compute_availability_gaps(comparison_data)
    seasonal = compute_seasonal_patterns(comparison_data)
    changes = compute_price_changes(db)
    recommendations = compute_recommendations(price_index, position, gaps, seasonal)

    result = {
        "metadata": {
            "scrape_date": actual_date,
            "run_timestamp": datetime.now().isoformat(),
            "comparison_count": len(comparison_data),
            "competitors": sorted(set(
                comp for row in comparison_data
                for comp in row["competitors"].keys()
            )),
        },
        "price_index": price_index,
        "price_per_night": ppn,
        "competitive_position": position,
        "availability_gaps": gaps,
        "seasonal_patterns": seasonal,
        "price_changes": changes,
        "recommendations": recommendations,
        "comparison_data": comparison_data,
    }

    if print_to_console:
        print_report(result)

    return result
