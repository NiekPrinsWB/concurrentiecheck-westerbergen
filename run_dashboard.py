"""Generate Excel dashboard from latest analytics data.

Usage:
    python run_dashboard.py                         # Latest data
    python run_dashboard.py --date 2026-02-13       # Specific scrape date
    python run_dashboard.py --output my_report.xlsx # Custom output path
"""

import argparse
import sys

import yaml

from analytics import run_analytics
from dashboard import generate_dashboard


def main():
    parser = argparse.ArgumentParser(
        description="Concurrentiecheck Westerbergen - Excel Dashboard"
    )
    parser.add_argument("--date", "-d", help="Scrape date (YYYY-MM-DD)")
    parser.add_argument("--output", "-o", help="Custom output file path")
    parser.add_argument("--config", default="config/settings.yaml")
    args = parser.parse_args()

    # Ensure UTF-8 output on Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    db_path = config.get("general", {}).get("database_path", "data/concurrentiecheck.db")

    print("Analytics berekenen...")
    result = run_analytics(
        db_path=db_path,
        scrape_date=args.date,
        print_to_console=False,
    )

    if result["metadata"]["comparison_count"] == 0:
        print("Geen data beschikbaar voor dashboard.")
        sys.exit(1)

    print("Excel dashboard genereren...")
    path = generate_dashboard(
        analytics_result=result,
        config=config.get("dashboard", {}),
        output_path=args.output,
    )

    print(f"Dashboard gegenereerd: {path}")
    print(f"  - {result['metadata']['comparison_count']} vergelijkingen")
    print(f"  - {len(result.get('recommendations', []))} prijsadviezen")
    print(f"  - 4 werkbladen: Overzicht, Prijsvergelijking, Concurrenten, Historisch")


if __name__ == "__main__":
    main()
