"""Run yield management analytics.

Usage:
    python run_analytics.py                     # Full report, latest data
    python run_analytics.py --date 2026-02-13   # Specific scrape date
    python run_analytics.py --json              # Output JSON instead of report
"""

import argparse
import json
import sys

import yaml

from analytics import run_analytics


def main():
    parser = argparse.ArgumentParser(
        description="Concurrentiecheck Westerbergen - Yield Analytics"
    )
    parser.add_argument("--date", "-d", help="Scrape date (YYYY-MM-DD)")
    parser.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    parser.add_argument("--excel", "-e", action="store_true", help="Generate Excel dashboard")
    parser.add_argument("--output", "-o", help="Custom output path for Excel")
    parser.add_argument("--config", default="config/settings.yaml")
    args = parser.parse_args()

    # Ensure UTF-8 output on Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # Load config for db_path
    with open(args.config, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    db_path = config.get("general", {}).get("database_path", "data/concurrentiecheck.db")

    result = run_analytics(
        db_path=db_path,
        scrape_date=args.date,
        print_to_console=not args.json,
    )

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    if args.excel:
        from dashboard import generate_dashboard
        dashboard_config = config.get("dashboard", {})
        excel_path = generate_dashboard(
            analytics_result=result,
            config=dashboard_config,
            output_path=args.output,
        )
        print(f"\nExcel dashboard gegenereerd: {excel_path}")


if __name__ == "__main__":
    main()
