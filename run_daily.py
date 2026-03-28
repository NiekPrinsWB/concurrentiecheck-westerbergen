"""Dagelijkse pipeline: scrape + analytics + Excel dashboard.

Draait alle enabled scrapers, berekent analytics en genereert een
Excel dashboard in één geautomatiseerde run. Ontworpen voor
Windows Task Scheduler.

Exit codes:
    0 = succes (alle scrapers OK)
    1 = partial (sommige scrapers gefaald, maar data beschikbaar)
    2 = fatal (geen data of kritieke fout)

Usage:
    python run_daily.py                  # Volledige run
    python run_daily.py --dry-run        # Test zonder scraping
    python run_daily.py --skip-scrape    # Alleen analytics + dashboard
    python run_daily.py --config alt.yaml
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import yaml

from database import Database
from run_scraper import get_scraper_map, setup_logging, load_config


# Domain groups: scrapers hitting the same domain run sequentially,
# different groups run in parallel to avoid rate-limiting conflicts.
DOMAIN_GROUPS = {
    "api_centerparcs": ["centerparcs_sandur"],
    "api_molecaten": ["molecaten_bosven", "molecaten_camping"],
    "api_ommerland": ["camping_ommerland", "ommerland_camping", "ommerland_psanitair"],
    "api_maurik": ["eiland_van_maurik", "maurik_camping"],
    "be_beerzebulten": ["beerze_bulten", "bb_camping", "bb_psanitair"],
    "be_boshoek": ["de_boshoek"],
    "be_witteberg": ["de_witte_berg"],
    "be_zandstuve": ["zandstuve_boslodge", "zandstuve_camping", "zandstuve_psanitair"],
    "tomm_witterzomer": ["witter_zomer"],
    "capfun": ["stoetenslagh_camping", "stoetenslagh_acc", "sprookjes_camping",
               "fruithof_camping", "fruithof_acc"],
    "api_landal": ["landal_aelderholt", "landal_aelderholt_premium", "landal_bartje"],
    "api_kleinewolf": ["kleinewolf_camping", "kleinewolf_acc"],
    "rcn": ["rcn_mercurius", "rcn_luna", "rcn_camping"],
    "westerbergen": ["westerbergen", "westerbergen_camping", "westerbergen_psanitair"],
}


EXIT_SUCCESS = 0
EXIT_PARTIAL = 1
EXIT_FATAL = 2


def run_pipeline(config: dict, dry_run: bool = False,
                 skip_scrape: bool = False) -> int:
    """Run the full daily pipeline.

    Returns exit code (0=success, 1=partial, 2=fatal).
    """
    logger = logging.getLogger("daily")
    start_time = time.time()
    today = datetime.now().strftime("%Y-%m-%d")

    # --- Database ---
    db_path = config.get("general", {}).get("database_path", "data/concurrentiecheck.db")
    db = Database(db_path)

    # --- Phase 1: Scraping ---
    scrape_results = {}
    failed_scrapers = []

    if skip_scrape:
        logger.info("Scraping overgeslagen (--skip-scrape)")
    elif dry_run:
        logger.info("Dry-run modus: scraping wordt gesimuleerd")
        scrapers = get_scraper_map(db, headless=True)
        enabled = config.get("competitors", {})
        for key, scraper in scrapers.items():
            if key == "westerbergen" or enabled.get(key, {}).get("enabled", True):
                scrape_results[key] = {
                    "status": "dry-run",
                    "records": 0,
                    "duration": 0,
                }
                logger.info(f"  [DRY-RUN] {scraper.competitor_name}: overgeslagen")
    else:
        headless = config.get("scraping", {}).get("headless", True)
        persons = config.get("scraping", {}).get("default_persons", 4)
        scrapers = get_scraper_map(db, headless=headless)
        enabled = config.get("competitors", {})
        automation = config.get("automation", {})
        retry_failed = automation.get("retry_failed", True)
        max_retries = automation.get("max_retry_attempts", 1)

        # Determine which scrapers to run (enabled + westerbergen)
        to_run = {}
        for key, scraper in scrapers.items():
            if key == "westerbergen":
                to_run[key] = scraper
            elif enabled.get(key, {}).get("enabled", True):
                to_run[key] = scraper

        logger.info(f"Scrapers te draaien: {', '.join(to_run.keys())}")

        # Scrape 12 months ahead
        days_ahead = 365
        months_ahead = 12
        target_end_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        max_pages = days_ahead // 3 + 10  # ~130 pages for BookingExperts

        def _run_scraper(key, scraper):
            """Run a single scraper and return result dict."""
            scraper_logger = logging.getLogger("daily")
            scraper_logger.info(f"\n--- Scraping: {scraper.competitor_name} ({key}) ---")
            t0 = time.time()
            try:
                results = scraper.run_efficient(
                    max_pages=max_pages,
                    months_ahead=months_ahead,
                    target_end_date=target_end_date,
                    persons=persons,
                )
                available = [r for r in results if r.get("available") and r.get("price")]
                dur = time.time() - t0
                result = {
                    "status": "success",
                    "records": len(results),
                    "available": len(available),
                    "duration": dur,
                }
                scraper_logger.info(
                    f"  OK: {key} - {len(results)} records "
                    f"({len(available)} beschikbaar), {dur:.1f}s"
                )
                return key, result
            except Exception as e:
                dur = time.time() - t0
                result = {
                    "status": "failed",
                    "records": 0,
                    "available": 0,
                    "duration": dur,
                    "error": str(e),
                }
                scraper_logger.error(f"  MISLUKT: {key} - {e}", exc_info=True)
                return key, result

        def _run_domain_group(group_name, scraper_keys):
            """Run scrapers in a domain group sequentially, return results."""
            results = []
            for key in scraper_keys:
                if key in to_run:
                    results.append(_run_scraper(key, to_run[key]))
            return results

        # Build domain groups from enabled scrapers
        active_groups = {}
        assigned_keys = set()
        for group_name, keys in DOMAIN_GROUPS.items():
            active_keys = [k for k in keys if k in to_run]
            if active_keys:
                active_groups[group_name] = active_keys
                assigned_keys.update(active_keys)

        # Any scrapers not in a domain group run as their own group
        for key in to_run:
            if key not in assigned_keys:
                active_groups[f"_ungrouped_{key}"] = [key]

        logger.info(
            f"Parallel uitvoering: {len(active_groups)} domeingroepen, "
            f"{len(to_run)} scrapers totaal"
        )
        for gname, gkeys in active_groups.items():
            logger.info(f"  {gname}: {', '.join(gkeys)}")

        # Run domain groups in parallel (max 6 concurrent to limit resources)
        max_workers = min(6, len(active_groups))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_run_domain_group, gname, gkeys): gname
                for gname, gkeys in active_groups.items()
            }
            for future in as_completed(futures):
                group_name = futures[future]
                try:
                    group_results = future.result()
                    for key, result in group_results:
                        scrape_results[key] = result
                        if result["status"] == "failed":
                            failed_scrapers.append(key)
                except Exception as e:
                    logger.error(f"  Groep {group_name} crash: {e}", exc_info=True)
                    # Mark all scrapers in this group as failed
                    for key in active_groups[group_name]:
                        if key in to_run:
                            scrape_results[key] = {
                                "status": "failed",
                                "records": 0,
                                "available": 0,
                                "duration": 0,
                                "error": str(e),
                            }
                            failed_scrapers.append(key)

        # Retry failed scrapers (sequentially)
        if failed_scrapers and retry_failed:
            for attempt in range(1, max_retries + 1):
                retry_list = list(failed_scrapers)
                failed_scrapers.clear()
                logger.info(f"\n--- Retry poging {attempt} voor {len(retry_list)} scrapers ---")

                for key in retry_list:
                    scraper = to_run[key]
                    logger.info(f"  Retry: {scraper.competitor_name}")
                    rkey, result = _run_scraper(key, scraper)
                    scrape_results[rkey] = result
                    if result["status"] == "failed":
                        failed_scrapers.append(key)

    # --- Phase 2 & 3: Analytics + Excel Dashboard per segment ---
    analytics_result = None  # Will hold the 'accommodatie' result for backwards compat
    analytics_results = {}   # All segments
    excel_paths = []

    generate_analytics = config.get("automation", {}).get("generate_analytics", True)
    should_generate_dashboard = config.get("automation", {}).get("generate_dashboard", True)

    SEGMENT_LABELS = {
        "accommodatie": "Accommodaties",
        "kampeerplaats": "Kampeerplaatsen",
        "prive_sanitair": "Privé sanitair",
    }

    if generate_analytics or should_generate_dashboard:
        from analytics import run_analytics
        from dashboard import generate_dashboard

        # Determine which segments have data
        segments = db.get_available_segments(today if not skip_scrape else None)
        if not segments:
            segments = ["accommodatie"]

        for segment in segments:
            label = SEGMENT_LABELS.get(segment, segment)
            logger.info(f"\n--- Analytics: {label} ---")
            try:
                seg_result = run_analytics(
                    db_path=db_path,
                    scrape_date=today if not skip_scrape else None,
                    segment=segment,
                    print_to_console=True,
                )
                analytics_results[segment] = seg_result
                comparison_count = seg_result.get("metadata", {}).get("comparison_count", 0)
                rec_count = len(seg_result.get("recommendations", []))
                logger.info(f"  {comparison_count} vergelijkingen, {rec_count} prijsadviezen")

                # Generate Excel dashboard for this segment
                if should_generate_dashboard and comparison_count > 0:
                    try:
                        dashboard_config = config.get("dashboard", {})
                        excel_path = generate_dashboard(
                            analytics_result=seg_result,
                            config=dashboard_config,
                        )
                        excel_paths.append(excel_path)
                        logger.info(f"  Dashboard: {excel_path}")
                    except Exception as e:
                        logger.error(f"  Dashboard generatie mislukt: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"  Analytics mislukt voor {label}: {e}", exc_info=True)

        # Keep accommodatie result as primary for backwards compat (email etc)
        analytics_result = analytics_results.get("accommodatie")
    excel_path = excel_paths[0] if excel_paths else None

    # --- Phase 4: Git auto-push (voor Streamlit Cloud) ---
    total_records = sum(r.get("records", 0) for r in scrape_results.values())
    git_auto_push = config.get("automation", {}).get("git_auto_push", False)
    if git_auto_push and not dry_run and total_records > 0:
        logger.info("\n--- Database pushen naar GitHub ---")
        try:
            project_dir = os.path.dirname(os.path.abspath(__file__))
            git_run = lambda cmd: subprocess.run(
                cmd, cwd=project_dir, capture_output=True, text=True, timeout=60,
            )
            # Stage only the database file
            result = git_run(["git", "add", db_path])
            if result.returncode != 0:
                raise RuntimeError(f"git add failed: {result.stderr}")

            # Check if there are staged changes
            result = git_run(["git", "diff", "--cached", "--quiet"])
            if result.returncode == 0:
                logger.info("  Geen wijzigingen in database, push overgeslagen")
            else:
                msg = f"Auto-update prijsdata {today}"
                result = git_run(["git", "commit", "-m", msg])
                if result.returncode != 0:
                    raise RuntimeError(f"git commit failed: {result.stderr}")

                result = git_run(["git", "push"])
                if result.returncode != 0:
                    raise RuntimeError(f"git push failed: {result.stderr}")

                logger.info("  Database gepusht naar GitHub")
        except Exception as e:
            logger.error(f"  Git push mislukt: {e}")

    # --- Samenvatting ---
    total_duration = time.time() - start_time
    total_records = sum(r.get("records", 0) for r in scrape_results.values())
    total_available = sum(r.get("available", 0) for r in scrape_results.values())
    success_count = sum(1 for r in scrape_results.values() if "success" in r.get("status", ""))
    fail_count = sum(1 for r in scrape_results.values() if r.get("status") == "failed")

    logger.info("\n" + "=" * 60)
    logger.info("DAGELIJKSE RUN VOLTOOID")
    logger.info(f"  Datum:           {today}")
    logger.info(f"  Totale duur:     {total_duration:.0f}s ({total_duration/60:.1f} min)")

    if not skip_scrape and not dry_run:
        logger.info(f"  Scrapers OK:     {success_count}")
        logger.info(f"  Scrapers mislukt: {fail_count}")
        logger.info(f"  Totaal records:  {total_records}")
        logger.info(f"  Beschikbaar:     {total_available}")

    if analytics_results:
        for seg, seg_result in analytics_results.items():
            meta = seg_result.get("metadata", {})
            label = SEGMENT_LABELS.get(seg, seg)
            logger.info(f"  [{label}] {meta.get('comparison_count', 0)} vergelijkingen, "
                        f"{len(seg_result.get('recommendations', []))} adviezen")

    for ep in excel_paths:
        logger.info(f"  Dashboard:       {ep}")

    if scrape_results:
        logger.info("\n  Per scraper:")
        for key, result in sorted(scrape_results.items()):
            status = result.get("status", "?")
            records = result.get("records", 0)
            dur = result.get("duration", 0)
            error = result.get("error", "")
            line = f"    {key:25s} {status:18s} {records:4d} records  {dur:6.1f}s"
            if error:
                line += f"  ({error[:60]})"
            logger.info(line)

    logger.info("=" * 60)

    # --- Phase 5: E-mail rapport versturen ---
    if not dry_run and scrape_results:
        logger.info("\n--- E-mail rapport versturen ---")
        try:
            from email_report import send_report
            send_report(
                scrape_results=scrape_results,
                excel_path=excel_path,
                total_duration=total_duration,
            )
        except Exception as e:
            logger.error(f"  E-mail versturen mislukt: {e}", exc_info=True)

    # Determine exit code
    if dry_run or skip_scrape:
        return EXIT_SUCCESS
    elif fail_count == 0:
        return EXIT_SUCCESS
    elif total_records > 0:
        return EXIT_PARTIAL
    else:
        return EXIT_FATAL


def main():
    parser = argparse.ArgumentParser(
        description="Concurrentiecheck Westerbergen - Dagelijkse Pipeline"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simuleer scraping zonder daadwerkelijk te draaien",
    )
    parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Sla scraping over, draai alleen analytics + dashboard",
    )
    parser.add_argument(
        "--config", default="config/settings.yaml",
        help="Pad naar configuratiebestand",
    )
    args = parser.parse_args()

    # Ensure UTF-8 output on Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # Load config
    config = load_config(args.config)

    # Setup logging
    log_file = setup_logging(
        log_dir=config.get("general", {}).get("log_dir", "logs"),
        level=config.get("general", {}).get("log_level", "INFO"),
    )

    logger = logging.getLogger("daily")
    logger.info("=" * 60)
    logger.info("CONCURRENTIECHECK WESTERBERGEN - DAGELIJKSE RUN")
    logger.info(f"  Datum:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Modus:      {'dry-run' if args.dry_run else 'skip-scrape' if args.skip_scrape else 'volledig'}")
    logger.info(f"  Log:        {log_file}")
    logger.info("=" * 60)

    exit_code = run_pipeline(
        config=config,
        dry_run=args.dry_run,
        skip_scrape=args.skip_scrape,
    )

    status_msg = {
        EXIT_SUCCESS: "SUCCES",
        EXIT_PARTIAL: "GEDEELTELIJK (sommige scrapers gefaald)",
        EXIT_FATAL: "MISLUKT",
    }
    logger.info(f"\nEindstatus: {status_msg.get(exit_code, '?')} (exit code {exit_code})")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
