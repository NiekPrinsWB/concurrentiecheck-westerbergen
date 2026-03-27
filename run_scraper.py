"""Main entry point for running all competitor scrapers.

Usage:
    python run_scraper.py                    # Run all enabled scrapers
    python run_scraper.py --competitor beerze # Run specific competitor
    python run_scraper.py --days 60          # Override days ahead
    python run_scraper.py --visible          # Show browser window
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

import yaml

from database import Database
from scrapers.beerze_bulten import (
    BeerzeBultenScraper, BeerzeBultenCampingScraper, BeerzeBultenPsanitairScraper
)
from scrapers.holidayagent_scraper import (
    CampingOmmerlandScraper, EilandVanMaurikScraper,
    CampingOmmerlandCampingScraper, CampingOmmerlandPsanitairScraper,
    EilandVanMaurikCampingScraper,
)
from scrapers.witter_zomer import WitterZomerScraper
from scrapers.de_witte_berg import DeWitteBergScraper
from scrapers.de_boshoek import DeBoshoekScraper
from scrapers.westerbergen import (
    WesterbergenScraper, WesterbergenCampingScraper, WesterbergenPsanitairScraper
)
from scrapers.centerparcs_scraper import CenterParcsScraper
from scrapers.molecaten_scraper import MolecatenKuierpadBosvenScraper, MolecatenKuierpadCampingScraper
from scrapers.zandstuve_scraper import (
    ZandstuveBoslodgeScraper, ZandstuveCampingScraper, ZandstuvePsanitairScraper
)
from scrapers.capfun_scraper import (
    CapfunStoetenslaghCampingScraper, CapfunStoetenslaghAccScraper,
    CapfunSprookjesCampingScraper, CapfunFruithofCampingScraper,
    CapfunFruithofAccScraper,
)
from scrapers.landal_scraper import (
    LandalAelderholtScraper, LandalAelderholtPremiumScraper, LandalBartjeScraper,
)
from scrapers.kleinewolf_scraper import KleineWolfCampingScraper, KleineWolfAccScraper
from scrapers.rcn_scraper import (
    RcnNoordsterMercuriusScraper, RcnNoordsterLunaScraper, RcnNoordsterCampingScraper,
)


def setup_logging(log_dir: str = "logs", level: str = "INFO"):
    """Configure logging to both console and file."""
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    # Root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(console)

    # File handler (more verbose)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(file_handler)

    return log_file


def load_config(config_path: str = "config/settings.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_scraper_map(db: Database, headless: bool):
    """Return mapping of competitor key -> scraper instance."""
    # Volgorde: API-scrapers eerst (snel, geen browser), dan BookingExperts
    # scrapers gespreid om rate-limiting te voorkomen.
    return {
        # --- API-only scrapers (snel, geen browser) ---
        "centerparcs_sandur": CenterParcsScraper(db=db, headless=headless),
        "molecaten_bosven": MolecatenKuierpadBosvenScraper(db=db, headless=headless),
        "molecaten_camping": MolecatenKuierpadCampingScraper(db=db, headless=headless),
        "camping_ommerland": CampingOmmerlandScraper(db=db, headless=headless),
        "ommerland_camping": CampingOmmerlandCampingScraper(db=db, headless=headless),
        "ommerland_psanitair": CampingOmmerlandPsanitairScraper(db=db, headless=headless),
        "eiland_van_maurik": EilandVanMaurikScraper(db=db, headless=headless),
        "maurik_camping": EilandVanMaurikCampingScraper(db=db, headless=headless),

        # --- BookingExperts scrapers (browser, gespreid) ---
        "de_boshoek": DeBoshoekScraper(db=db, headless=headless),
        "zandstuve_boslodge": ZandstuveBoslodgeScraper(db=db, headless=headless),
        "beerze_bulten": BeerzeBultenScraper(db=db, headless=headless),
        "bb_camping": BeerzeBultenCampingScraper(db=db, headless=headless),
        "zandstuve_camping": ZandstuveCampingScraper(db=db, headless=headless),
        "de_witte_berg": DeWitteBergScraper(db=db, headless=headless),
        "bb_psanitair": BeerzeBultenPsanitairScraper(db=db, headless=headless),
        "zandstuve_psanitair": ZandstuvePsanitairScraper(db=db, headless=headless),

        # --- Other browser scrapers ---
        "witter_zomer": WitterZomerScraper(db=db, headless=headless),

        # --- Capfun scrapers (HTTP API) ---
        "stoetenslagh_camping": CapfunStoetenslaghCampingScraper(db=db, headless=headless),
        "stoetenslagh_acc": CapfunStoetenslaghAccScraper(db=db, headless=headless),
        "sprookjes_camping": CapfunSprookjesCampingScraper(db=db, headless=headless),
        "fruithof_camping": CapfunFruithofCampingScraper(db=db, headless=headless),
        "fruithof_acc": CapfunFruithofAccScraper(db=db, headless=headless),

        # --- Landal scrapers (HTTP API) ---
        "landal_aelderholt": LandalAelderholtScraper(db=db, headless=headless),
        "landal_aelderholt_premium": LandalAelderholtPremiumScraper(db=db, headless=headless),
        "landal_bartje": LandalBartjeScraper(db=db, headless=headless),

        # --- De Kleine Wolf scrapers (HTTP API) ---
        "kleinewolf_camping": KleineWolfCampingScraper(db=db, headless=headless),
        "kleinewolf_acc": KleineWolfAccScraper(db=db, headless=headless),

        # --- RCN De Noordster scrapers (Playwright) ---
        "rcn_mercurius": RcnNoordsterMercuriusScraper(db=db, headless=headless),
        "rcn_luna": RcnNoordsterLunaScraper(db=db, headless=headless),
        "rcn_camping": RcnNoordsterCampingScraper(db=db, headless=headless),

        # --- Westerbergen (eigen park, alle segmenten) ---
        "westerbergen": WesterbergenScraper(db=db, headless=headless),
        "westerbergen_camping": WesterbergenCampingScraper(db=db, headless=headless),
        "westerbergen_psanitair": WesterbergenPsanitairScraper(db=db, headless=headless),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Concurrentiecheck Westerbergen - Price Scraper"
    )
    parser.add_argument(
        "--competitor", "-c",
        help="Run only a specific competitor (key from config)",
    )
    parser.add_argument(
        "--days", "-d", type=int, default=365,
        help="Days ahead to scrape (default: 365)",
    )
    parser.add_argument(
        "--visible", "-v", action="store_true",
        help="Show browser window (non-headless mode)",
    )
    parser.add_argument(
        "--config", default="config/settings.yaml",
        help="Path to config file",
    )
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # Setup logging
    log_file = setup_logging(
        log_dir=config.get("general", {}).get("log_dir", "logs"),
        level=config.get("general", {}).get("log_level", "INFO"),
    )

    logger = logging.getLogger("main")
    logger.info("=" * 60)
    logger.info("Concurrentiecheck Westerbergen - Starting scrape run")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Days ahead: {args.days}")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 60)

    # Initialize database
    db_path = config.get("general", {}).get("database_path", "data/concurrentiecheck.db")
    db = Database(db_path)

    # Get scrapers
    headless = not args.visible
    scrapers = get_scraper_map(db, headless)

    # Determine which scrapers to run
    if args.competitor:
        if args.competitor not in scrapers:
            logger.error(
                f"Unknown competitor: {args.competitor}. "
                f"Available: {list(scrapers.keys())}"
            )
            sys.exit(1)
        to_run = {args.competitor: scrapers[args.competitor]}
    else:
        # Run all enabled scrapers
        enabled = config.get("competitors", {})
        to_run = {
            key: scraper
            for key, scraper in scrapers.items()
            if enabled.get(key, {}).get("enabled", True)
        }

    # Run scrapers
    total_records = 0
    total_errors = 0

    # Convert days to months for scrapers that use months_ahead
    months_ahead = max(1, args.days // 30)
    target_end_date = (datetime.now() + timedelta(days=args.days)).strftime("%Y-%m-%d")
    # BookingExperts pages: ~3 days per page step
    max_pages = max(30, args.days // 3 + 10)

    for key, scraper in to_run.items():
        logger.info(f"\n--- Running: {scraper.competitor_name} ---")
        try:
            results = scraper.run_efficient(
                max_pages=max_pages,
                months_ahead=months_ahead,
                target_end_date=target_end_date,
                persons=config.get("scraping", {}).get("default_persons", 4),
            )
            available = [r for r in results if r.get("available") and r.get("price")]
            total_records += len(results)

            logger.info(
                f"  Results: {len(results)} total, "
                f"{len(available)} available with price"
            )
            if available:
                prices = [r["price"] for r in available]
                logger.info(
                    f"  Price range: EUR {min(prices):.0f} - EUR {max(prices):.0f}"
                )

        except Exception as e:
            total_errors += 1
            logger.error(f"  FAILED: {e}", exc_info=True)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SCRAPE RUN COMPLETE")
    logger.info(f"  Competitors: {len(to_run)}")
    logger.info(f"  Total records: {total_records}")
    logger.info(f"  Failed competitors: {total_errors}")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
