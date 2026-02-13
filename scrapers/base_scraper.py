"""Base scraper class with common functionality for all competitor scrapers."""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

from database import Database


class BaseScraper(ABC):
    """Abstract base class for competitor price scrapers."""

    def __init__(self, competitor_name: str, accommodation_type: str,
                 url: str, db: Database, headless: bool = True,
                 rate_limit: float = 5.0, max_retries: int = 3,
                 page_timeout: int = 60000):
        self.competitor_name = competitor_name
        self.accommodation_type = accommodation_type
        self.url = url
        self.db = db
        self.headless = headless
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.page_timeout = page_timeout
        self._last_request_time = 0

        self.logger = logging.getLogger(f"scraper.{competitor_name}")

    def _wait_rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            wait_time = self.rate_limit - elapsed
            self.logger.debug(f"Rate limiting: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _create_browser(self, playwright) -> Browser:
        """Create a browser instance with realistic settings."""
        return playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
        )

    def _create_page(self, browser: Browser) -> Page:
        """Create a new page with realistic browser fingerprint."""
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
        )
        page = context.new_page()
        page.set_default_timeout(self.page_timeout)
        return page

    def generate_check_dates(self, days_ahead_list: list[int] = None,
                              stay_types: dict = None) -> list[dict]:
        """Generate check-in/check-out date pairs to scrape.

        Returns list of dicts with: check_in, check_out, stay_type, nights
        """
        if days_ahead_list is None:
            days_ahead_list = [7, 14, 21, 30, 45, 60, 90]
        if stay_types is None:
            stay_types = {
                "weekend": {"check_in_day": "friday", "nights": 2},
                "midweek": {"check_in_day": "monday", "nights": 4},
                "week": {"check_in_day": "friday", "nights": 7},
            }

        day_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
            "friday": 4, "saturday": 5, "sunday": 6
        }

        dates = []
        today = datetime.now().date()

        for days_ahead in days_ahead_list:
            target = today + timedelta(days=days_ahead)
            for stay_name, stay_config in stay_types.items():
                target_day = day_map[stay_config["check_in_day"]]
                # Find the next occurrence of the target day
                days_until = (target_day - target.weekday()) % 7
                check_in = target + timedelta(days=days_until)
                check_out = check_in + timedelta(days=stay_config["nights"])

                dates.append({
                    "check_in": check_in,
                    "check_out": check_out,
                    "stay_type": stay_name,
                    "nights": stay_config["nights"],
                })

        # Remove duplicates (same check_in + check_out)
        seen = set()
        unique_dates = []
        for d in dates:
            key = (d["check_in"], d["check_out"])
            if key not in seen:
                seen.add(key)
                unique_dates.append(d)

        return sorted(unique_dates, key=lambda x: x["check_in"])

    @abstractmethod
    def scrape_price(self, page: Page, check_in: datetime,
                     check_out: datetime, persons: int = 4) -> dict | None:
        """Scrape price for a specific date range.

        Must return dict with keys:
            price (float or None), available (bool),
            min_nights (int or None), special_offers (str or None)
        Or None if scraping failed entirely.
        """
        pass

    def run(self, dates: list[dict] = None, persons: int = 4) -> list[dict]:
        """Run the scraper for all date combinations.

        Returns list of successfully scraped records.
        """
        if dates is None:
            dates = self.generate_check_dates()

        self.logger.info(
            f"Starting scrape for {self.competitor_name}: "
            f"{len(dates)} date combinations"
        )

        start_time = time.time()
        results = []
        errors = 0

        with sync_playwright() as playwright:
            browser = self._create_browser(playwright)
            try:
                page = self._create_page(browser)

                for date_info in dates:
                    check_in = date_info["check_in"]
                    check_out = date_info["check_out"]

                    for attempt in range(1, self.max_retries + 1):
                        self._wait_rate_limit()
                        try:
                            result = self.scrape_price(
                                page, check_in, check_out, persons
                            )
                            if result is not None:
                                record = {
                                    "competitor_name": self.competitor_name,
                                    "accommodation_type": self.accommodation_type,
                                    "check_in_date": check_in.isoformat(),
                                    "check_out_date": check_out.isoformat(),
                                    "price": result.get("price"),
                                    "available": result.get("available", True),
                                    "min_nights": result.get("min_nights"),
                                    "special_offers": result.get("special_offers"),
                                    "persons": persons,
                                }
                                self.db.save_price(**record)
                                results.append(record)
                                self.logger.info(
                                    f"  {check_in} -> {check_out}: "
                                    f"€{result.get('price', 'N/A')} "
                                    f"({'beschikbaar' if result.get('available') else 'niet beschikbaar'})"
                                )
                                break
                        except PlaywrightTimeout:
                            self.logger.warning(
                                f"  Timeout attempt {attempt}/{self.max_retries} "
                                f"for {check_in} -> {check_out}"
                            )
                            if attempt == self.max_retries:
                                errors += 1
                                self.logger.error(
                                    f"  FAILED after {self.max_retries} attempts: "
                                    f"{check_in} -> {check_out}"
                                )
                        except Exception as e:
                            self.logger.warning(
                                f"  Error attempt {attempt}/{self.max_retries} "
                                f"for {check_in} -> {check_out}: {e}"
                            )
                            if attempt == self.max_retries:
                                errors += 1
                                self.logger.error(
                                    f"  FAILED after {self.max_retries} attempts: "
                                    f"{check_in} -> {check_out}: {e}"
                                )
                            # Create fresh page on error
                            try:
                                page.close()
                            except Exception:
                                pass
                            page = self._create_page(browser)

            finally:
                browser.close()

        duration = time.time() - start_time
        status = "success" if errors == 0 else "partial" if results else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(results),
            error_message=f"{errors} date(s) failed" if errors else None,
            duration_seconds=duration,
        )

        self.logger.info(
            f"Completed {self.competitor_name}: {len(results)} prices scraped, "
            f"{errors} errors, {duration:.1f}s"
        )

        return results
