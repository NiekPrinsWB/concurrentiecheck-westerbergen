"""Scraper for Beerze Bulten - Luxe Bungalow.

Website uses BookingExperts platform with a price grid table.
The grid shows 7 days as columns and 1-30 nights as rows.
Navigate to different weeks via: ?grid_center[search_date]=YYYY-MM-DD

Price cells either contain a price (e.g. "€ 524") or are marked
unavailable with class 'price-grid-table-unavailable'.
"""

import re
import time
import logging
from datetime import datetime, timedelta

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)


class BeerzeBultenScraper(BaseScraper):

    # Default number of guests for pricing (affects tourist tax etc.)
    DEFAULT_PERSONS = 4

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Beerze Bulten",
            accommodation_type="Luxe Bungalow",
            url="https://www.beerzebulten.nl/accommodaties/bungalow",
            db=db,
            headless=headless,
            **kwargs,
        )

    def _accept_cookies(self, page: Page):
        """Handle cookie consent banner."""
        try:
            for selector in [
                "button:has-text('Alles accepteren')",
                "button:has-text('Accepteer')",
                "button:has-text('Akkoord')",
            ]:
                btn = page.query_selector(selector)
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(1)
                    self.logger.debug("Cookie banner accepted")
                    return
        except Exception as e:
            self.logger.debug(f"Cookie handling: {e}")

    def _parse_price_grid(self, page: Page) -> list[dict]:
        """Parse the price grid table and extract all prices with dates.

        Returns list of dicts with keys:
            check_in_date (str YYYY-MM-DD), nights (int), price (float or None),
            available (bool)
        """
        results = page.evaluate("""
            () => {
                const table = document.querySelector('.price-grid-table');
                if (!table) return [];

                // Parse column headers to get dates
                const headerCells = table.querySelectorAll('thead th');
                const dates = [];
                for (let i = 1; i < headerCells.length; i++) {
                    const text = headerCells[i].innerText.trim();
                    dates.push(text);  // e.g. "vr\\n27 feb"
                }

                // Parse each row to get nights and prices
                const rows = table.querySelectorAll('tbody tr.price-grid-table-result-row');
                const results = [];

                for (const row of rows) {
                    const cells = row.querySelectorAll('td, th');
                    if (cells.length < 2) continue;

                    // First cell is the nights label
                    const nightsText = cells[0].innerText.trim();
                    const nightsMatch = nightsText.match(/(\\d+)/);
                    if (!nightsMatch) continue;
                    const nights = parseInt(nightsMatch[1]);

                    // Remaining cells are prices for each date column
                    for (let i = 1; i < cells.length; i++) {
                        const cell = cells[i];
                        const isUnavailable = cell.classList.contains('price-grid-table-unavailable');
                        const priceText = cell.innerText.trim();

                        // Extract price from text like "€ 524" or "€ 1.065"
                        const priceMatch = priceText.match(/€\\s*([\\d.]+)/);
                        const price = priceMatch
                            ? parseFloat(priceMatch[1].replace('.', '').replace(',', '.'))
                            : null;

                        // Get the booking link if present
                        const link = cell.querySelector('a');
                        const href = link ? link.href : '';

                        if (i - 1 < dates.length) {
                            results.push({
                                dateHeader: dates[i - 1],
                                nights: nights,
                                price: price,
                                available: !isUnavailable && price !== null,
                                href: href,
                            });
                        }
                    }
                }

                return results;
            }
        """)
        return results

    def _resolve_date(self, date_header: str, reference_date: datetime) -> str | None:
        """Convert a date header like 'vr\\n27 feb' to 'YYYY-MM-DD'.

        Uses the reference_date to determine the correct year.
        The grid shows dates close to reference_date, so we pick the year
        that produces a date closest to the reference.
        """
        month_map = {
            'jan': 1, 'feb': 2, 'mrt': 3, 'apr': 4, 'mei': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'okt': 10, 'nov': 11, 'dec': 12,
        }

        # Extract day and month from text like "vr\n27 feb" or "ma\n 2 mrt"
        match = re.search(r'(\d+)\s+(jan|feb|mrt|apr|mei|jun|jul|aug|sep|okt|nov|dec)',
                          date_header, re.IGNORECASE)
        if not match:
            return None

        day = int(match.group(1))
        month = month_map.get(match.group(2).lower())
        if month is None:
            return None

        # Try current year and next year, pick the one closest to reference
        ref = reference_date if isinstance(reference_date, datetime) else datetime.combine(reference_date, datetime.min.time())
        best = None
        for year in [ref.year, ref.year + 1]:
            try:
                candidate = datetime(year, month, day)
                diff = abs((candidate - ref).days)
                if best is None or diff < best[1]:
                    best = (candidate, diff)
            except ValueError:
                continue

        return best[0].strftime("%Y-%m-%d") if best else None

    def scrape_price(self, page: Page, check_in: datetime,
                     check_out: datetime, persons: int = 4) -> dict | None:
        """Scrape price for a specific date range from Beerze Bulten.

        Uses the price grid approach: navigates to the week containing
        check_in, then reads the grid to find the matching cell.
        """
        nights = (check_out - check_in).days

        # Navigate to the page with the grid centered on the check-in date
        grid_url = (
            f"{self.url}"
            f"?grid_center%5Bsearch_date%5D={check_in.strftime('%Y-%m-%d')}"
        )
        self.logger.debug(f"Loading grid for {check_in}: {grid_url}")

        page.goto(grid_url, wait_until="networkidle")
        time.sleep(2)

        # Accept cookies on first visit
        self._accept_cookies(page)

        # Wait for the price grid to load
        try:
            page.wait_for_selector(".price-grid-table", timeout=15000)
        except PlaywrightTimeout:
            self.logger.warning("Price grid table not found")
            return None

        time.sleep(1)  # Extra wait for grid to fully populate

        # Parse all prices from the grid
        grid_data = self._parse_price_grid(page)

        # Find the matching cell: correct check-in date + correct nights
        target_date_str = check_in.strftime("%Y-%m-%d")

        for entry in grid_data:
            resolved = self._resolve_date(entry["dateHeader"], check_in)
            if resolved == target_date_str and entry["nights"] == nights:
                return {
                    "price": entry["price"],
                    "available": entry["available"],
                    "min_nights": nights,
                    "special_offers": None,
                }

        # If we didn't find an exact match, the date might not be in view
        self.logger.warning(
            f"No matching grid cell for {check_in} ({nights}n) - "
            f"grid had {len(grid_data)} entries"
        )
        return {
            "price": None,
            "available": False,
            "min_nights": nights,
            "special_offers": None,
        }

    def scrape_grid_week(self, page: Page, center_date: datetime,
                         persons: int = 4) -> list[dict]:
        """Scrape ALL prices from one grid page (7 days x multiple night options).

        This is more efficient than scrape_price() for bulk scraping, as it
        gets all available prices in one page load.

        Returns list of records ready for database storage.
        """
        grid_url = (
            f"{self.url}"
            f"?grid_center%5Bsearch_date%5D={center_date.strftime('%Y-%m-%d')}"
        )
        self.logger.debug(f"Loading grid week around {center_date}")

        page.goto(grid_url, wait_until="networkidle")
        time.sleep(2)

        self._accept_cookies(page)

        try:
            page.wait_for_selector(".price-grid-table", timeout=15000)
        except PlaywrightTimeout:
            self.logger.warning("Price grid table not found")
            return []

        time.sleep(1)

        grid_data = self._parse_price_grid(page)
        records = []

        for entry in grid_data:
            check_in_str = self._resolve_date(entry["dateHeader"], center_date)
            if not check_in_str:
                continue

            check_in_dt = datetime.strptime(check_in_str, "%Y-%m-%d")
            check_out_dt = check_in_dt + timedelta(days=entry["nights"])

            records.append({
                "competitor_name": self.competitor_name,
                "accommodation_type": self.accommodation_type,
                "check_in_date": check_in_str,
                "check_out_date": check_out_dt.strftime("%Y-%m-%d"),
                "price": entry["price"],
                "available": entry["available"],
                "min_nights": entry["nights"],
                "special_offers": None,
                "persons": persons,
            })

        return records

    def _get_later_url(self, page: Page) -> str | None:
        """Get the URL of the 'Later' navigation link in the price grid."""
        return page.evaluate("""
            () => {
                const links = document.querySelectorAll('a');
                for (const a of links) {
                    if (a.innerText.trim() === 'Later') return a.href;
                }
                return null;
            }
        """)

    def _get_grid_date_range(self, page: Page) -> tuple[str, str] | None:
        """Get the first and last date shown in the current grid."""
        headers = page.evaluate("""
            () => {
                const table = document.querySelector('.price-grid-table');
                if (!table) return [];
                const ths = table.querySelectorAll('thead th');
                return Array.from(ths).slice(1).map(th => th.innerText.trim());
            }
        """)
        if not headers:
            return None
        return (headers[0], headers[-1])

    def _build_url_with_guests(self, base_url: str = None, persons: int = None) -> str:
        """Add or update guest_group parameter in URL for correct pricing."""
        url = base_url or self.url
        persons = persons or self.DEFAULT_PERSONS
        # Remove existing guest_group param to avoid duplicates
        import re as _re
        url = _re.sub(r'[&?]guest_group%5Badults%5D=\d+', '', url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}guest_group%5Badults%5D={persons}"

    def run_efficient(self, max_pages: int = 30, target_end_date: str = None,
                      persons: int = None) -> list[dict]:
        """Efficiently scrape by navigating through grid pages using 'Later' link.

        The grid shows 7 days at a time, and the 'Later' link shifts ~3 days forward.
        We follow this link to cover all dates up to target_end_date.

        Args:
            max_pages: Maximum number of grid pages to load (safety limit).
            target_end_date: Stop when grid dates pass this date (YYYY-MM-DD).
                           Defaults to ~90 days from now.
            persons: Number of persons for pricing (default: DEFAULT_PERSONS).
        """
        from playwright.sync_api import sync_playwright

        if persons is None:
            persons = self.DEFAULT_PERSONS

        if target_end_date is None:
            target_end = datetime.now() + timedelta(days=90)
            target_end_date = target_end.strftime("%Y-%m-%d")

        self.logger.info(
            f"Starting efficient scrape for {self.competitor_name}: "
            f"up to {target_end_date} (max {max_pages} pages, {persons} persons)"
        )

        start_time = time.time()
        all_records = []
        seen_keys = set()
        errors = 0

        with sync_playwright() as playwright:
            browser = self._create_browser(playwright)
            try:
                page = self._create_page(browser)

                # Start at the base URL with guest count
                start_url = self._build_url_with_guests(persons=persons)
                page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(3)
                self._accept_cookies(page)

                for page_num in range(1, max_pages + 1):
                    try:
                        page.wait_for_selector(".price-grid-table", timeout=15000)
                    except PlaywrightTimeout:
                        self.logger.warning(f"Page {page_num}: no price grid found")
                        errors += 1
                        break

                    time.sleep(1)

                    # Get current grid date range for logging
                    date_range = self._get_grid_date_range(page)
                    range_str = f"{date_range[0]} - {date_range[1]}" if date_range else "unknown"

                    try:
                        # Parse the current grid
                        grid_data = self._parse_price_grid(page)

                        # Determine reference date from first header
                        ref_date = datetime.now()
                        if date_range:
                            resolved = self._resolve_date(date_range[0], datetime.now())
                            if resolved:
                                ref_date = datetime.strptime(resolved, "%Y-%m-%d")

                        # Convert to records
                        new_count = 0
                        for entry in grid_data:
                            if entry["nights"] not in (2, 3, 4, 7):
                                continue

                            check_in_str = self._resolve_date(entry["dateHeader"], ref_date)
                            if not check_in_str:
                                continue

                            check_in_dt = datetime.strptime(check_in_str, "%Y-%m-%d")
                            check_out_dt = check_in_dt + timedelta(days=entry["nights"])
                            key = (check_in_str, check_out_dt.strftime("%Y-%m-%d"))

                            if key in seen_keys:
                                continue
                            seen_keys.add(key)

                            record = {
                                "competitor_name": self.competitor_name,
                                "accommodation_type": self.accommodation_type,
                                "check_in_date": check_in_str,
                                "check_out_date": check_out_dt.strftime("%Y-%m-%d"),
                                "price": entry["price"],
                                "available": entry["available"],
                                "min_nights": entry["nights"],
                                "special_offers": None,
                                "persons": persons,
                            }
                            self.db.save_price(**record)
                            all_records.append(record)
                            new_count += 1

                        self.logger.info(
                            f"  Page {page_num} ({range_str}): "
                            f"{new_count} new prices"
                        )

                        # Check if we've passed the target end date
                        if date_range:
                            last_date = self._resolve_date(date_range[-1], ref_date)
                            if last_date and last_date > target_end_date:
                                self.logger.info(f"  Reached target end date {target_end_date}")
                                break

                    except Exception as e:
                        errors += 1
                        self.logger.error(f"  Page {page_num} failed: {e}")

                    # Navigate to next week via "Later" link
                    later_url = self._get_later_url(page)
                    if not later_url:
                        self.logger.info("  No 'Later' link found, stopping.")
                        break

                    # Ensure guest count is preserved in the URL
                    later_url = self._build_url_with_guests(later_url, persons)

                    self._wait_rate_limit()
                    page.goto(later_url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(2)

            finally:
                browser.close()

        duration = time.time() - start_time
        status = "success" if errors == 0 else "partial" if all_records else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_records),
            error_message=f"{errors} week(s) failed" if errors else None,
            duration_seconds=duration,
        )

        self.logger.info(
            f"Completed {self.competitor_name}: {len(all_records)} prices, "
            f"{errors} errors, {duration:.1f}s"
        )

        return all_records
