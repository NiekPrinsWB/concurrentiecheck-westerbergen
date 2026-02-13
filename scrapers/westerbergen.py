"""Scraper for Westerbergen (own park) - Bosbungalow Sequoia C6.

Uses a custom booking system with REST API endpoints:
1. /web/recreation/getAvailableDatesByYearMonth - available arrival dates
2. /web/recreation/getPricesByYearMonth - prices for a given arrival date

Both endpoints require X-Requested-With: XMLHttpRequest header.
The API returns prices for all possible stay durations (1-28 nights)
for each arrival date.

Prices are fetched with withExtras=true to include all mandatory costs:
eindschoonmaak, bedlinnen, administratiekosten, parklasten.
This ensures fair comparison with competitor prices.
"""

import json
import time
import logging
from datetime import datetime, timedelta

from playwright.sync_api import TimeoutError as PlaywrightTimeout

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)


class WesterbergenScraper(BaseScraper):
    """Scraper for Westerbergen via its custom booking REST API."""

    BOOKING_URL = (
        "https://www.westerbergen.nl/accommodaties/bosbungalow-sequoia-c6"
        "/boeken?type%5B0%5D=169"
    )

    # objectType=354 and rental=169 correspond to Bosbungalow Sequoia C6
    OBJECT_TYPE = "354"
    RENTAL_ID = "169"

    # Stay durations to track (matching competitor scrapers)
    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Westerbergen",
            accommodation_type="Bosbungalow Sequoia (C6)",
            url="https://www.westerbergen.nl/accommodaties/bosbungalow-sequoia-c6",
            db=db,
            headless=headless,
            **kwargs,
        )

    def scrape_price(self, page, check_in, check_out, persons=4):
        raise NotImplementedError("Use run_efficient()")

    def run_efficient(self, months_ahead: int = 3, persons: int = 4,
                      **kwargs) -> list[dict]:
        """Scrape prices using the booking page REST API.

        Loads the booking page to get a valid session, then uses
        Promise.all to batch-fetch all prices efficiently.
        """
        from playwright.sync_api import sync_playwright

        self.logger.info(
            f"Starting scrape for {self.competitor_name}: "
            f"{months_ahead} months ahead"
        )

        start_time = time.time()
        all_records = []
        errors = 0

        # Build list of (year, month) to query
        now = datetime.now()
        year_months = []
        y, m = now.year, now.month
        for i in range(months_ahead + 1):
            ym = (y, m)
            if ym not in year_months:
                year_months.append(ym)
            m += 1
            if m > 12:
                m = 1
                y += 1

        durations_js = json.dumps(self.STAY_DURATIONS)

        with sync_playwright() as playwright:
            browser = self._create_browser(playwright)
            try:
                page = self._create_page(browser)

                # Load the booking page to establish session
                self.logger.info("  Loading booking page...")
                page.goto(self.BOOKING_URL, wait_until="networkidle", timeout=120000)
                time.sleep(3)

                # Accept cookies (Cookiebot)
                try:
                    btn = page.query_selector(
                        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"
                    )
                    if btn and btn.is_visible():
                        btn.click()
                        time.sleep(1)
                except Exception:
                    pass

                # Build the months array for JS
                months_js = json.dumps(year_months)

                # Batch fetch all available dates + prices in one evaluate call
                self.logger.info(
                    f"  Fetching prices for {len(year_months)} months..."
                )

                result = page.evaluate("""
                    async (params) => {
                        const headers = {'X-Requested-With': 'XMLHttpRequest'};
                        const months = params.months;
                        const objectType = params.objectType;
                        const rentalId = params.rentalId;
                        const durations = params.durations;
                        const persons = params.persons;

                        // Step 1: Get available dates for all months
                        const datePromises = months.map(([y, m]) =>
                            fetch('/web/recreation/getAvailableDatesByYearMonth'
                                + '?language=nl&year=' + y
                                + '&month=' + String(m).padStart(2, '0')
                                + '&objectType=' + objectType
                                + '&rental[]=' + rentalId
                                + '&package=all',
                                {headers}
                            )
                            .then(r => r.json())
                            .catch(e => ({available: []}))
                        );
                        const dateResults = await Promise.all(datePromises);

                        // Collect unique dates
                        const allDates = new Set();
                        dateResults.forEach(r =>
                            (r.available || []).forEach(d => allDates.add(d))
                        );
                        const dates = [...allDates].sort();

                        // Step 2: Fetch prices in batches of 10
                        const allPrices = [];
                        for (let i = 0; i < dates.length; i += 10) {
                            const batch = dates.slice(i, i + 10);
                            const pricePromises = batch.map(dateStr => {
                                const parts = dateStr.split('/');
                                const day = parseInt(parts[0]);
                                const month = parseInt(parts[1]);
                                const year = parseInt(parts[2]);
                                return fetch(
                                    '/web/recreation/getPricesByYearMonth'
                                    + '?language=nl&withExtras=true'
                                    + '&persons=' + persons
                                    + '&objectType=' + objectType
                                    + '&year=' + year
                                    + '&month=' + month
                                    + '&day=' + day
                                    + '&rental[]=' + rentalId,
                                    {headers}
                                )
                                .then(r => r.json())
                                .catch(e => ({periods: [], packages: []}));
                            });
                            const priceResults = await Promise.all(pricePromises);
                            priceResults.forEach(r => {
                                (r.periods || []).forEach(p => {
                                    const raw = p.raw;
                                    if (raw && durations.includes(raw.nights)) {
                                        allPrices.push(raw);
                                    }
                                });
                                (r.packages || []).forEach(p => {
                                    const raw = p.raw;
                                    if (raw && durations.includes(raw.nights)) {
                                        raw.is_package = true;
                                        allPrices.push(raw);
                                    }
                                });
                            });
                        }

                        return {dates: dates.length, prices: allPrices};
                    }
                """, {
                    "months": year_months,
                    "objectType": self.OBJECT_TYPE,
                    "rentalId": self.RENTAL_ID,
                    "durations": self.STAY_DURATIONS,
                    "persons": persons,
                })

                dates_count = result.get("dates", 0)
                prices = result.get("prices", [])

                self.logger.info(
                    f"  Found {dates_count} arrival dates, "
                    f"{len(prices)} price entries"
                )

                # Process results
                seen_keys = set()
                for p in prices:
                    arrival_raw = p.get("arrivaldate", "")
                    departure_raw = p.get("departuredate", "")
                    nights = p.get("nights", 0)
                    price = p.get("price")
                    available = p.get("available", 0)

                    if not arrival_raw or price is None:
                        continue

                    # Convert DD/MM/YYYY to YYYY-MM-DD
                    try:
                        arr_parts = arrival_raw.split("/")
                        check_in = f"{arr_parts[2]}-{arr_parts[1]}-{arr_parts[0]}"
                        dep_parts = departure_raw.split("/")
                        check_out = f"{dep_parts[2]}-{dep_parts[1]}-{dep_parts[0]}"
                    except (IndexError, ValueError):
                        continue

                    key = (check_in, check_out)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    special = None
                    if p.get("discounted"):
                        from_price = p.get("fromprice", 0)
                        if from_price and from_price > price:
                            special = f"Was EUR {from_price:.0f}"

                    record = {
                        "competitor_name": self.competitor_name,
                        "accommodation_type": self.accommodation_type,
                        "check_in_date": check_in,
                        "check_out_date": check_out,
                        "price": float(price),
                        "available": bool(available),
                        "min_nights": nights,
                        "special_offers": special,
                        "persons": persons,
                    }
                    self.db.save_price(**record)
                    all_records.append(record)

            except Exception as e:
                errors += 1
                self.logger.error(f"  Scraping failed: {e}", exc_info=True)
            finally:
                browser.close()

        duration = time.time() - start_time
        status = "success" if errors == 0 else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_records),
            error_message=f"{errors} errors" if errors else None,
            duration_seconds=duration,
        )

        available = [r for r in all_records if r["available"] and r["price"]]
        self.logger.info(
            f"Completed {self.competitor_name}: {len(all_records)} records "
            f"({len(available)} available), {duration:.1f}s"
        )

        return all_records
