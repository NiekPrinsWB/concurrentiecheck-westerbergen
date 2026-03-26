"""Scraper for Center Parcs Parc Sandur — Comfort cottage SR390 6p.

Uses the flexCalendar API at cpe-search-api.groupepvcp.com, routed through
a Playwright browser session to bypass Akamai Bot Manager protection.

The browser loads the Center Parcs page first to establish Akamai cookies,
then uses page.evaluate(fetch(...)) to call the API with those cookies.

One API call per duration returns all available dates with prices at once.
"""

import json
import time
import logging
from datetime import datetime, timedelta

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

# Parc Sandur cottage codes
COTTAGE_CODES = {
    "comfort_6p": "SR390",      # Comfort cottage 6 personen (standaard referentie)
    "premium_6p": "SR393",      # Premium cottage 6 personen
    "vip_6p": "SR391",          # VIP cottage 6 personen
}


class CenterParcsScraper(BaseScraper):
    """Scraper for Center Parcs Parc Sandur via the flexCalendar API."""

    API_BASE = "https://cpe-search-api.groupepvcp.com/v1/product/flexCalendar"
    TOKEN_PAGE = "https://www.centerparcs.nl/nl-nl/nederland/fp_SR_vakantiepark-parc-sandur.htm"

    STAY_DURATIONS = [2, 3, 4, 5, 7]

    def __init__(self, db: Database, headless: bool = True,
                 housing_code: str = "SR390",
                 accommodation_type: str = "Comfort cottage SR390 (6p)",
                 segment: str = "accommodatie", **kwargs):
        super().__init__(
            competitor_name="Center Parcs Parc Sandur",
            accommodation_type=accommodation_type,
            url="https://www.centerparcs.nl/nl-nl/nederland/fp_SR_vakantiepark-parc-sandur.htm",
            db=db,
            headless=headless,
            **kwargs,
        )
        self.housing_code = housing_code
        self.segment = segment

    def _parse_api_response(self, data: dict, duration: int) -> list[dict]:
        """Parse the flexCalendar API response into price records."""
        results = []
        dates = data.get("results", {}).get("results", {}).get("dates", {})

        for date_str, info in dates.items():
            cache = info.get("cache", {})
            if not cache:
                continue

            price_data = cache.get("price", {})
            original = price_data.get("original", {})
            promo = price_data.get("promo", {})

            price_incl = promo.get("raw") or original.get("raw")
            discount_pct = price_data.get("discount", 0)
            stock = cache.get("stock", 0)

            if price_incl is None:
                continue

            try:
                price_incl = float(price_incl)
                check_in = datetime.strptime(date_str, "%Y-%m-%d")
                check_out = check_in + timedelta(days=duration)
            except (ValueError, TypeError):
                continue

            special = None
            if discount_pct and float(discount_pct) > 0:
                orig_price = float(original.get("raw", 0))
                special = f"Korting {float(discount_pct):.0f}% (was EUR {orig_price:.0f})"

            results.append({
                "check_in": check_in,
                "check_out": check_out,
                "price": float(price_incl),
                "available": stock > 0,
                "duration": duration,
                "special_offers": special,
            })

        return results

    def scrape_price(self, page, check_in, check_out, persons=6):
        raise NotImplementedError("Use run_efficient()")

    def run_efficient(self, months_ahead: int = 12, persons: int = 6,
                      **kwargs) -> list[dict]:
        """Scrape prices by loading CP page in browser, then fetching API via JS.

        The browser session carries Akamai cookies that allow API access.
        """
        from playwright.sync_api import sync_playwright

        self.logger.info(
            f"Starting browser-based scrape for {self.competitor_name} "
            f"({self.housing_code}, segment={self.segment})"
        )

        start_time = time.time()
        all_records = []
        errors = 0

        with sync_playwright() as playwright:
            browser = self._create_browser(playwright)
            try:
                page = self._create_page(browser)

                # Load Center Parcs page to establish Akamai session
                self.logger.info("  Loading Center Parcs page for Akamai session...")
                page.goto(self.TOKEN_PAGE, wait_until="networkidle", timeout=60000)
                time.sleep(3)

                # Extract search token from page
                token = page.evaluate("""() => {
                    for (const s of document.querySelectorAll('script')) {
                        const m = s.textContent.match(/SEARCH_TOKEN['"\\s:]+['"](\\w{20,})['"]/);
                        if (m) return m[1];
                    }
                    return null;
                }""")

                if not token:
                    self.logger.error("  Could not find SEARCH_TOKEN in page")
                    raise RuntimeError("SEARCH_TOKEN not found")

                self.logger.info(f"  Token: {token[:8]}...")

                # Fetch prices for each duration via browser fetch
                durations_json = json.dumps(self.STAY_DURATIONS)
                results = page.evaluate("""async (params) => {
                    const results = {};
                    for (const duration of params.durations) {
                        try {
                            const url = params.apiBase
                                + '?univers=cpe&language=nl&market=nl'
                                + '&token=' + params.token
                                + '&currency=EUR&residence=SR'
                                + '&housing=' + params.housing
                                + '&duration=' + duration
                                + '&adults=' + params.adults
                                + '&children=0&babies=0';
                            const resp = await fetch(url);
                            if (resp.ok) {
                                results[duration] = await resp.json();
                            } else {
                                results[duration] = {error: resp.status};
                            }
                        } catch(e) {
                            results[duration] = {error: e.message};
                        }
                    }
                    return results;
                }""", {
                    "apiBase": self.API_BASE,
                    "token": token,
                    "housing": self.housing_code,
                    "adults": persons,
                    "durations": self.STAY_DURATIONS,
                })

                for duration in self.STAY_DURATIONS:
                    data = results.get(str(duration), {})
                    if "error" in data:
                        errors += 1
                        self.logger.error(
                            f"  Duration {duration}n failed: {data['error']}"
                        )
                        continue

                    prices = self._parse_api_response(data, duration)
                    self.logger.info(
                        f"  Duration {duration}n: {len(prices)} dates"
                    )

                    for p in prices:
                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "check_in_date": p["check_in"].strftime("%Y-%m-%d"),
                            "check_out_date": p["check_out"].strftime("%Y-%m-%d"),
                            "price": p["price"],
                            "available": p["available"],
                            "min_nights": p["duration"],
                            "special_offers": p["special_offers"],
                            "persons": persons,
                            "segment": self.segment,
                        }
                        self.db.save_price(**record)
                        all_records.append(record)

            except Exception as e:
                errors += 1
                self.logger.error(f"  Scraping failed: {e}", exc_info=True)
            finally:
                browser.close()

        duration_s = time.time() - start_time
        status = "success" if errors == 0 else "partial" if all_records else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_records),
            error_message=f"{errors} errors" if errors else None,
            duration_seconds=duration_s,
            segment=self.segment,
        )

        available = [r for r in all_records if r["available"] and r["price"]]
        self.logger.info(
            f"Completed {self.competitor_name}: {len(all_records)} records "
            f"({len(available)} available), {duration_s:.1f}s"
        )

        return all_records
