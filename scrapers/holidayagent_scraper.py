"""Scraper for sites using the HolidayAgent/HMCMS platform.

These sites have a REST API at api.holidayagent.nl that returns
arrival dates with departure options and prices in JSON format.

The API returns totalPrice (base ~2 persons) and additionalPrice
(surcharge for extra guests). For 4 persons the correct all-in
price is totalPrice + additionalPrice.

Used by: Camping Ommerland, Eiland van Maurik
"""

import json
import time
import logging
from datetime import datetime, timedelta

import requests

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)


class HolidayAgentScraper(BaseScraper):
    """Scraper for HolidayAgent-based booking sites.

    Uses the REST API directly (no browser needed) which is much faster.
    """

    def __init__(self, competitor_name: str, accommodation_type: str,
                 url: str, resort_slug: str, level_id: str,
                 db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name=competitor_name,
            accommodation_type=accommodation_type,
            url=url,
            db=db,
            headless=headless,
            **kwargs,
        )
        self.resort_slug = resort_slug
        self.level_id = level_id
        self.api_base = "https://api.holidayagent.nl/v1/resort"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Referer": url,
        })

    def _fetch_arrivals(self, months_ahead: int = 6) -> list[dict]:
        """Fetch all arrival dates with prices from the API."""
        api_url = (
            f"{self.api_base}/{self.resort_slug}/arrivals"
            f"?lng=nl"
            f"&levels%5B%5D={self.level_id}"
            f"&startdate-use-nearest=true"
            f"&amount-of-months={months_ahead}"
            f"&includes%5B%5D=specialperiods"
        )
        self.logger.debug(f"Fetching arrivals: {api_url}")

        resp = self.session.get(api_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        return data.get("response", {}).get("arrivals", [])

    def scrape_price(self, page, check_in, check_out, persons=4):
        """Not used - we use run_efficient() with the API instead."""
        raise NotImplementedError("Use run_efficient() for HolidayAgent sites")

    def run_efficient(self, months_ahead: int = 6, persons: int = 4,
                      **kwargs) -> list[dict]:
        """Scrape all prices via the HolidayAgent REST API.

        This is very fast (~1-2 seconds) since it's a single API call
        that returns all available dates and prices.
        """
        self.logger.info(
            f"Starting API scrape for {self.competitor_name}: "
            f"{months_ahead} months ahead"
        )

        start_time = time.time()
        all_records = []
        errors = 0

        try:
            arrivals = self._fetch_arrivals(months_ahead)
            self.logger.info(f"  API returned {len(arrivals)} arrival dates")

            for arrival in arrivals:
                arrival_date = arrival.get("date", "")  # e.g. "13-02-2026"
                if not arrival_date:
                    continue

                # Parse arrival date (DD-MM-YYYY format)
                try:
                    check_in_dt = datetime.strptime(arrival_date, "%d-%m-%Y")
                except ValueError:
                    continue

                for departure in arrival.get("departures", []):
                    try:
                        dep_date = departure.get("date", "")
                        nights = int(departure.get("nights", 0))
                        prices = departure.get("prices", {})
                        base_price = prices.get("totalPrice")
                        additional_price = prices.get("additionalPrice", 0)
                        discount_price = prices.get("discountPrice", 0)
                        amount_available = departure.get("amountAvailable", 0)

                        # additionalPrice = surcharge for extra guests (above base 2)
                        # For 4 persons: total = base + additional
                        total_price = base_price
                        if base_price is not None and additional_price:
                            total_price = base_price + additional_price

                        # Filter for relevant stay durations
                        if nights not in (2, 3, 4, 5, 7):
                            continue

                        check_out_dt = datetime.strptime(dep_date, "%d-%m-%Y")

                        # Determine special offers
                        special_offers = None
                        if discount_price and discount_price > 0:
                            special_offers = f"Korting: EUR {discount_price:.0f}"

                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "check_in_date": check_in_dt.strftime("%Y-%m-%d"),
                            "check_out_date": check_out_dt.strftime("%Y-%m-%d"),
                            "price": total_price,
                            "available": amount_available > 0 and total_price is not None,
                            "min_nights": nights,
                            "special_offers": special_offers,
                            "persons": persons,
                        }
                        self.db.save_price(**record)
                        all_records.append(record)
                    except (TypeError, ValueError, KeyError) as e:
                        self.logger.warning(f"  Skipping malformed departure: {e}")
                        continue

        except Exception as e:
            errors += 1
            self.logger.error(f"  API call failed: {e}")

        duration = time.time() - start_time
        status = "success" if errors == 0 else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_records),
            error_message=str(errors) + " errors" if errors else None,
            duration_seconds=duration,
        )

        available = [r for r in all_records if r["available"] and r["price"]]
        self.logger.info(
            f"Completed {self.competitor_name}: {len(all_records)} records "
            f"({len(available)} available), {duration:.1f}s"
        )

        return all_records


class CampingOmmerlandScraper(HolidayAgentScraper):
    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Camping Ommerland",
            accommodation_type="Bos Villa (6p)",
            url="https://www.ommerland.nl/huren/bos-villa",
            resort_slug="campingommerland",
            level_id="20334",
            db=db,
            headless=headless,
            **kwargs,
        )


class EilandVanMaurikScraper(HolidayAgentScraper):
    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Eiland van Maurik",
            accommodation_type="EilandLodge (6 persoons)",
            url="https://www.eilandvanmaurik.nl/accommodaties/eilandlodge-6",
            resort_slug="eilandvanmaurik",
            level_id="9504",
            db=db,
            headless=headless,
            **kwargs,
        )
