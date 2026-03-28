"""Scraper for Camping De Kleine Wolf.

De Kleine Wolf uses a HolidayAgent variant with a TOMM bridge.
The API endpoint and authentication differ from standard HolidayAgent:
- Custom URL: app.holidayagent.nl/api/v1/resort/kleinewolf_tomm/availability
- Bearer-style Authorization header (no "Bearer " prefix)
- Uses alternativesDaysBeforeAndAfterArrival for date coverage
- Requires explicit persons[adults] and alternativeNrOfNights[] params

Segments:
- Camping: Comfortplaats (level 56373), 2 persons
- Accommodation: Klaverlodge 6p (level 56397), 4 persons
"""

import time
import logging
from datetime import datetime, timedelta

import requests

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

API_URL = "https://app.holidayagent.nl/api/v1/resort/kleinewolf_tomm/availability"
AUTH_TOKEN = "e9l3Nt14fZ7vHb4QZBw1ZfLgnl6n7nQB"
TARGET_DURATIONS = {2, 3, 4, 7}


class KleineWolfScraper(BaseScraper):
    """Base scraper for Camping De Kleine Wolf (HolidayAgent/TOMM bridge).

    Uses a REST API with weekly iteration and alternatives parameter
    to cover all arrival dates over 12 months.
    """

    def __init__(self, competitor_name: str, accommodation_type: str,
                 url: str, level_id: str, segment: str, persons: int,
                 db: Database, headless: bool = True,
                 add_additional_price: bool = True, **kwargs):
        super().__init__(
            competitor_name=competitor_name,
            accommodation_type=accommodation_type,
            url=url,
            db=db,
            headless=headless,
            rate_limit=0.5,
            **kwargs,
        )
        self.level_id = level_id
        self.segment = segment
        self.persons = persons
        self.add_additional_price = add_additional_price
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Authorization": AUTH_TOKEN,
            "Referer": url,
        })

    def _build_params(self, arrival_date: str) -> dict:
        """Build query parameters for the availability API.

        Args:
            arrival_date: Date string in DD-MM-YYYY format.
        """
        return {
            "arrivalDate": arrival_date,
            "nrOfNights": 7,
            "alternativesDaysBeforeAndAfterArrival": 3,
            "alternativeNrOfNights[]": [2, 3, 4, 7],
            "levels[]": self.level_id,
            "persons[adults]": self.persons,
            "lng": "nl",
            "includes[]": "criteria_combinations",
            "alternativeSorting": "insideout",
        }

    def _fetch_availability(self, arrival_date: str) -> dict:
        """Fetch availability data for a given arrival date.

        Args:
            arrival_date: Date string in DD-MM-YYYY format.

        Returns:
            Parsed JSON response dict.
        """
        params = self._build_params(arrival_date)
        resp = self.session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _parse_response(self, data: dict) -> list[dict]:
        """Parse the API response into price records.

        Response structure: levels[].arrivals[].departures[]
        Each departure has: price, discount, additional, total, date, nights, etc.

        Returns:
            List of record dicts ready for db.save_price().
        """
        records = []
        levels = data.get("response", data).get("levels", [])

        for level in levels:
            # Filter: only process our target level
            level_ident = str(level.get("ident", ""))
            if level_ident != str(self.level_id):
                continue

            arrivals = level.get("arrivals", [])
            for arrival in arrivals:
                arrival_date_str = arrival.get("date", "")
                if not arrival_date_str:
                    continue

                try:
                    check_in_dt = datetime.strptime(arrival_date_str, "%d-%m-%Y")
                except ValueError:
                    self.logger.warning(f"  Could not parse arrival date: {arrival_date_str}")
                    continue

                for departure in arrival.get("departures", []):
                    try:
                        dep_date_str = departure.get("date", "")
                        nights = int(departure.get("nights", 0))

                        if nights not in TARGET_DURATIONS:
                            continue

                        base_price = departure.get("price")
                        additional = departure.get("additional", 0) or 0
                        discount = departure.get("discount", 0) or 0
                        total_field = departure.get("total")

                        # Determine the price to store
                        if base_price is None and total_field is None:
                            continue

                        if self.add_additional_price:
                            # Accommodation: add extra person surcharge
                            price = (base_price or 0) + additional
                        else:
                            # Camping: price already includes tourist tax
                            price = base_price

                        if price is None or price <= 0:
                            continue

                        check_out_dt = datetime.strptime(dep_date_str, "%d-%m-%Y")

                        # Special offers
                        special_offers = None
                        if discount and discount > 0:
                            special_offers = f"Korting: EUR {discount:.0f}"

                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "check_in_date": check_in_dt.strftime("%Y-%m-%d"),
                            "check_out_date": check_out_dt.strftime("%Y-%m-%d"),
                            "price": round(price, 2),
                            "available": True,
                            "min_nights": nights,
                            "special_offers": special_offers,
                            "persons": self.persons,
                            "segment": self.segment,
                        }
                        records.append(record)

                    except (TypeError, ValueError, KeyError) as e:
                        self.logger.warning(f"  Skipping malformed departure: {e}")
                        continue

        return records

    def scrape_price(self, page, check_in, check_out, persons=4):
        """Not used - we use run_efficient() with the API instead."""
        raise NotImplementedError("Use run_efficient() for De Kleine Wolf")

    def run_efficient(self, months_ahead: int = 12, **kwargs) -> list[dict]:
        """Scrape all prices via the HolidayAgent/TOMM availability API.

        Iterates weekly over the date range. Each request covers ~7 days
        (arrival date +/- 3 days via alternativesDaysBeforeAndAfterArrival).
        With a 2 second delay, ~52 requests take about 2 minutes.
        """
        self.logger.info(
            f"Starting API scrape for {self.competitor_name} "
            f"({self.accommodation_type}, segment={self.segment}): "
            f"{months_ahead} months ahead"
        )

        start_time = time.time()
        all_records = []
        seen_keys = set()  # Deduplicate across overlapping requests
        errors = 0
        request_count = 0

        today = datetime.now().date()
        end_date = today + timedelta(days=months_ahead * 30)
        current_date = today

        while current_date <= end_date:
            arrival_str = current_date.strftime("%d-%m-%Y")
            request_count += 1

            try:
                self._wait_rate_limit()
                data = self._fetch_availability(arrival_str)
                records = self._parse_response(data)

                new_count = 0
                for record in records:
                    key = (
                        record["check_in_date"],
                        record["check_out_date"],
                        record["min_nights"],
                    )
                    if key not in seen_keys:
                        seen_keys.add(key)
                        self.db.save_price(**record)
                        all_records.append(record)
                        new_count += 1

                self.logger.debug(
                    f"  Request {request_count} ({arrival_str}): "
                    f"{len(records)} prices found, {new_count} new"
                )

            except requests.exceptions.RequestException as e:
                errors += 1
                self.logger.warning(
                    f"  Request {request_count} ({arrival_str}) failed: {e}"
                )
            except Exception as e:
                errors += 1
                self.logger.error(
                    f"  Request {request_count} ({arrival_str}) unexpected error: {e}"
                )

            # Step forward 7 days (alternatives=3 gives +/-3 days coverage)
            current_date += timedelta(days=7)

        duration = time.time() - start_time
        status = "success" if errors == 0 else "partial" if all_records else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_records),
            error_message=f"{errors}/{request_count} requests failed" if errors else None,
            duration_seconds=duration,
            segment=self.segment,
        )

        available = [r for r in all_records if r["available"] and r["price"]]
        self.logger.info(
            f"Completed {self.competitor_name} ({self.segment}): "
            f"{len(all_records)} records ({len(available)} available), "
            f"{request_count} requests, {errors} errors, {duration:.1f}s"
        )

        return all_records


class KleineWolfCampingScraper(KleineWolfScraper):
    """Camping De Kleine Wolf - Comfortplaats (kampeerplaats segment)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="De Kleine Wolf",
            accommodation_type="Comfortplaats",
            url="https://www.dekleinewolf.nl/kamperen/comfortplaats",
            level_id="56373",
            segment="kampeerplaats",
            persons=2,
            db=db,
            headless=headless,
            add_additional_price=False,  # Camping: price includes tourist tax
            **kwargs,
        )


class KleineWolfAccScraper(KleineWolfScraper):
    """Camping De Kleine Wolf - Klaverlodge 6p (accommodatie segment).

    Uses persons=2 in the API query to ensure all levels are returned
    (the API filters out levels that don't match the person count).
    The level_id filter in _parse_response ensures only Klaverlodge data is stored.
    add_additional_price=True adds the extra person surcharge to the base price.
    """

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="De Kleine Wolf",
            accommodation_type="Klaverlodge 6p",
            url="https://www.dekleinewolf.nl/verblijven/klaverlodge-6p",
            level_id="56397",
            segment="accommodatie",
            persons=2,  # Use 2 to get all levels; level_id filter selects Klaverlodge
            db=db,
            headless=headless,
            add_additional_price=True,  # Accommodation: add extra person surcharge
            **kwargs,
        )
