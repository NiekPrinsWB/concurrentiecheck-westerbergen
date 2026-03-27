"""Scraper for Landal GreenParks parks via their REST API.

Uses two endpoints:
1. /arrivaldates/get — returns available arrival dates with durations per month
2. /parksAvailabilities/search — returns accommodation prices for a given stay

Session cookies are obtained by loading the park's pricing page first.
No browser needed — pure HTTP with requests.

Used by: Landal Aelderholt, Landal Het Land van Bartje
"""

import time
import logging
from datetime import datetime, timedelta

import requests

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

# stayType mapping: determines which duration bucket to query
STAY_TYPE_SHORT = "915"   # 2-4 night stays (weekend/midweek)
STAY_TYPE_WEEK = "968"    # 6-8 night stays (week)

# Map stayType to the durations it covers
STAY_TYPE_DURATIONS = {
    STAY_TYPE_SHORT: [2, 3, 4],
    STAY_TYPE_WEEK: [7],
}


class LandalScraper(BaseScraper):
    """Base scraper for Landal GreenParks parks via REST API.

    Uses the Landal website API directly (no browser needed).
    One session request + arrivaldates call + search calls per date/duration.
    """

    SESSION_URL_TEMPLATE = "https://www.landal.nl/parken/{park_slug}/prijzen-en-beschikbaarheid"
    ARRIVALS_URL = "https://www.landal.nl/nl/api/arrivaldates/get"
    SEARCH_URL = "https://www.landal.nl/nl/api/destinations/parksAvailabilities/search"

    def __init__(self, competitor_name: str, accommodation_type: str,
                 park_code: str, park_slug: str, target_acc_code: str,
                 db: Database, headless: bool = True,
                 segment: str = "accommodatie", **kwargs):
        super().__init__(
            competitor_name=competitor_name,
            accommodation_type=accommodation_type,
            url=self.SESSION_URL_TEMPLATE.format(park_slug=park_slug),
            db=db,
            headless=headless,
            rate_limit=0.5,  # 0.5 second between requests
            **kwargs,
        )
        self.park_code = park_code
        self.park_slug = park_slug
        self.target_acc_code = target_acc_code
        self.segment = segment
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Referer": f"https://www.landal.nl/parken/{park_slug}/prijzen-en-beschikbaarheid",
            "Origin": "https://www.landal.nl",
        })

    def _init_session(self):
        """Load the main Landal site to get session cookies.

        The park-specific pricing URL may 404 but the API works without it.
        We just need the session cookies from any Landal page.
        """
        self.logger.info("  Initializing session (loading Landal homepage for cookies)...")
        resp = self.session.get("https://www.landal.nl/", timeout=30)
        resp.raise_for_status()
        self.logger.info(f"  Session initialized, got {len(self.session.cookies)} cookies")

    def _fetch_arrival_dates(self, stay_type: str) -> list[dict]:
        """Fetch available arrival dates and durations for a stayType.

        Returns list of dicts: {"date": "dd-mm-yyyy", "durations": [7, ...]}
        """
        params = {
            "selectedParkCode": self.park_code,
            "searchType": "3",
            "stayType": stay_type,
            "accommodationType": self.target_acc_code,
        }
        self.logger.debug(f"  Fetching arrival dates: stayType={stay_type}, accType={self.target_acc_code}")
        resp = self.session.get(self.ARRIVALS_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Parse response: months with available days and durations
        arrival_dates = []

        # Actual Landal format: {"arrivalDates": [{"Year", "Month", "DaysWithCount": [{"Day", "Durations"}]}]}
        months = data.get("arrivalDates", []) if isinstance(data, dict) else data

        # Fallback: try other known keys
        if not months and isinstance(data, dict):
            for key in ("months", "data"):
                if key in data and isinstance(data[key], list):
                    months = data[key]
                    break

        for month_data in months:
            if not isinstance(month_data, dict):
                continue

            # Landal format: Year + Month + DaysWithCount
            year = month_data.get("Year")
            month = month_data.get("Month")
            days_with_count = month_data.get("DaysWithCount", [])

            if year and month and days_with_count:
                # Landal-specific structure
                for day_entry in days_with_count:
                    if not isinstance(day_entry, dict):
                        continue
                    day_num = day_entry.get("Day", "")
                    durations = day_entry.get("Durations", [])
                    if day_num and durations:
                        date_str = f"{day_num}-{month}-{year}"
                        arrival_dates.append({
                            "date": date_str,
                            "durations": durations,
                        })
                continue

            # Generic fallback structure
            days = month_data.get("days", month_data.get("arrivalDays", []))
            for day in days:
                if isinstance(day, dict):
                    date_str = day.get("date", day.get("arrivalDate", ""))
                    durations = day.get("durations", day.get("stayDurations", []))
                    if date_str and durations:
                        arrival_dates.append({
                            "date": date_str,
                            "durations": durations,
                        })
                elif isinstance(day, str):
                    # Just a date string, durations come from stayType
                    arrival_dates.append({
                        "date": day,
                        "durations": STAY_TYPE_DURATIONS.get(stay_type, [7]),
                    })

        self.logger.info(
            f"  stayType {stay_type}: {len(arrival_dates)} arrival dates available"
        )
        return arrival_dates

    def _search_prices(self, arrival_date: str, departure_date: str,
                       stay_type: str, persons: int = 6) -> list[dict]:
        """Search for accommodation prices for a specific date range.

        Returns list of accommodation results. With accommodationType filter,
        typically returns only a few results so no pagination needed.
        """
        form_data = {
            "selectedParkCode": self.park_code,
            "searchType": "3",
            "arrivalDate": arrival_date,
            "departureDate": departure_date,
            "stayType": stay_type,
            "paginationOffset": "0",
            "numberOfGuests": str(persons),
            "accommodationType": self.target_acc_code,
        }

        resp = self.session.post(
            self.SEARCH_URL,
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("Success", False):
            return []

        return data.get("searchResult", {}).get("accommodations", [])

    def _parse_accommodation(self, acc: dict) -> dict | None:
        """Parse a single accommodation result into a price record.

        Returns dict with check_in, check_out, price, nights, special_offers
        or None if not the target accommodation.
        """
        acc_info = acc.get("AccommodationInfo", {})
        price_info = acc.get("PriceInfo", {})
        stay_duration = acc.get("stayDuration", {})

        acc_code = acc_info.get("Code", "")
        if acc_code != self.target_acc_code:
            return None

        # Use bestRentalPriceInEuros (base rental, comparable to competitors)
        price = price_info.get("bestRentalPriceInEuros")
        if price is None:
            return None

        try:
            price = float(price)
        except (TypeError, ValueError):
            return None

        # Parse dates from ISO response format
        arrival_str = stay_duration.get("arrivalDate", "")
        departure_str = stay_duration.get("departureDate", "")
        nights = stay_duration.get("numberOfNights", 0)

        try:
            check_in = datetime.fromisoformat(arrival_str.replace("Z", "+00:00"))
            check_out = datetime.fromisoformat(departure_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

        # Check for discounts (formattedPrice vs bestRentalPriceInEuros)
        special_offers = None
        formatted_total = price_info.get("bestTotalPriceInCents")
        if formatted_total:
            total_eur = formatted_total / 100.0
            if total_eur > price * 1.01:  # >1% difference means surcharges
                special_offers = f"Totaalprijs incl. toeslagen: EUR {total_eur:.0f}"

        return {
            "check_in": check_in,
            "check_out": check_out,
            "price": price,
            "nights": nights,
            "special_offers": special_offers,
        }

    def scrape_price(self, page, check_in, check_out, persons=6):
        """Not used — we use run_efficient() with the API instead."""
        raise NotImplementedError("Use run_efficient() for Landal sites")

    def run_efficient(self, months_ahead: int = 12, persons: int = 6,
                      **kwargs) -> list[dict]:
        """Scrape all prices via the Landal REST API.

        Workflow:
        1. Load park page for session cookies
        2. Fetch arrival dates for each stayType
        3. For each arrival date + duration, search for prices
        4. Filter for target accommodation code
        5. Save to database
        """
        self.logger.info(
            f"Starting API scrape for {self.competitor_name} "
            f"(park={self.park_code}, acc={self.target_acc_code})"
        )

        start_time = time.time()
        all_records = []
        errors = 0
        seen_stays = set()  # Avoid duplicate (check_in, check_out) combos

        try:
            # Step 1: Initialize session
            self._init_session()
            time.sleep(2)

            # Step 2-3: For each stayType, fetch arrivals and search prices
            for stay_type, target_durations in STAY_TYPE_DURATIONS.items():
                self._wait_rate_limit()

                try:
                    arrival_dates = self._fetch_arrival_dates(stay_type)
                except Exception as e:
                    self.logger.error(
                        f"  Failed to fetch arrival dates for stayType {stay_type}: {e}"
                    )
                    errors += 1
                    continue

                # Build unique (date, duration) pairs to search
                search_pairs = []
                for arrival_info in arrival_dates:
                    date_str = arrival_info["date"]
                    durations = arrival_info["durations"]

                    try:
                        check_in_dt = datetime.strptime(date_str, "%d-%m-%Y")
                    except ValueError:
                        try:
                            check_in_dt = datetime.strptime(date_str, "%Y-%m-%d")
                        except ValueError:
                            continue

                    for dur in durations:
                        try:
                            dur = int(dur)
                        except (TypeError, ValueError):
                            continue
                        if dur not in target_durations:
                            continue
                        check_out_dt = check_in_dt + timedelta(days=dur)
                        stay_key = (check_in_dt.date(), check_out_dt.date())
                        if stay_key not in seen_stays:
                            search_pairs.append((check_in_dt, check_out_dt, dur, stay_key))

                self.logger.info(
                    f"  stayType {stay_type}: {len(search_pairs)} date/duration combos to search"
                )

                for check_in_dt, check_out_dt, dur, stay_key in search_pairs:
                    if stay_key in seen_stays:
                        continue

                    arr_str = check_in_dt.strftime("%d-%m-%Y")
                    dep_str = check_out_dt.strftime("%d-%m-%Y")

                    self._wait_rate_limit()

                    try:
                        accommodations = self._search_prices(
                            arr_str, dep_str, stay_type, persons
                        )
                    except Exception as e:
                        self.logger.warning(
                            f"  Search failed for {arr_str}->{dep_str}: {e}"
                        )
                        errors += 1
                        continue

                    # Filter for our target accommodation
                    found = False
                    for acc in accommodations:
                        parsed = self._parse_accommodation(acc)
                        if parsed is None:
                            continue

                        found = True
                        seen_stays.add(stay_key)

                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "check_in_date": parsed["check_in"].strftime("%Y-%m-%d"),
                            "check_out_date": parsed["check_out"].strftime("%Y-%m-%d"),
                            "price": parsed["price"],
                            "available": True,
                            "min_nights": parsed["nights"],
                            "special_offers": parsed["special_offers"],
                            "persons": persons,
                            "segment": self.segment,
                        }
                        self.db.save_price(**record)
                        all_records.append(record)

                        self.logger.debug(
                            f"  {arr_str} -> {dep_str} ({dur}n): "
                            f"EUR {parsed['price']:.0f}"
                        )
                        break  # Only need first match per stay

                    if not found:
                        # Accommodation not available for this date
                        self.logger.debug(
                            f"  {arr_str} -> {dep_str} ({dur}n): "
                            f"not available (sold out or not offered)"
                        )

        except Exception as e:
            errors += 1
            self.logger.error(f"  Scraping failed: {e}", exc_info=True)

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

        self.logger.info(
            f"Completed {self.competitor_name}: {len(all_records)} records, "
            f"{errors} errors, {duration_s:.1f}s"
        )

        return all_records


class LandalAelderholtScraper(LandalScraper):
    """Landal Aelderholt — 6-persoons bungalow (6CE)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Landal Aelderholt",
            accommodation_type="6-persoons bungalow 6CE (6p)",
            park_code="AHT",
            park_slug="aelderholt",
            target_acc_code="6CE",
            db=db,
            headless=headless,
            segment="accommodatie",
            **kwargs,
        )


class LandalAelderholtPremiumScraper(LandalScraper):
    """Landal Aelderholt — 6-persoons kinderwoning (6ELK)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Landal Aelderholt",
            accommodation_type="6-persoons kinderwoning 6ELK (6p)",
            park_code="AHT",
            park_slug="aelderholt",
            target_acc_code="6ELK",
            db=db,
            headless=headless,
            segment="accommodatie",
            **kwargs,
        )


class LandalBartjeScraper(LandalScraper):
    """Landal Het Land van Bartje — Comfort 6p standard (6D5)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Landal Het Land van Bartje",
            accommodation_type="Comfort bungalow 6D5 (6p)",
            park_code="LJE",
            park_slug="het-land-van-bartje",
            target_acc_code="6D5",
            db=db,
            headless=headless,
            segment="accommodatie",
            **kwargs,
        )
