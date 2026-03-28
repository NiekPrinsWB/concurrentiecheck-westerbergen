"""Scraper for RCN De Noordster - Bungalows and Camping.

Website uses Maxxton with Nuxt.js SSR. All pricing data is server-side
rendered in __NUXT_DATA__ script tags. We fetch pages via HTTP (no browser
needed) and extract prices from the embedded JSON data.

Each page load for a given arrival date returns prices for ALL available
durations, so we only need one request per unique arrival date.

URL pattern:
  https://www.rcn.nl/nl/vakantieparken/nederland/drenthe/rcn-de-noordster/verhuur/{slug}
    ?arrival={date}T00:00:00&departure={date}T00:00:00

Accommodations:
  - Comfort kampeerplaats (objTypeId 3257) - listing page /kamperen
  - Bungalow Mercurius 6p (objTypeId 2131) - /verhuur/bungalow-mercurius
  - Bungalow Luna 6p (objTypeId 2980) - /verhuur/bungalow-luna
"""

import json
import re
import time
import logging
from datetime import datetime, timedelta

import requests as http_requests

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

# Base URL for RCN De Noordster
BASE_URL = "https://www.rcn.nl/nl/vakantieparken/nederland/drenthe/rcn-de-noordster"


class RcnScraper(BaseScraper):
    """Base scraper for RCN De Noordster accommodations.

    Uses direct HTTP requests to fetch Nuxt SSR pages and extract pricing
    data from __NUXT_DATA__ JSON embedded in the HTML. No browser needed.
    """

    # Subclasses must set these
    ACCOMMODATION_SLUG = ""       # e.g. "bungalow-mercurius"
    ACCOMMODATION_NAME = ""       # e.g. "Bungalow Mercurius 6p"
    URL_PREFIX = "verhuur"        # "verhuur" for bungalows, "kamperen" for camping
    SEGMENT = "accommodatie"
    DEFAULT_PERSONS = 6
    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="RCN De Noordster",
            accommodation_type=self.ACCOMMODATION_NAME,
            url=f"{BASE_URL}/{self.URL_PREFIX}/{self.ACCOMMODATION_SLUG}",
            db=db,
            headless=headless,
            rate_limit=1.5,
            page_timeout=60000,
            **kwargs,
        )
        self.http = http_requests.Session()
        self.http.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
        })

    def _build_url(self, check_in: datetime, check_out: datetime) -> str:
        """Build the URL for a specific accommodation with date parameters."""
        arrival = check_in.strftime("%Y-%m-%dT00:00:00")
        departure = check_out.strftime("%Y-%m-%dT00:00:00")
        return (
            f"{BASE_URL}/{self.URL_PREFIX}/{self.ACCOMMODATION_SLUG}"
            f"?arrival={arrival}&departure={departure}"
        )

    def _extract_nuxt_prices(self, html: str) -> list[dict]:
        """Extract all price records from __NUXT_DATA__ in the raw HTML.

        The Nuxt data array contains objects with pricing information
        like arrivalDate, departureDate, basePriceInclusive, and
        offerPriceInclusive. We extract all of them in one pass.

        Returns list of dicts with keys: check_in, check_out, price, duration
        """
        records = []

        # Find the __NUXT_DATA__ script tag
        match = re.search(
            r'<script\s+type="application/json"\s+id="__NUXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if not match:
            return records

        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            return records

        if not isinstance(data, list):
            return records

        # Strategy: walk the flat array looking for price-related objects.
        # In Nuxt's serialization, objects have string keys followed by
        # index references to their values. We look for elements that
        # contain price keys.

        # First, build a map of all string values and numeric values
        # Then look for patterns: arrivalDate, departureDate, price keys

        # Strategy 1: Look for "basePriceInclusive" or "offerPriceInclusive"
        # and reconstruct objects from nearby indices
        price_key_indices = []
        for i, item in enumerate(data):
            if isinstance(item, str) and item in (
                "basePriceInclusive", "offerPriceInclusive",
                "totalPrice", "price", "basePrice"
            ):
                price_key_indices.append(i)

        # Strategy 2: Look for date strings that match arrival/departure pattern
        # and build price records from surrounding data
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T00:00:00$')
        arrival_indices = {}
        for i, item in enumerate(data):
            if isinstance(item, str) and date_pattern.match(item):
                arrival_indices[i] = item

        # Strategy 3: Directly search for "arrivalDate" keys and reconstruct objects
        for i, item in enumerate(data):
            if item == "arrivalDate" and i + 1 < len(data):
                # Try to reconstruct the object this key belongs to.
                # In Nuxt flat arrays, an object at index X references
                # its keys and values by subsequent indices.
                try:
                    record = self._reconstruct_price_record(data, i)
                    if record:
                        records.append(record)
                except Exception:
                    continue

        # Strategy 4: Brute-force search for price-like numeric values
        # near date-like string values
        if not records:
            records = self._brute_force_extract(data)

        return records

    def _reconstruct_price_record(self, data: list, arrival_key_idx: int) -> dict | None:
        """Try to reconstruct a price record from the Nuxt data array.

        Starting from an "arrivalDate" key, look for related keys
        (departureDate, price fields) in nearby indices.
        """
        # Search a window around the arrivalDate key
        window = 30
        start = max(0, arrival_key_idx - 5)
        end = min(len(data), arrival_key_idx + window)

        record = {}
        for i in range(start, end):
            item = data[i]
            if not isinstance(item, str):
                continue

            # Look for the value at the next index (or referenced index)
            val_idx = i + 1
            if val_idx >= len(data):
                continue
            val = data[val_idx]

            # If val is an integer, it might be an index reference
            if isinstance(val, int) and 0 <= val < len(data):
                dereferenced = data[val]
                # Use dereferenced value if it looks like a date or number
                if isinstance(dereferenced, str) and 'T' in dereferenced:
                    val = dereferenced
                elif isinstance(dereferenced, (int, float)) and dereferenced > 10:
                    val = dereferenced

            if item == "arrivalDate" and isinstance(val, str):
                record["arrivalDate"] = val
            elif item == "departureDate" and isinstance(val, str):
                record["departureDate"] = val
            elif item == "duration" and isinstance(val, (int, float)):
                record["duration"] = int(val)
            elif item == "offerPriceInclusive" and isinstance(val, (int, float)):
                record["offerPrice"] = float(val)
            elif item == "basePriceInclusive" and isinstance(val, (int, float)):
                record["basePrice"] = float(val)

        # Validate we have enough data
        if "arrivalDate" not in record:
            return None

        price = record.get("offerPrice") or record.get("basePrice")
        if not price or price <= 0:
            return None

        try:
            check_in = datetime.strptime(
                record["arrivalDate"][:10], "%Y-%m-%d"
            )
        except ValueError:
            return None

        if "departureDate" in record:
            try:
                check_out = datetime.strptime(
                    record["departureDate"][:10], "%Y-%m-%d"
                )
                duration = (check_out - check_in).days
            except ValueError:
                duration = record.get("duration", 0)
                check_out = check_in + timedelta(days=duration)
        elif "duration" in record:
            duration = record["duration"]
            check_out = check_in + timedelta(days=duration)
        else:
            return None

        if duration not in self.STAY_DURATIONS:
            return None

        return {
            "check_in": check_in,
            "check_out": check_out,
            "price": price,
            "duration": duration,
        }

    def _brute_force_extract(self, data: list) -> list[dict]:
        """Fallback: search for EUR price patterns in string elements."""
        records = []
        for i, item in enumerate(data):
            if isinstance(item, str):
                match = re.search(r'€\s*([\d.,]+)', item)
                if match:
                    price_str = match.group(1).replace('.', '').replace(',', '.')
                    try:
                        price = float(price_str)
                        if 10 < price < 50000:
                            # Can't determine dates from this, skip
                            pass
                    except ValueError:
                        continue
        return records

    def _check_unavailable(self, html: str) -> bool:
        """Check if the page indicates the accommodation is unavailable."""
        lower = html.lower()
        return any(p in lower for p in [
            'niet beschikbaar', 'not available', 'uitverkocht',
            'sold out', 'geen beschikbaarheid',
        ])

    def _extract_price_from_html(self, html: str) -> float | None:
        """Fallback: extract price from rendered HTML using regex."""
        matches = re.findall(r'€\s*([\d.,]+)', html)
        if not matches:
            return None

        prices = []
        for m in matches:
            raw = m.replace('.', '').replace(',', '.')
            try:
                p = float(raw)
                if 10 < p < 50000 and p != 35:  # Filter €35 pitch fee
                    prices.append(p)
            except ValueError:
                continue

        return min(prices) if prices else None

    def scrape_price(self, page, check_in, check_out, persons=6):
        """Not used - we use run_efficient() with HTTP requests instead."""
        raise NotImplementedError("Use run_efficient() for RCN sites")

    def _generate_date_pairs(self, months_ahead: int = 12) -> list[dict]:
        """Generate weekly arrival dates with all stay durations.

        Creates date pairs at 7-day intervals (Fridays and Mondays)
        for the next N months.
        """
        today = datetime.now().date()
        end_date = today + timedelta(days=months_ahead * 30)

        date_pairs = []
        seen = set()
        current = today

        while current <= end_date:
            # Find next Friday
            days_until_friday = (4 - current.weekday()) % 7
            friday = current + timedelta(days=days_until_friday)
            if friday < today:
                friday += timedelta(days=7)

            # Find next Monday
            days_until_monday = (0 - current.weekday()) % 7
            monday = current + timedelta(days=days_until_monday)
            if monday < today:
                monday += timedelta(days=7)

            for duration in self.STAY_DURATIONS:
                # Friday arrivals: 2n (weekend), 3n, 7n
                if duration in (2, 3, 7) and friday <= end_date:
                    key = (friday, duration)
                    if key not in seen:
                        seen.add(key)
                        date_pairs.append({
                            "check_in": friday,
                            "check_out": friday + timedelta(days=duration),
                            "nights": duration,
                        })
                # Monday arrivals: 4n (midweek)
                if duration == 4 and monday <= end_date:
                    key = (monday, duration)
                    if key not in seen:
                        seen.add(key)
                        date_pairs.append({
                            "check_in": monday,
                            "check_out": monday + timedelta(days=duration),
                            "nights": duration,
                        })

            current += timedelta(days=7)

        date_pairs.sort(key=lambda x: (x["check_in"], x["nights"]))
        return date_pairs

    def run_efficient(self, months_ahead: int = 12, persons: int = None,
                      **kwargs) -> list[dict]:
        """Scrape all prices via direct HTTP requests (no browser needed).

        For each (arrival_date, duration) pair, fetches the accommodation
        page via HTTP and extracts the price from rendered HTML.
        ~5x faster than the Playwright approach (no browser overhead,
        no 3s hydration sleep).
        """
        if persons is None:
            persons = self.DEFAULT_PERSONS

        date_pairs = self._generate_date_pairs(months_ahead)

        self.logger.info(
            f"Starting HTTP-based scrape for {self.competitor_name} - "
            f"{self.accommodation_type}: {len(date_pairs)} date combinations, "
            f"{months_ahead} months ahead"
        )

        start_time = time.time()
        all_records = []
        seen_keys = set()
        errors = 0
        consecutive_failures = 0
        max_consecutive_failures = 10

        # First request: establish session cookies
        try:
            self._wait_rate_limit()
            resp = self.http.get(self.url, timeout=30)
            resp.raise_for_status()
            self.logger.info("  Session established via base page")
        except Exception as e:
            self.logger.warning(f"  Base page failed (continuing): {e}")

        for i, dp in enumerate(date_pairs):
            check_in = dp["check_in"]
            check_out = dp["check_out"]
            nights = dp["nights"]

            key = (check_in.isoformat(), check_out.isoformat())
            if key in seen_keys:
                continue

            # Log progress every 25 dates
            if i > 0 and i % 25 == 0:
                elapsed = time.time() - start_time
                self.logger.info(
                    f"  Progress: {i}/{len(date_pairs)} dates, "
                    f"{len(all_records)} prices, {elapsed:.0f}s elapsed"
                )

            self._wait_rate_limit()

            check_in_dt = datetime.combine(check_in, datetime.min.time())
            check_out_dt = datetime.combine(check_out, datetime.min.time())
            url = self._build_url(check_in_dt, check_out_dt)

            try:
                resp = self.http.get(url, timeout=30)

                if resp.status_code == 404:
                    continue
                resp.raise_for_status()

                html = resp.text
                consecutive_failures = 0

                # Check unavailability
                unavailable = self._check_unavailable(html)

                if not unavailable:
                    # Extract price from HTML (primary method for SSR pages)
                    price = self._extract_price_from_html(html)

                    if price:
                        seen_keys.add(key)
                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "check_in_date": check_in.isoformat(),
                            "check_out_date": check_out.isoformat(),
                            "price": price,
                            "available": True,
                            "min_nights": nights,
                            "special_offers": None,
                            "persons": persons,
                            "segment": self.SEGMENT,
                        }
                        self.db.save_price(**record)
                        all_records.append(record)

                        self.logger.info(
                            f"  {check_in} -> {check_out} ({nights}n): "
                            f"EUR {price:.0f} (beschikbaar)"
                        )
                    else:
                        # Try NUXT data as fallback
                        nuxt_records = self._extract_nuxt_prices(html)
                        for pr in nuxt_records:
                            nkey = (
                                pr["check_in"].strftime("%Y-%m-%d"),
                                pr["check_out"].strftime("%Y-%m-%d"),
                            )
                            if nkey not in seen_keys:
                                seen_keys.add(nkey)
                                record = {
                                    "competitor_name": self.competitor_name,
                                    "accommodation_type": self.accommodation_type,
                                    "check_in_date": pr["check_in"].strftime("%Y-%m-%d"),
                                    "check_out_date": pr["check_out"].strftime("%Y-%m-%d"),
                                    "price": pr["price"],
                                    "available": True,
                                    "min_nights": pr["duration"],
                                    "special_offers": None,
                                    "persons": persons,
                                    "segment": self.SEGMENT,
                                }
                                self.db.save_price(**record)
                                all_records.append(record)

                        if nuxt_records:
                            self.logger.info(
                                f"  {check_in} ({nights}n): "
                                f"{len(nuxt_records)} prices from NUXT data"
                            )
                else:
                    seen_keys.add(key)
                    record = {
                        "competitor_name": self.competitor_name,
                        "accommodation_type": self.accommodation_type,
                        "check_in_date": check_in.isoformat(),
                        "check_out_date": check_out.isoformat(),
                        "price": None,
                        "available": False,
                        "min_nights": nights,
                        "special_offers": None,
                        "persons": persons,
                        "segment": self.SEGMENT,
                    }
                    self.db.save_price(**record)
                    all_records.append(record)

            except http_requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    self.logger.warning("  Rate limited, waiting 10s...")
                    time.sleep(10)
                    consecutive_failures += 1
                else:
                    errors += 1
                    consecutive_failures += 1
                    self.logger.warning(f"  HTTP error for {check_in}: {e}")
            except Exception as e:
                errors += 1
                consecutive_failures += 1
                self.logger.warning(f"  Error for {check_in}: {e}")

            if consecutive_failures >= max_consecutive_failures:
                self.logger.error(
                    f"  {max_consecutive_failures} consecutive failures, stopping."
                )
                break

        duration = time.time() - start_time
        status = "success" if errors == 0 else "partial" if all_records else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_records),
            error_message=f"{errors} date(s) failed" if errors else None,
            duration_seconds=duration,
            segment=self.SEGMENT,
        )

        available = [r for r in all_records if r["available"] and r["price"]]
        self.logger.info(
            f"Completed {self.competitor_name} - {self.accommodation_type}: "
            f"{len(all_records)} records ({len(available)} available with price), "
            f"{errors} errors, {duration:.1f}s"
        )

        return all_records


# ---------------------------------------------------------------------------
# Concrete subclasses for each RCN De Noordster accommodation
# ---------------------------------------------------------------------------

class RcnNoordsterMercuriusScraper(RcnScraper):
    """RCN De Noordster - Bungalow Mercurius 6p (standard segment B)."""

    ACCOMMODATION_SLUG = "bungalow-mercurius"
    ACCOMMODATION_NAME = "Bungalow Mercurius 6p"
    URL_PREFIX = "verhuur"
    SEGMENT = "accommodatie"
    DEFAULT_PERSONS = 6

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(db=db, headless=headless, **kwargs)


class RcnNoordsterLunaScraper(RcnScraper):
    """RCN De Noordster - Bungalow Luna 6p (premium segment B)."""

    ACCOMMODATION_SLUG = "bungalow-luna"
    ACCOMMODATION_NAME = "Bungalow Luna 6p"
    URL_PREFIX = "verhuur"
    SEGMENT = "accommodatie"
    DEFAULT_PERSONS = 6

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(db=db, headless=headless, **kwargs)


class RcnNoordsterCampingScraper(RcnScraper):
    """RCN De Noordster - Comfort kampeerplaats (camping segment A)."""

    ACCOMMODATION_SLUG = "comfort-kampeerplaats"
    ACCOMMODATION_NAME = "Comfort kampeerplaats"
    URL_PREFIX = "kamperen"
    SEGMENT = "kampeerplaats"
    DEFAULT_PERSONS = 2
    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(db=db, headless=headless, **kwargs)
