"""Scraper for Capfun holiday parks using the Sequoiasoft/Thelis REST API.

The booking engine at reserveren.capfun.com exposes a Search/search endpoint
that returns available products with prices for a given arrival date and duration.

The API requires a PHPSESSID session cookie, obtained by visiting the booking
page. All endpoints are POST with JSON body.

Used by: Het Stoetenslagh, De Sprookjescamping, De Fruithof
"""

import time
import logging
from datetime import datetime, timedelta

import requests

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

# Canonical durations to scrape
DURATIONS = [2, 3, 4, 7]

# Step size between arrival dates (days)
DATE_STEP = 3


class CapfunScraper(BaseScraper):
    """Base scraper for Capfun parks via Sequoiasoft/Thelis REST API.

    Uses requests (no browser needed). One Search call per
    (arrival_date, duration) combination; each call returns multiple
    products with nearby stays, giving good date coverage.
    """

    BOOKING_BASE = "https://reserveren.capfun.com"

    def __init__(self, competitor_name: str, accommodation_type: str,
                 camping_param: str, product_type: int | str,
                 segment: str, persons: int, db: Database,
                 headless: bool = True, capacity_min: int = 0, **kwargs):
        """
        Args:
            competitor_name: Name for database storage.
            accommodation_type: Accommodation description for database.
            camping_param: The camping identifier in the Capfun URL (e.g. 'capfunstoetenslagh').
            product_type: 1 = camping only, 2 = accommodation only, "both" = all.
            segment: 'kampeerplaats' or 'accommodatie'.
            persons: Number of persons for search (2 for camping, 4-6 for accommodation).
            db: Database instance.
            capacity_min: Minimum capacity filter for products (0 = any).
        """
        url = f"{self.BOOKING_BASE}/?camping={camping_param}&lang=nl"
        super().__init__(
            competitor_name=competitor_name,
            accommodation_type=accommodation_type,
            url=url,
            db=db,
            headless=headless,
            rate_limit=2.5,
            **kwargs,
        )
        self.camping_param = camping_param
        self.product_type = product_type
        self.segment = segment
        self.persons = persons
        self.capacity_min = capacity_min
        self.session_id = None
        self.http = requests.Session()
        self.http.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": self.BOOKING_BASE,
            "Referer": url,
        })

    def _get_session(self):
        """Obtain a PHPSESSID by loading the booking page in Playwright.

        The Capfun site is an Angular SPA that sets PHPSESSID via JavaScript,
        so plain HTTP requests don't receive the cookie.
        """
        self.logger.info(f"  Obtaining session for {self.camping_param} via browser...")
        from playwright.sync_api import sync_playwright

        url = f"{self.BOOKING_BASE}/?camping={self.camping_param}&lang=nl"
        session_id = None

        import re
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Wait for Angular to initialize and redirect with PHPSESSID in URL
                page.wait_for_timeout(5000)
                # PHPSESSID appears in the URL, not as a cookie
                current_url = page.url
                match = re.search(r"PHPSESSID=([a-zA-Z0-9]+)", current_url)
                if match:
                    session_id = match.group(1)
                else:
                    # Fallback: check cookies
                    for cookie in context.cookies():
                        if cookie["name"] == "PHPSESSID":
                            session_id = cookie["value"]
                            break
            finally:
                browser.close()

        if not session_id:
            raise RuntimeError(
                f"Could not obtain PHPSESSID for {self.camping_param} via browser."
            )

        self.session_id = session_id
        # Also set cookie on HTTP session for API calls
        self.http.cookies.set("PHPSESSID", session_id, domain="reserveren.capfun.com")
        self.logger.info(f"  Session: {session_id[:8]}...")

    def _api_url(self, service: str) -> str:
        """Build the API URL for a given service endpoint."""
        return (
            f"{self.BOOKING_BASE}/2017/services/{service}"
            f"?camping={self.camping_param}&lang=nl&PHPSESSID={self.session_id}"
        )

    def _search(self, begin_date: str, duration: int) -> dict:
        """Call Search/search for a specific arrival date and duration.

        Args:
            begin_date: Arrival date in YYYY-MM-DD format.
            duration: Stay duration in nights.

        Returns:
            Raw API response dict.
        """
        end_date_dt = datetime.strptime(begin_date, "%Y-%m-%d") + timedelta(days=duration)
        end_date = end_date_dt.strftime("%Y-%m-%d")

        body = {
            "dates": {
                "begin": begin_date,
                "end": end_date,
            },
            "duration": duration,
            "type": self.product_type,
            "persons": {
                "adults": self.persons,
                "children": 0,
                "babies": 0,
            },
            "criteria": [],
            "global_criteria": {},
        }

        resp = self.http.post(
            self._api_url("Search/search"),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _extract_prices(self, data: dict, duration: int) -> list[dict]:
        """Parse the Search response and extract price records.

        Returns list of dicts ready for save_price().
        """
        records = []
        results_outer = data.get("results", [])

        for result_group in results_outer:
            inner_results = result_group.get("results", [])
            for result in inner_results:
                products = result.get("products", [])
                for product in products:
                    prod_info = product.get("product", {})
                    prod_name = prod_info.get("name", "")
                    prod_type = prod_info.get("type")
                    try:
                        capacity = int(prod_info.get("capacity", 0) or 0)
                    except (TypeError, ValueError):
                        capacity = 0

                    # Filter by capacity
                    if self.capacity_min > 0 and capacity < self.capacity_min:
                        continue

                    stays = product.get("stays", [])
                    for stay in stays:
                        try:
                            price = stay.get("price")
                            if price is None:
                                continue
                            price = float(price)
                            if price <= 0:
                                continue

                            stay_begin = stay.get("begin", "")
                            stay_end = stay.get("end", "")
                            try:
                                stay_duration = int(stay.get("duration", duration))
                            except (TypeError, ValueError):
                                stay_duration = duration

                            # Only keep canonical durations
                            if stay_duration not in DURATIONS:
                                continue

                            # Parse dates (API returns YYYY-MM-DD)
                            check_in_dt = datetime.strptime(stay_begin, "%Y-%m-%d")
                            check_out_dt = datetime.strptime(stay_end, "%Y-%m-%d")

                            records.append({
                                "check_in": check_in_dt,
                                "check_out": check_out_dt,
                                "price": price,
                                "duration": stay_duration,
                                "product_name": prod_name,
                                "product_type": prod_type,
                                "capacity": capacity,
                            })
                        except (ValueError, TypeError) as e:
                            self.logger.debug(
                                f"  Skipping malformed stay in '{prod_name}': {e}"
                            )
                            continue

        return records

    def _select_best_product(self, records: list[dict]) -> list[dict]:
        """Select the representative product from all returned records.

        For accommodations (type=2): pick the cheapest 6+ person product per date/duration.
        For camping (type=1): pick the standard kampeerplaats per date/duration.
        """
        # Group by (check_in, check_out)
        grouped: dict[tuple, list[dict]] = {}
        for r in records:
            key = (r["check_in"], r["check_out"])
            grouped.setdefault(key, []).append(r)

        best = []
        for key, candidates in grouped.items():
            if self.product_type == 2:
                # Accommodation: cheapest product with capacity >= 6
                filtered = [c for c in candidates if int(c.get("capacity") or 0) >= 6]
                if not filtered:
                    filtered = candidates
                chosen = min(filtered, key=lambda x: x["price"])
            else:
                # Camping: prefer "Staanplaats" or "Kampeerplaats" in name
                camping_options = [
                    c for c in candidates
                    if any(kw in (c.get("product_name") or "").lower()
                           for kw in ("staanplaats", "kampeerplaats", "emplacement"))
                ]
                if camping_options:
                    chosen = min(camping_options, key=lambda x: x["price"])
                else:
                    chosen = min(candidates, key=lambda x: x["price"])
            best.append(chosen)

        return best

    def scrape_price(self, page, check_in, check_out, persons=4):
        """Not used - we use run_efficient() with the API instead."""
        raise NotImplementedError("Use run_efficient() for Capfun sites")

    def run_efficient(self, months_ahead: int = 12, **kwargs) -> list[dict]:
        """Scrape all prices via the Capfun Sequoiasoft REST API.

        Iterates over arrival dates (every DATE_STEP days) for the next
        months_ahead months, calling Search/search for each combination
        of date and duration.
        """
        self.logger.info(
            f"Starting API scrape for {self.competitor_name} "
            f"(park={self.camping_param}, type={self.product_type}, "
            f"segment={self.segment}, persons={self.persons})"
        )

        start_time = time.time()
        all_raw_records = []
        all_saved_records = []
        errors = 0
        request_count = 0

        try:
            self._get_session()

            # Generate arrival dates for the next N months
            today = datetime.now().date()
            end_date = today + timedelta(days=months_ahead * 30)

            arrival_dates = []
            current = today + timedelta(days=1)  # Start from tomorrow
            while current <= end_date:
                arrival_dates.append(current)
                current += timedelta(days=DATE_STEP)

            total_requests = len(arrival_dates) * len(DURATIONS)
            self.logger.info(
                f"  {len(arrival_dates)} arrival dates x {len(DURATIONS)} durations "
                f"= {total_requests} API calls"
            )

            for i, arrival in enumerate(arrival_dates):
                for duration in DURATIONS:
                    request_count += 1
                    date_str = arrival.strftime("%Y-%m-%d")

                    try:
                        self._wait_rate_limit()
                        data = self._search(date_str, duration)
                        raw = self._extract_prices(data, duration)
                        all_raw_records.extend(raw)

                        if request_count % 25 == 0:
                            self.logger.info(
                                f"  Progress: {request_count}/{total_requests} requests, "
                                f"{len(all_raw_records)} raw records so far"
                            )

                    except requests.exceptions.HTTPError as e:
                        if e.response is not None and e.response.status_code == 429:
                            self.logger.warning(
                                f"  Rate limited at request {request_count}, "
                                f"waiting 10s..."
                            )
                            time.sleep(10)
                            # Retry once
                            try:
                                data = self._search(date_str, duration)
                                raw = self._extract_prices(data, duration)
                                all_raw_records.extend(raw)
                            except Exception as retry_e:
                                errors += 1
                                self.logger.error(
                                    f"  Retry failed for {date_str}/{duration}n: {retry_e}"
                                )
                        elif e.response is not None and e.response.status_code == 403:
                            self.logger.warning(
                                f"  Session expired at request {request_count}, "
                                f"refreshing..."
                            )
                            try:
                                self._get_session()
                                data = self._search(date_str, duration)
                                raw = self._extract_prices(data, duration)
                                all_raw_records.extend(raw)
                            except Exception as retry_e:
                                errors += 1
                                self.logger.error(
                                    f"  Session refresh failed: {retry_e}"
                                )
                        else:
                            errors += 1
                            self.logger.warning(
                                f"  HTTP error for {date_str}/{duration}n: {e}"
                            )
                    except Exception as e:
                        errors += 1
                        self.logger.warning(
                            f"  Error for {date_str}/{duration}n: {e}"
                        )

            # Deduplicate and select best product per (check_in, check_out)
            self.logger.info(
                f"  Raw records: {len(all_raw_records)}, deduplicating..."
            )
            best_records = self._select_best_product(all_raw_records)
            self.logger.info(
                f"  After dedup/selection: {len(best_records)} records"
            )

            # Save to database
            for rec in best_records:
                try:
                    saved = {
                        "competitor_name": self.competitor_name,
                        "accommodation_type": self.accommodation_type,
                        "check_in_date": rec["check_in"].strftime("%Y-%m-%d"),
                        "check_out_date": rec["check_out"].strftime("%Y-%m-%d"),
                        "price": rec["price"],
                        "available": True,
                        "min_nights": rec["duration"],
                        "special_offers": None,
                        "persons": self.persons,
                        "segment": self.segment,
                    }
                    self.db.save_price(**saved)
                    all_saved_records.append(saved)
                except Exception as e:
                    self.logger.warning(f"  Failed to save record: {e}")

        except Exception as e:
            errors += 1
            self.logger.error(f"  Scraping failed: {e}", exc_info=True)

        duration_s = time.time() - start_time
        status = "success" if errors == 0 else "partial" if all_saved_records else "failed"

        self.db.log_scrape(
            competitor_name=self.competitor_name,
            status=status,
            records_scraped=len(all_saved_records),
            error_message=f"{errors} errors" if errors else None,
            duration_seconds=duration_s,
            segment=self.segment,
        )

        self.logger.info(
            f"Completed {self.competitor_name} ({self.segment}): "
            f"{len(all_saved_records)} records saved, "
            f"{errors} errors, {duration_s:.1f}s"
        )

        return all_saved_records


# ---------------------------------------------------------------------------
# Subclasses per park + segment
# ---------------------------------------------------------------------------

class CapfunStoetenslaghCampingScraper(CapfunScraper):
    """Het Stoetenslagh - Kampeerplaats (camping segment)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Capfun Het Stoetenslagh",
            accommodation_type="Kampeerplaats",
            camping_param="capfunstoetenslagh",
            product_type=1,
            segment="kampeerplaats",
            persons=2,
            db=db,
            headless=headless,
            **kwargs,
        )


class CapfunStoetenslaghAccScraper(CapfunScraper):
    """Het Stoetenslagh - Accommodatie (6+ personen)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Capfun Het Stoetenslagh",
            accommodation_type="Accommodatie (6p)",
            camping_param="capfunstoetenslagh",
            product_type=2,
            segment="accommodatie",
            persons=4,
            capacity_min=6,
            db=db,
            headless=headless,
            **kwargs,
        )


class CapfunSprookjesCampingScraper(CapfunScraper):
    """De Sprookjescamping - Kampeerplaats (camping only)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Capfun De Sprookjescamping",
            accommodation_type="Kampeerplaats",
            camping_param="sprookjes",
            product_type=1,
            segment="kampeerplaats",
            persons=2,
            db=db,
            headless=headless,
            **kwargs,
        )


class CapfunFruithofCampingScraper(CapfunScraper):
    """De Fruithof - Kampeerplaats (camping segment)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Capfun De Fruithof",
            accommodation_type="Kampeerplaats",
            camping_param="vakantiedefruithof",
            product_type=1,
            segment="kampeerplaats",
            persons=2,
            db=db,
            headless=headless,
            **kwargs,
        )


class CapfunFruithofAccScraper(CapfunScraper):
    """De Fruithof - Accommodatie (6+ personen)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Capfun De Fruithof",
            accommodation_type="Accommodatie (6p)",
            camping_param="vakantiedefruithof",
            product_type=2,
            segment="accommodatie",
            persons=4,
            capacity_min=6,
            db=db,
            headless=headless,
            **kwargs,
        )
