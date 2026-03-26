"""Scraper for Molecaten Park Kuierpad — Bosven 6p + Comfortplaats.

Uses Molecaten's custom ASP.NET ASMX web service endpoints:
- /Services/Search.asmx/GetAvailability — price matrix (7 dates x 21 durations)
- /Services/Search.asmx/GetAvailabilityDates — all available arrival dates

No anti-bot measures, no auth needed. Only requires X-Requested-With header.
Pagination via navigatenextdate field in the response.
"""

import time
import logging
from datetime import datetime, timedelta

import requests

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

# Molecaten Kuierpad accommodation codes
KUIERPAD_ACCOMMODATIONS = {
    "bosven_6p": {"AC": "4063", "OS": "1300", "name": "Vakantiehuisje Bosven (6p)"},
    "comfortplaats": {"AC": "4064", "OS": "1189", "name": "Comfortplaats"},
}


class MolecatenScraper(BaseScraper):
    """Scraper for Molecaten parks via their Search.asmx API."""

    API_BASE = "https://www.molecaten.nl/Services/Search.asmx"

    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True,
                 ac_code: str = "4063", os_code: str = "1300",
                 accommodation_type: str = "Vakantiehuisje Bosven (6p)",
                 segment: str = "accommodatie", **kwargs):
        super().__init__(
            competitor_name="Molecaten Kuierpad",
            accommodation_type=accommodation_type,
            url="https://www.molecaten.nl/vakantiepark-kuierpad",
            db=db,
            headless=headless,
            **kwargs,
        )
        self.ac_code = ac_code
        self.os_code = os_code
        self.segment = segment
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        })

    def _fetch_availability_page(self, arrival_date: str, persons: int = 6) -> dict:
        """Fetch one page of the availability/price matrix.

        Args:
            arrival_date: Start date in YYYYMMDD format
            persons: Number of guests

        Returns:
            dict with 'prices' (list) and 'next_date' (str or None)
        """
        params = {
            "AC": self.ac_code,
            "OS": self.os_code,
            "ad": arrival_date,
            "dtt": "7",
            "ap": str(persons),
        }

        resp = self.session.get(
            f"{self.API_BASE}/GetAvailability",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # The response wraps in {"d": {...}} for ASMX
        inner = data.get("d", data)

        prices = []
        # Structure: availabilities[{nights, items[{arrivaldate, departuredate, ...}]}]
        for avail_group in inner.get("availabilities", []):
            nights = avail_group.get("nights", 0)
            if nights not in self.STAY_DURATIONS:
                continue

            for cell in avail_group.get("items", []):
                price = cell.get("priceoriginal")
                available = cell.get("available", False)
                arrival = cell.get("arrivaldate", "")
                departure = cell.get("departuredate", "")
                quantity = cell.get("quantity", 0)

                if not arrival or not departure:
                    continue

                try:
                    check_in = datetime.strptime(arrival, "%Y%m%d")
                    check_out = datetime.strptime(departure, "%Y%m%d")
                except ValueError:
                    continue

                if price is not None and price > 0:
                    prices.append({
                        "check_in": check_in,
                        "check_out": check_out,
                        "nights": nights,
                        "price": float(price),
                        "available": bool(available) and quantity > 0,
                        "quantity": quantity,
                    })

        next_date = inner.get("navigatenextdate")
        return {"prices": prices, "next_date": next_date}

    def scrape_price(self, page, check_in, check_out, persons=6):
        raise NotImplementedError("Use run_efficient()")

    def run_efficient(self, max_pages: int = 60, months_ahead: int = 12,
                      persons: int = 6, **kwargs) -> list[dict]:
        """Scrape all prices by paginating through the GetAvailability API."""
        self.logger.info(
            f"Starting API scrape for {self.competitor_name} "
            f"(AC={self.ac_code}, OS={self.os_code}, segment={self.segment})"
        )

        start_time = time.time()
        all_records = []
        seen_keys = set()
        errors = 0

        # Start from today
        current_date = datetime.now().strftime("%Y%m%d")

        try:
            for page_num in range(1, max_pages + 1):
                try:
                    result = self._fetch_availability_page(current_date, persons)
                    prices = result["prices"]
                    next_date = result["next_date"]

                    new_count = 0
                    for p in prices:
                        key = (
                            p["check_in"].strftime("%Y-%m-%d"),
                            p["check_out"].strftime("%Y-%m-%d"),
                        )
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)

                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "check_in_date": p["check_in"].strftime("%Y-%m-%d"),
                            "check_out_date": p["check_out"].strftime("%Y-%m-%d"),
                            "price": p["price"],
                            "available": p["available"],
                            "min_nights": p["nights"],
                            "special_offers": None,
                            "persons": persons,
                            "segment": self.segment,
                        }
                        self.db.save_price(**record)
                        all_records.append(record)
                        new_count += 1

                    self.logger.info(
                        f"  Page {page_num} (from {current_date}): "
                        f"{new_count} new prices"
                    )

                    if not next_date:
                        self.logger.info("  No more pages available")
                        break

                    current_date = next_date
                    time.sleep(0.2)  # Gentle rate limiting

                except Exception as e:
                    errors += 1
                    self.logger.error(f"  Page {page_num} failed: {e}")
                    break

        except Exception as e:
            errors += 1
            self.logger.error(f"  Scraping failed: {e}")

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


class MolecatenKuierpadBosvenScraper(MolecatenScraper):
    """Molecaten Kuierpad — Vakantiehuisje Bosven 6p (accommodatie)."""
    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            db=db, headless=headless,
            ac_code="4063", os_code="1300",
            accommodation_type="Vakantiehuisje Bosven (6p)",
            segment="accommodatie",
            **kwargs,
        )


class MolecatenKuierpadCampingScraper(MolecatenScraper):
    """Molecaten Kuierpad — Comfortplaats (kampeerplaats)."""
    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            db=db, headless=headless,
            ac_code="4064", os_code="1189",
            accommodation_type="Comfortplaats",
            segment="kampeerplaats",
            **kwargs,
        )
