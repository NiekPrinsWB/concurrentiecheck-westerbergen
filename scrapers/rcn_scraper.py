"""Scraper for RCN De Noordster - Bungalows and Camping.

Website uses Maxxton with Nuxt.js SSR. All pricing data is server-side
rendered - no public API available. Uses Playwright to load pages and
extract prices from the embedded __NUXT_DATA__ script tags.

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

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)

# Base URL for RCN De Noordster
BASE_URL = "https://www.rcn.nl/nl/vakantieparken/nederland/drenthe/rcn-de-noordster"


class RcnScraper(BaseScraper):
    """Base scraper for RCN De Noordster accommodations.

    Uses Playwright to load Nuxt.js SSR pages and extract pricing data
    from __NUXT_DATA__ script tags or rendered HTML elements.
    """

    # Subclasses must set these
    ACCOMMODATION_SLUG = ""       # e.g. "bungalow-mercurius"
    ACCOMMODATION_NAME = ""       # e.g. "Bungalow Mercurius 6p"
    SEGMENT = "accommodatie"
    DEFAULT_PERSONS = 6
    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="RCN De Noordster",
            accommodation_type=self.ACCOMMODATION_NAME,
            url=f"{BASE_URL}/verhuur/{self.ACCOMMODATION_SLUG}",
            db=db,
            headless=headless,
            rate_limit=5.0,
            page_timeout=60000,
            **kwargs,
        )

    def _accept_cookies(self, page: Page):
        """Handle cookie consent banner (Cookiebot or similar)."""
        try:
            # Try common cookie banner selectors
            for selector in [
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                "button:has-text('Alles accepteren')",
                "button:has-text('Alle cookies accepteren')",
                "button:has-text('Accepteer')",
                "button:has-text('Akkoord')",
                "#onetrust-accept-btn-handler",
                "button[data-testid='cookie-accept']",
            ]:
                btn = page.query_selector(selector)
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(1)
                    self.logger.debug("Cookie banner accepted")
                    return
        except Exception as e:
            self.logger.debug(f"Cookie handling: {e}")

    def _extract_nuxt_data(self, page: Page) -> list:
        """Extract the __NUXT_DATA__ JSON array from the page.

        Nuxt 3 embeds server data in <script type="application/json"
        id="__NUXT_DATA__"> tags. The data is a flat JSON array where
        elements reference each other by index.

        Returns the raw list, or empty list on failure.
        """
        try:
            data = page.evaluate("""
                () => {
                    const el = document.querySelector('script#__NUXT_DATA__');
                    if (!el) return null;
                    try {
                        return JSON.parse(el.textContent);
                    } catch(e) {
                        return null;
                    }
                }
            """)
            if data and isinstance(data, list):
                self.logger.debug(f"__NUXT_DATA__ found: {len(data)} elements")
                return data
            else:
                self.logger.debug("__NUXT_DATA__ not found or not a list")
                return []
        except Exception as e:
            self.logger.warning(f"Failed to extract __NUXT_DATA__: {e}")
            return []

    def _extract_price_from_nuxt(self, nuxt_data: list) -> float | None:
        """Search the NUXT_DATA array for a price value.

        The Nuxt data array is a flat structure. We look for price-related
        keys and their values. This is heuristic and may need adjustment
        after testing with real pages.

        Strategy:
        1. Convert the array to a searchable string representation
        2. Look for price-related patterns (totalPrice, price, amount, etc.)
        3. Extract numeric values near those keys
        """
        if not nuxt_data:
            return None

        try:
            # Strategy 1: Walk the array looking for price-like keys
            # In Nuxt data, string keys and their values are sequential
            price_keys = [
                "totalPrice", "price", "total", "amount", "basePrice",
                "displayPrice", "salePrice", "totalAmount", "priceTotal",
            ]

            for i, item in enumerate(nuxt_data):
                if isinstance(item, str) and item.lower() in [k.lower() for k in price_keys]:
                    # Check next few elements for a numeric value
                    for j in range(i + 1, min(i + 5, len(nuxt_data))):
                        val = nuxt_data[j]
                        if isinstance(val, (int, float)) and val > 10:
                            self.logger.debug(
                                f"Found price via key '{item}' at index {i}: {val}"
                            )
                            return float(val)

            # Strategy 2: Look for EUR currency patterns in string elements
            for i, item in enumerate(nuxt_data):
                if isinstance(item, str):
                    match = re.search(r'€\s*([\d.,]+)', item)
                    if match:
                        price_str = match.group(1).replace('.', '').replace(',', '.')
                        try:
                            price = float(price_str)
                            if price > 10:
                                self.logger.debug(
                                    f"Found price via EUR pattern at index {i}: {price}"
                                )
                                return price
                        except ValueError:
                            continue

        except Exception as e:
            self.logger.warning(f"Error parsing NUXT data for price: {e}")

        return None

    def _extract_price_from_html(self, page: Page) -> float | None:
        """Fallback: extract price from rendered HTML elements.

        Tries various CSS selectors commonly used for pricing on
        Maxxton/Nuxt booking pages.
        """
        try:
            price = page.evaluate("""
                () => {
                    // Common price selectors for Maxxton/RCN pages
                    const selectors = [
                        '[data-testid="price"]',
                        '.price-total',
                        '.price__total',
                        '.booking-price',
                        '.accommodation-price',
                        '.price-amount',
                        '.total-price',
                        '.price',
                    ];

                    for (const sel of selectors) {
                        const els = document.querySelectorAll(sel);
                        for (const el of els) {
                            const text = el.innerText || el.textContent || '';
                            // Match price patterns: "€ 524", "EUR 1.065", "524,-"
                            const match = text.match(/€\\s*([\\d.,]+)|EUR\\s*([\\d.,]+)|([\\d.,]+)\\s*,-/);
                            if (match) {
                                const raw = (match[1] || match[2] || match[3])
                                    .replace(/\\./g, '')
                                    .replace(',', '.');
                                const price = parseFloat(raw);
                                if (price > 10 && price < 50000) return price;
                            }
                        }
                    }

                    // Broader search: any element with euro sign
                    const all = document.body.innerText;
                    const matches = all.match(/€\\s*([\\d.,]+)/g);
                    if (matches) {
                        // Return the largest price found (likely total price)
                        let maxPrice = 0;
                        for (const m of matches) {
                            const raw = m.replace('€', '').trim()
                                .replace(/\\./g, '').replace(',', '.');
                            const p = parseFloat(raw);
                            if (p > maxPrice && p < 50000) maxPrice = p;
                        }
                        if (maxPrice > 10) return maxPrice;
                    }

                    return null;
                }
            """)
            if price:
                self.logger.debug(f"Found price from HTML: {price}")
            return price
        except Exception as e:
            self.logger.warning(f"HTML price extraction failed: {e}")
            return None

    def _check_availability(self, page: Page) -> bool:
        """Check if the accommodation is marked as unavailable on the page."""
        try:
            unavailable = page.evaluate("""
                () => {
                    const text = document.body.innerText.toLowerCase();
                    const unavailablePatterns = [
                        'niet beschikbaar',
                        'not available',
                        'uitverkocht',
                        'sold out',
                        'geen beschikbaarheid',
                        'no availability',
                    ];
                    return unavailablePatterns.some(p => text.includes(p));
                }
            """)
            return not unavailable
        except Exception:
            return True  # Assume available if check fails

    def _build_accommodation_url(self, check_in: datetime, check_out: datetime) -> str:
        """Build the URL for a specific accommodation with date parameters."""
        arrival = check_in.strftime("%Y-%m-%dT00:00:00")
        departure = check_out.strftime("%Y-%m-%dT00:00:00")
        return (
            f"{BASE_URL}/verhuur/{self.ACCOMMODATION_SLUG}"
            f"?arrival={arrival}&departure={departure}"
        )

    def scrape_price(self, page: Page, check_in: datetime,
                     check_out: datetime, persons: int = 6) -> dict | None:
        """Scrape price for a specific date range from RCN De Noordster.

        Loads the accommodation page with date parameters and extracts
        the price from __NUXT_DATA__ or rendered HTML.
        """
        nights = (check_out - check_in).days
        url = self._build_accommodation_url(check_in, check_out)

        self.logger.debug(f"Loading {self.ACCOMMODATION_SLUG}: {check_in.date()} - {check_out.date()} ({nights}n)")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)  # Wait for Nuxt hydration
        except PlaywrightTimeout:
            self.logger.warning(f"Timeout loading page for {check_in.date()}")
            return None

        # Accept cookies on first visit
        self._accept_cookies(page)

        # Check availability first
        available = self._check_availability(page)

        # Try extracting price from __NUXT_DATA__ first (most reliable)
        nuxt_data = self._extract_nuxt_data(page)
        price = self._extract_price_from_nuxt(nuxt_data)

        # Fallback to HTML extraction
        if price is None:
            price = self._extract_price_from_html(page)

        if price is None and available:
            self.logger.debug(
                f"No price found for {check_in.date()} ({nights}n) "
                f"but page loaded. May need selector adjustment."
            )

        return {
            "price": price,
            "available": available and price is not None,
            "min_nights": nights,
            "special_offers": None,
        }

    def _generate_date_pairs(self, months_ahead: int = 12) -> list[dict]:
        """Generate weekly arrival dates for all stay durations.

        Creates date pairs at 7-day intervals (Fridays and Mondays)
        for the next N months. Covers weekend (2n), midweek (4n),
        and week (7n) stays.
        """
        today = datetime.now().date()
        end_date = today + timedelta(days=months_ahead * 30)

        date_pairs = []
        seen = set()

        # Generate Friday arrivals (weekend + week stays)
        # and Monday arrivals (midweek stays)
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
        """Scrape all prices by loading individual accommodation pages.

        For each date combination, loads the accommodation page with
        arrival/departure params and extracts the price.

        Args:
            months_ahead: Number of months ahead to scrape (default 12).
            persons: Number of guests (default: DEFAULT_PERSONS).
        """
        from playwright.sync_api import sync_playwright

        if persons is None:
            persons = self.DEFAULT_PERSONS

        date_pairs = self._generate_date_pairs(months_ahead)

        self.logger.info(
            f"Starting RCN scrape for {self.competitor_name} - "
            f"{self.accommodation_type}: {len(date_pairs)} date combinations, "
            f"{months_ahead} months ahead, {persons} persons"
        )

        start_time = time.time()
        all_records = []
        seen_keys = set()
        errors = 0
        consecutive_failures = 0
        max_consecutive_failures = 5

        with sync_playwright() as playwright:
            browser = self._create_browser(playwright)
            try:
                page = self._create_page(browser)

                # Load the base accommodation page first to establish session
                self.logger.info("  Loading base page to establish session...")
                try:
                    page.goto(self.url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(3)
                    self._accept_cookies(page)
                except PlaywrightTimeout:
                    self.logger.warning("  Base page timeout, continuing anyway...")

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

                    try:
                        # Convert date to datetime for scrape_price
                        ci_dt = datetime.combine(check_in, datetime.min.time())
                        co_dt = datetime.combine(check_out, datetime.min.time())

                        result = self.scrape_price(page, ci_dt, co_dt, persons)

                        if result is not None:
                            seen_keys.add(key)
                            consecutive_failures = 0

                            record = {
                                "competitor_name": self.competitor_name,
                                "accommodation_type": self.accommodation_type,
                                "check_in_date": check_in.isoformat(),
                                "check_out_date": check_out.isoformat(),
                                "price": result.get("price"),
                                "available": result.get("available", False),
                                "min_nights": nights,
                                "special_offers": result.get("special_offers"),
                                "persons": persons,
                                "segment": self.SEGMENT,
                            }
                            self.db.save_price(**record)
                            all_records.append(record)

                            price_str = f"EUR {result['price']:.0f}" if result.get("price") else "N/A"
                            avail_str = "beschikbaar" if result.get("available") else "niet beschikbaar"
                            self.logger.info(
                                f"  {check_in} -> {check_out} ({nights}n): "
                                f"{price_str} ({avail_str})"
                            )
                        else:
                            consecutive_failures += 1
                            errors += 1

                    except PlaywrightTimeout:
                        consecutive_failures += 1
                        errors += 1
                        self.logger.warning(
                            f"  Timeout for {check_in} -> {check_out} ({nights}n)"
                        )
                    except Exception as e:
                        consecutive_failures += 1
                        errors += 1
                        self.logger.warning(
                            f"  Error for {check_in} -> {check_out}: {e}"
                        )
                        # Recreate page on unexpected errors
                        try:
                            page.close()
                        except Exception:
                            pass
                        page = self._create_page(browser)

                    # Bail out if too many consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        self.logger.error(
                            f"  {max_consecutive_failures} consecutive failures, "
                            f"stopping scraper. Check page structure / selectors."
                        )
                        break

            except Exception as e:
                errors += 1
                self.logger.error(f"  Scraping failed: {e}", exc_info=True)
            finally:
                browser.close()

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
    SEGMENT = "accommodatie"
    DEFAULT_PERSONS = 6

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(db=db, headless=headless, **kwargs)


class RcnNoordsterLunaScraper(RcnScraper):
    """RCN De Noordster - Bungalow Luna 6p (premium segment B)."""

    ACCOMMODATION_SLUG = "bungalow-luna"
    ACCOMMODATION_NAME = "Bungalow Luna 6p"
    SEGMENT = "accommodatie"
    DEFAULT_PERSONS = 6

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(db=db, headless=headless, **kwargs)


class RcnNoordsterCampingScraper(RcnScraper):
    """RCN De Noordster - Comfort kampeerplaats (camping segment A).

    Uses the camping listing page (/kamperen) instead of a specific
    accommodation detail page. URL pattern and price extraction may
    differ from bungalow pages.
    """

    ACCOMMODATION_SLUG = "comfort-kampeerplaats"
    ACCOMMODATION_NAME = "Comfort kampeerplaats"
    SEGMENT = "kampeerplaats"
    DEFAULT_PERSONS = 2
    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        # Call BaseScraper directly to set the detail page URL
        BaseScraper.__init__(
            self,
            competitor_name="RCN De Noordster",
            accommodation_type=self.ACCOMMODATION_NAME,
            url=f"{BASE_URL}/kamperen/{self.ACCOMMODATION_SLUG}",
            db=db,
            headless=headless,
            rate_limit=5.0,
            page_timeout=60000,
            **kwargs,
        )

    def _build_accommodation_url(self, check_in: datetime, check_out: datetime) -> str:
        """Build URL for comfort-kampeerplaats detail page with date parameters."""
        arrival = check_in.strftime("%Y-%m-%dT00:00:00")
        departure = check_out.strftime("%Y-%m-%dT00:00:00")
        return (
            f"{BASE_URL}/kamperen/{self.ACCOMMODATION_SLUG}"
            f"?arrival={arrival}&departure={departure}"
        )

    def _extract_price_from_html(self, page: Page) -> float | None:
        """Extract camping price from the detail page.

        The detail page shows the price as plain text (e.g. "€ 287") near
        the booking section. Filters out the €35 pitch selection surcharge.
        """
        try:
            price = page.evaluate("""
                () => {
                    const text = document.body.innerText;
                    const matches = [...text.matchAll(/€\\s*([\\d.,]+)/g)];
                    if (!matches.length) return null;

                    let prices = [];
                    for (const m of matches) {
                        const raw = m[1].replace(/\\./g, '').replace(',', '.');
                        const p = parseFloat(raw);
                        // Filter out €35 pitch selection fee and invalid values
                        if (p > 10 && p < 10000 && p !== 35) prices.push(p);
                    }

                    // Return the smallest valid price (base price)
                    if (prices.length) return Math.min(...prices);
                    return null;
                }
            """)

            if price:
                self.logger.debug(f"Found camping price from HTML: {price}")
                return price

            return super()._extract_price_from_html(page)

        except Exception as e:
            self.logger.warning(f"Camping HTML price extraction failed: {e}")
            return super()._extract_price_from_html(page)

    def run_efficient(self, months_ahead: int = 12, persons: int = 2,
                      **kwargs) -> list[dict]:
        """Run with default 2 persons for camping."""
        return super().run_efficient(
            months_ahead=months_ahead, persons=persons, **kwargs
        )
