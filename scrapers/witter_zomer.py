"""Scraper for Witter Zomer using TOMM/BoekingPro booking widget.

The site uses a Vue-based booking widget (widgets.boekingpro.nl) that shows
a price matrix on the accommodation detail page with date columns and
duration rows.

Prices are read directly from the rendered widget DOM rather than the
raw matrix API, because the Vue widget applies client-side promotional
discounts that are not included in the API response. This ensures we
capture the actual consumer-facing prices.

The matrix cells contain booking links with structured parameters including
check-in/check-out dates and the final price. We parse these links for
reliable data extraction.

The widget is paginated; clicking the 'next' arrow advances to the
next set of date columns (typically 4 columns per page).
"""

import json
import re
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import unquote

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout

from scrapers.base_scraper import BaseScraper
from database import Database

logger = logging.getLogger(__name__)


class WitterZomerScraper(BaseScraper):
    """Scraper for Witter Zomer via the BoekingPro/TOMM matrix widget."""

    # 6-persoons Vakantiehuis C (code 35)
    ACCOMMODATION_ID = "65598"

    # Stay durations we care about
    STAY_DURATIONS = [2, 3, 4, 7]

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        super().__init__(
            competitor_name="Witter Zomer",
            accommodation_type="6-persoons vakantiehuis (C6)",
            url="https://www.witterzomer.nl/accommodaties/nederland-drenthe-6-persoons-vakantiehuis-c6",
            db=db,
            headless=headless,
            **kwargs,
        )

    @staticmethod
    def _build_detail_url(persons: int = 4) -> str:
        """Build the detail page URL with person count."""
        return (
            "https://www.witterzomer.nl/accommodaties/verhuur/6-persoons-vakantiehuis"
            f'?house=[%2287%22]&stay=[%2238%22]'
            f'&travelgroup={{%22adult%22:{persons}}}'
        )

    def scrape_price(self, page, check_in, check_out, persons=4):
        raise NotImplementedError("Use run_efficient()")

    def _parse_widget_matrix(self, page: Page) -> tuple[list[dict], bool]:
        """Parse the visible price matrix from the rendered widget DOM.

        Extracts prices from booking links in matrix cells. Each cell's <a> tag
        contains a href with period start/end dates and the final price.

        Returns:
            (records, has_next) where records is a list of price dicts and
            has_next indicates whether a 'next' pagination button exists.
        """
        data = page.evaluate("""() => {
            const widget = document.querySelector('.w3media-booking-matrix-widget');
            if (!widget) return {error: 'no widget', records: [], hasNext: false};

            const records = [];
            const rows = widget.querySelectorAll('.matrix-row');

            for (const row of rows) {
                const cells = row.querySelectorAll('.matrix-cel');
                if (cells.length < 2) continue;

                // First cell contains the duration label
                const durEl = cells[0].querySelector('.duration');
                if (!durEl) continue;
                const durText = durEl.innerText.trim();
                let duration = 0;
                const nightsMatch = durText.match(/(\\d+)\\s+nachten?/);
                const weekMatch = durText.match(/(\\d+)\\s+w(?:ek|eken)/);
                if (nightsMatch) duration = parseInt(nightsMatch[1]);
                else if (weekMatch) duration = parseInt(weekMatch[1]) * 7;
                if (duration === 0) continue;

                // Remaining cells are price cells
                for (let i = 1; i < cells.length; i++) {
                    const cell = cells[i];
                    const link = cell.querySelector('a.available, a.matrix-price-popover-container');
                    if (!link) continue;

                    const href = link.getAttribute('href') || '';

                    // Parse dates from href: period={"start":"2026-02-14","end":"2026-02-16"}
                    let checkIn = null, checkOut = null;
                    const periodMatch = href.match(/period=([^&]+)/);
                    if (periodMatch) {
                        try {
                            const periodStr = decodeURIComponent(periodMatch[1]);
                            const period = JSON.parse(periodStr);
                            checkIn = period.start;
                            checkOut = period.end;
                        } catch(e) {}
                    }

                    // Parse prices from the DOM elements
                    const pricesDiv = cell.querySelector('.prices');
                    if (!pricesDiv) continue;

                    const discountEl = pricesDiv.querySelector('.discount-price');
                    const priceEl = pricesDiv.querySelector('.price');
                    const oldPriceEl = pricesDiv.querySelector('.price-old');

                    let price = null;
                    let originalPrice = null;

                    if (discountEl) {
                        // Discounted: use discount price as actual, old as original
                        price = parseFloat(discountEl.innerText.trim().replace('.', '').replace(',', '.'));
                        if (oldPriceEl) {
                            originalPrice = parseFloat(oldPriceEl.innerText.trim().replace('.', '').replace(',', '.'));
                        }
                    } else if (priceEl) {
                        // Regular price
                        price = parseFloat(priceEl.innerText.trim().replace('.', '').replace(',', '.'));
                    }

                    // Fallback: try price from href
                    if (price === null || isNaN(price)) {
                        const priceMatch = href.match(/price=([\\d.]+)/);
                        if (priceMatch) price = parseFloat(priceMatch[1]);
                    }

                    if (checkIn && checkOut && price !== null && !isNaN(price)) {
                        records.push({
                            checkIn: checkIn,
                            checkOut: checkOut,
                            duration: duration,
                            price: price,
                            originalPrice: (originalPrice && !isNaN(originalPrice) && originalPrice > price) ? originalPrice : null,
                        });
                    }
                }
            }

            // Check for next button
            const nextBtn = widget.querySelector('a.btn-next');
            const hasNext = nextBtn !== null;

            return {records, hasNext};
        }""")

        if isinstance(data, str) or data.get("error"):
            return [], False

        raw_records = data.get("records", [])
        has_next = data.get("hasNext", False)

        result_records = []
        for r in raw_records:
            special = None
            if r.get("originalPrice"):
                special = f"Was EUR {r['originalPrice']:.0f}"

            result_records.append({
                "check_in_date": r["checkIn"],
                "check_out_date": r["checkOut"],
                "price": r["price"],
                "available": True,
                "min_nights": r["duration"],
                "special_offers": special,
            })

        return result_records, has_next

    def run_efficient(self, months_ahead: int = 3, persons: int = 4,
                      max_pages: int = 30, **kwargs) -> list[dict]:
        """Scrape prices by loading the widget and paginating through dates.

        Reads prices from the rendered DOM to capture promotional discounts
        that the raw matrix API does not include.
        """
        from playwright.sync_api import sync_playwright

        self.logger.info(
            f"Starting scrape for {self.competitor_name}: "
            f"{months_ahead} months ahead, {persons} persons"
        )

        target_end = datetime.now() + timedelta(days=months_ahead * 30)
        target_end_str = target_end.strftime("%Y-%m-%d")

        start_time = time.time()
        all_records = []
        seen_keys = set()
        errors = 0

        with sync_playwright() as playwright:
            browser = self._create_browser(playwright)
            try:
                page = self._create_page(browser)

                # Load the accommodation detail page with correct person count
                detail_url = self._build_detail_url(persons)
                self.logger.info("  Loading accommodation detail page...")
                page.goto(detail_url, wait_until="networkidle", timeout=120000)
                time.sleep(5)  # Wait for Vue widget to fully render

                for page_num in range(1, max_pages + 1):
                    try:
                        records, has_next = self._parse_widget_matrix(page)
                    except Exception as e:
                        errors += 1
                        self.logger.error(f"  Page {page_num} parse failed: {e}")
                        break

                    new_count = 0
                    max_date_seen = None
                    for r in records:
                        if r["min_nights"] not in self.STAY_DURATIONS:
                            continue
                        key = (r["check_in_date"], r["check_out_date"])
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)

                        record = {
                            "competitor_name": self.competitor_name,
                            "accommodation_type": self.accommodation_type,
                            "persons": persons,
                            **r,
                        }
                        self.db.save_price(**record)
                        all_records.append(record)
                        new_count += 1

                        if max_date_seen is None or r["check_in_date"] > max_date_seen:
                            max_date_seen = r["check_in_date"]

                    self.logger.info(
                        f"  Page {page_num} (up to {max_date_seen or '?'}): "
                        f"{new_count} new prices"
                    )

                    # Check if we've passed the target end date
                    if max_date_seen and max_date_seen >= target_end_str:
                        self.logger.info(
                            f"  Reached target end date {target_end_str}"
                        )
                        break

                    # Navigate to next page
                    if not has_next:
                        self.logger.info("  No next button, stopping.")
                        break

                    try:
                        self._wait_rate_limit()
                        next_btn = page.query_selector(
                            '.w3media-booking-matrix-widget a.btn-next'
                        )
                        if next_btn and next_btn.is_visible():
                            next_btn.click()
                            time.sleep(2)  # Wait for new data to render
                        else:
                            break
                    except Exception as e:
                        self.logger.warning(f"  Navigation failed: {e}")
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
            error_message=f"{errors} errors" if errors else None,
            duration_seconds=duration,
        )

        available = [r for r in all_records if r["available"] and r["price"]]
        self.logger.info(
            f"Completed {self.competitor_name}: {len(all_records)} records "
            f"({len(available)} available), {duration:.1f}s"
        )

        return all_records
