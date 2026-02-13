"""Scraper for De Boshoek - Bungalow Zeumeren.

Uses BookingExperts platform (same as Beerze Bulten).
Has a price grid table on the accommodation detail page.
"""

from database import Database
from scrapers.beerze_bulten import BeerzeBultenScraper


class DeBoshoekScraper(BeerzeBultenScraper):
    """Scraper for De Boshoek, inherits BookingExperts grid logic."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        from scrapers.base_scraper import BaseScraper
        BaseScraper.__init__(
            self,
            competitor_name="De Boshoek",
            accommodation_type="Bungalow Zeumeren",
            url="https://www.deboshoek.nl/accommodaties/nederland-gelderland-bungalow-zeumeren-6-personen",
            db=db,
            headless=headless,
            **kwargs,
        )
