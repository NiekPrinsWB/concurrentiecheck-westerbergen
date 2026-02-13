"""Scraper for De Witte Berg - Bungalow Dennenlust.

Uses BookingExperts platform (same as Beerze Bulten).
Has a price grid table on the accommodation detail page.
"""

from database import Database
from scrapers.beerze_bulten import BeerzeBultenScraper


class DeWitteBergScraper(BeerzeBultenScraper):
    """Scraper for De Witte Berg, inherits BookingExperts grid logic."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        # Skip BeerzeBultenScraper.__init__, call BaseScraper directly
        from scrapers.base_scraper import BaseScraper
        BaseScraper.__init__(
            self,
            competitor_name="De Witte Berg",
            accommodation_type="Bungalow Dennenlust",
            url="https://www.dewitteberg.nl/accommodaties/nederland-overijssel-bungalow-dennenlust-6-personen",
            db=db,
            headless=headless,
            **kwargs,
        )
