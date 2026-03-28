"""Scraper for Kampeerdorp De Zandstuve — BookingExperts platform.

Uses the same price-grid-table approach as Beerze Bulten and De Boshoek.
Supports multiple segments:
- Accommodatie: Vechtdallodge 6p
- Kampeerplaats: Comfort camping site (ID 6910)
- Privé sanitair: Pitch with private bathroom (ID 6900)
"""

from database import Database
from scrapers.beerze_bulten import BeerzeBultenScraper
from scrapers.base_scraper import BaseScraper


class ZandstuveBoslodgeScraper(BeerzeBultenScraper):
    """De Zandstuve — Vechtdallodge 6p (accommodatie segment)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        BaseScraper.__init__(
            self,
            competitor_name="De Zandstuve",
            accommodation_type="Vechtdallodge (6p)",
            url="https://www.zandstuve.com/holiday-homes/netherlands-province-of-overijssel-vechtdallodge-6-persons",
            db=db,
            headless=headless,
            **kwargs,
        )
        self.segment = "accommodatie"

    def run_efficient(self, **kwargs):
        self._inject_segment()
        return super().run_efficient(**kwargs)

    def _inject_segment(self):
        """Patch save_price to include segment."""
        original_save = self.db.save_price
        segment = self.segment

        def save_with_segment(**kw):
            kw.setdefault("segment", segment)
            return original_save(**kw)

        self.db.save_price = save_with_segment


class ZandstuveCampingScraper(BeerzeBultenScraper):
    """De Zandstuve — Comfort camping site (kampeerplaats segment)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        BaseScraper.__init__(
            self,
            competitor_name="De Zandstuve",
            accommodation_type="Comfort camping site",
            url="https://www.zandstuve.com/pitches/netherlands-province-of-overijssel-comfort-camping-site",
            db=db,
            headless=headless,
            **kwargs,
        )
        self.segment = "kampeerplaats"
        self.DEFAULT_PERSONS = 2

    def run_efficient(self, **kwargs):
        kwargs.setdefault("persons", 2)
        self._inject_segment()
        return super().run_efficient(**kwargs)

    def _inject_segment(self):
        original_save = self.db.save_price
        segment = self.segment

        def save_with_segment(**kw):
            kw.setdefault("segment", segment)
            return original_save(**kw)

        self.db.save_price = save_with_segment


class ZandstuvePsanitairScraper(BeerzeBultenScraper):
    """De Zandstuve — Pitch with private bathroom (privé sanitair segment)."""

    def __init__(self, db: Database, headless: bool = True, **kwargs):
        BaseScraper.__init__(
            self,
            competitor_name="De Zandstuve",
            accommodation_type="Pitch with private bathroom",
            url="https://www.zandstuve.com/pitches/netherlands-province-of-overijssel-pitch-with-private-bathroom",
            db=db,
            headless=headless,
            **kwargs,
        )
        self.segment = "prive_sanitair"
        self.DEFAULT_PERSONS = 2

    def run_efficient(self, **kwargs):
        kwargs.setdefault("persons", 2)
        self._inject_segment()
        return super().run_efficient(**kwargs)

    def _inject_segment(self):
        original_save = self.db.save_price
        segment = self.segment

        def save_with_segment(**kw):
            kw.setdefault("segment", segment)
            return original_save(**kw)

        self.db.save_price = save_with_segment
