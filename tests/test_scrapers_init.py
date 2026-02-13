"""Smoke tests: alle scrapers initialiseren zonder fouten."""

from database import Database


def test_beerze_bulten_init(tmp_db):
    """Beerze Bulten scraper initialiseert correct."""
    from scrapers.beerze_bulten import BeerzeBultenScraper
    s = BeerzeBultenScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "Beerze Bulten"
    assert s.accommodation_type == "Luxe Bungalow"
    assert "beerzebulten.nl" in s.url


def test_de_witte_berg_init(tmp_db):
    """De Witte Berg scraper initialiseert correct."""
    from scrapers.de_witte_berg import DeWitteBergScraper
    s = DeWitteBergScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "De Witte Berg"


def test_de_boshoek_init(tmp_db):
    """De Boshoek scraper initialiseert correct."""
    from scrapers.de_boshoek import DeBoshoekScraper
    s = DeBoshoekScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "De Boshoek"
    assert "deboshoek.nl" in s.url


def test_camping_ommerland_init(tmp_db):
    """Camping Ommerland scraper initialiseert correct."""
    from scrapers.holidayagent_scraper import CampingOmmerlandScraper
    s = CampingOmmerlandScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "Camping Ommerland"
    assert s.resort_slug == "campingommerland"


def test_eiland_van_maurik_init(tmp_db):
    """Eiland van Maurik scraper initialiseert correct."""
    from scrapers.holidayagent_scraper import EilandVanMaurikScraper
    s = EilandVanMaurikScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "Eiland van Maurik"
    assert s.resort_slug == "eilandvanmaurik"


def test_witter_zomer_init(tmp_db):
    """Witter Zomer scraper initialiseert correct."""
    from scrapers.witter_zomer import WitterZomerScraper
    s = WitterZomerScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "Witter Zomer"


def test_westerbergen_init(tmp_db):
    """Westerbergen scraper initialiseert correct."""
    from scrapers.westerbergen import WesterbergenScraper
    s = WesterbergenScraper(db=tmp_db, headless=True)
    assert s.competitor_name == "Westerbergen"


def test_scraper_map_complete(tmp_db):
    """Alle 7 scrapers in de scraper map."""
    from run_scraper import get_scraper_map
    scrapers = get_scraper_map(tmp_db, headless=True)
    expected = {
        "beerze_bulten", "camping_ommerland", "eiland_van_maurik",
        "witter_zomer", "de_witte_berg", "de_boshoek", "westerbergen",
    }
    assert set(scrapers.keys()) == expected
