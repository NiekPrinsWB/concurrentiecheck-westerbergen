"""Smoke tests voor database module."""

from database import Database


def test_save_and_retrieve(tmp_db):
    """Test dat opgeslagen prijzen correct terugkomen."""
    prices = tmp_db.get_prices(competitor_name="Westerbergen")
    assert len(prices) >= 3
    assert all(p["competitor_name"] == "Westerbergen" for p in prices)


def test_upsert_updates_price(tmp_db):
    """Test dat een duplicate key de prijs update."""
    # Save initial
    tmp_db.save_price(
        competitor_name="Test", accommodation_type="Test",
        check_in_date="2026-01-01", check_out_date="2026-01-04",
        price=100.0, available=True, min_nights=3, persons=4,
    )

    # Update with new price
    tmp_db.save_price(
        competitor_name="Test", accommodation_type="Test",
        check_in_date="2026-01-01", check_out_date="2026-01-04",
        price=150.0, available=True, min_nights=3, persons=4,
    )

    prices = tmp_db.get_prices(competitor_name="Test")
    assert len(prices) == 1
    assert prices[0]["price"] == 150.0


def test_comparison_data(tmp_db):
    """Test get_comparison_data retourneert gefilterde data."""
    date = tmp_db.get_latest_scrape_date()
    assert date is not None

    data = tmp_db.get_comparison_data(date, durations=[3])
    assert len(data) > 0
    # All should be 3-night stays
    for row in data:
        assert row["nights"] == 3


def test_scrape_summary(tmp_db):
    """Test get_scrape_summary retourneert dict per competitor."""
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")

    summary = tmp_db.get_scrape_summary(today)
    assert isinstance(summary, dict)
    assert "Westerbergen" in summary
    assert summary["Westerbergen"]["status"] == "success"
    assert summary["Westerbergen"]["records_scraped"] == 3


def test_scrape_dates(tmp_db):
    """Test get_all_scrape_dates retourneert lijst."""
    dates = tmp_db.get_all_scrape_dates()
    assert isinstance(dates, list)
    assert len(dates) >= 1


def test_empty_database(tmp_path):
    """Test dat een lege database correct initialiseert."""
    db = Database(str(tmp_path / "empty.db"))
    assert db.get_latest_scrape_date() is None
    assert db.get_prices() == []
    assert db.get_scrape_summary() == {}
