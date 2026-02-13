"""Pytest fixtures voor Concurrentiecheck Westerbergen smoke tests."""

import os
import tempfile
import pytest
import yaml

from database import Database


@pytest.fixture
def tmp_db(tmp_path):
    """Maak een tijdelijke database met sample data."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    # Sample prijzen: Westerbergen + 2 concurrenten, meerdere datums
    sample_prices = [
        # Westerbergen
        {"competitor_name": "Westerbergen", "accommodation_type": "Bosbungalow Sequoia C6",
         "check_in_date": "2026-03-06", "check_out_date": "2026-03-09",
         "price": 299.0, "available": True, "min_nights": 3, "persons": 4},
        {"competitor_name": "Westerbergen", "accommodation_type": "Bosbungalow Sequoia C6",
         "check_in_date": "2026-03-13", "check_out_date": "2026-03-16",
         "price": 349.0, "available": True, "min_nights": 3, "persons": 4},
        {"competitor_name": "Westerbergen", "accommodation_type": "Bosbungalow Sequoia C6",
         "check_in_date": "2026-04-10", "check_out_date": "2026-04-13",
         "price": 499.0, "available": True, "min_nights": 3, "persons": 4},
        # Beerze Bulten
        {"competitor_name": "Beerze Bulten", "accommodation_type": "Luxe Bungalow",
         "check_in_date": "2026-03-06", "check_out_date": "2026-03-09",
         "price": 392.0, "available": True, "min_nights": 3, "persons": 4},
        {"competitor_name": "Beerze Bulten", "accommodation_type": "Luxe Bungalow",
         "check_in_date": "2026-03-13", "check_out_date": "2026-03-16",
         "price": 420.0, "available": True, "min_nights": 3, "persons": 4},
        # De Boshoek
        {"competitor_name": "De Boshoek", "accommodation_type": "Bungalow Zeumeren",
         "check_in_date": "2026-03-06", "check_out_date": "2026-03-09",
         "price": 351.0, "available": True, "min_nights": 3, "persons": 4},
        {"competitor_name": "De Boshoek", "accommodation_type": "Bungalow Zeumeren",
         "check_in_date": "2026-04-10", "check_out_date": "2026-04-13",
         "price": 532.0, "available": True, "min_nights": 3, "persons": 4},
        # Unavailable record
        {"competitor_name": "De Boshoek", "accommodation_type": "Bungalow Zeumeren",
         "check_in_date": "2026-04-12", "check_out_date": "2026-04-15",
         "price": None, "available": False, "min_nights": 3, "persons": 4},
    ]

    for record in sample_prices:
        db.save_price(**record)

    # Sample scrape log entries
    db.log_scrape("Westerbergen", "success", records_scraped=3, duration_seconds=154.2)
    db.log_scrape("Beerze Bulten", "success", records_scraped=2, duration_seconds=130.5)
    db.log_scrape("De Boshoek", "partial", records_scraped=3, error_message="1 errors", duration_seconds=153.0)

    return db


@pytest.fixture
def sample_config(tmp_path):
    """Maak een tijdelijke YAML config."""
    config = {
        "general": {
            "project_name": "Test Concurrentiecheck",
            "database_path": str(tmp_path / "test.db"),
            "log_dir": str(tmp_path / "logs"),
            "log_level": "DEBUG",
        },
        "scraping": {
            "rate_limit_seconds": 5,
            "max_retries": 3,
            "page_timeout": 60,
            "headless": True,
            "default_persons": 4,
        },
        "competitors": {
            "beerze_bulten": {
                "name": "Beerze Bulten",
                "accommodation": "Luxe Bungalow",
                "url": "https://www.beerzebulten.nl/accommodaties/bungalow",
                "enabled": True,
                "persons": 4,
            },
        },
        "automation": {
            "schedule_time": "07:00",
            "retry_failed": True,
            "max_retry_attempts": 1,
            "generate_dashboard": True,
            "generate_analytics": True,
        },
        "dashboard": {
            "output_dir": str(tmp_path / "data"),
            "filename_template": "test_{date}.xlsx",
            "keep_last": 5,
        },
    }

    config_path = tmp_path / "settings.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False)

    return str(config_path), config
