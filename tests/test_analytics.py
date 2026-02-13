"""Smoke tests voor analytics module."""

from analytics import run_analytics


def test_analytics_on_sample_data(tmp_db):
    """Analytics draait zonder fouten op sample data."""
    result = run_analytics(
        db_path=tmp_db.db_path,
        print_to_console=False,
    )

    assert isinstance(result, dict)
    assert "metadata" in result
    assert result["metadata"]["comparison_count"] > 0


def test_analytics_output_structure(tmp_db):
    """Analytics output bevat alle verwachte keys."""
    result = run_analytics(
        db_path=tmp_db.db_path,
        print_to_console=False,
    )

    expected_keys = {
        "metadata", "price_index", "price_per_night",
        "competitive_position", "availability_gaps",
        "seasonal_patterns", "price_changes",
        "recommendations", "comparison_data",
    }
    assert expected_keys.issubset(set(result.keys()))


def test_analytics_metadata(tmp_db):
    """Analytics metadata bevat verplichte velden."""
    result = run_analytics(
        db_path=tmp_db.db_path,
        print_to_console=False,
    )
    meta = result["metadata"]
    assert "scrape_date" in meta
    assert "comparison_count" in meta
    assert "competitors" in meta
    assert isinstance(meta["competitors"], list)


def test_analytics_empty_db(tmp_path):
    """Analytics op lege database geeft lege resultaten."""
    from database import Database
    db = Database(str(tmp_path / "empty.db"))

    result = run_analytics(
        db_path=db.db_path,
        print_to_console=False,
    )
    assert result["metadata"]["comparison_count"] == 0
