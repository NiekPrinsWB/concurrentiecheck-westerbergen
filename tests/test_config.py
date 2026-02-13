"""Smoke tests voor configuratie laden."""

import yaml


def test_load_config(sample_config):
    """Test dat config correct laadt vanuit YAML."""
    config_path, config = sample_config
    assert config["general"]["project_name"] == "Test Concurrentiecheck"
    assert config["scraping"]["default_persons"] == 4


def test_load_config_from_file(sample_config):
    """Test dat config vanuit bestand geladen kan worden."""
    config_path, _ = sample_config
    with open(config_path, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    assert "general" in loaded
    assert "competitors" in loaded
    assert "automation" in loaded


def test_production_config():
    """Test dat de productie config geldig is."""
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Verplichte secties
    assert "general" in config
    assert "scraping" in config
    assert "competitors" in config
    assert "dashboard" in config
    assert "automation" in config

    # Database pad
    assert "database_path" in config["general"]

    # Minstens 1 enabled competitor
    enabled = [
        k for k, v in config["competitors"].items()
        if v.get("enabled", True)
    ]
    assert len(enabled) >= 1

    # Automation defaults
    auto = config["automation"]
    assert "schedule_time" in auto
    assert "retry_failed" in auto
