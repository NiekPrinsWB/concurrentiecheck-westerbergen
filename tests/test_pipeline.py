"""Smoke tests voor de dagelijkse pipeline."""

import sys
from unittest.mock import patch

from run_daily import run_pipeline, EXIT_SUCCESS


def test_dry_run(sample_config, tmp_db):
    """Dry-run modus draait zonder fouten."""
    config_path, config = sample_config
    # Point config to the temp database
    config["general"]["database_path"] = tmp_db.db_path

    exit_code = run_pipeline(config=config, dry_run=True)
    assert exit_code == EXIT_SUCCESS


def test_skip_scrape(sample_config, tmp_db):
    """Skip-scrape modus draait analytics op bestaande data."""
    config_path, config = sample_config
    config["general"]["database_path"] = tmp_db.db_path
    # Disable dashboard to avoid file creation issues
    config["automation"]["generate_dashboard"] = False

    exit_code = run_pipeline(config=config, skip_scrape=True)
    assert exit_code == EXIT_SUCCESS
