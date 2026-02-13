"""Data loading met Streamlit caching."""

import os
import sys
import streamlit as st

# Voeg projectroot toe aan sys.path
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from database import Database
from analytics import run_analytics


def get_db_path() -> str:
    """Bepaal database pad."""
    return os.path.join(_project_root, "data", "concurrentiecheck.db")


@st.cache_data(ttl=300)
def load_analytics(db_path: str, scrape_date: str = None) -> dict:
    """Laad analytics resultaten met 5 min cache."""
    return run_analytics(
        db_path=db_path,
        scrape_date=scrape_date,
        print_to_console=False,
    )


@st.cache_data(ttl=300)
def get_available_dates(db_path: str) -> list[str]:
    """Beschikbare scrape-datums (meest recent eerst)."""
    db = Database(db_path)
    return db.get_all_scrape_dates()


@st.cache_data(ttl=60)
def get_scrape_status(db_path: str, date: str = None) -> dict:
    """Scrape-status per concurrent."""
    db = Database(db_path)
    return db.get_scrape_summary(date)


@st.cache_data(ttl=300)
def get_scrape_history(db_path: str) -> list[dict]:
    """Scrape-statistieken van de laatste 30 dagen."""
    db = Database(db_path)
    return db.get_scrape_stats(days=30)
