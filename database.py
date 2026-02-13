"""Database module for storing competitor price data."""

import sqlite3
import os
from datetime import datetime
from pathlib import Path


class Database:
    """SQLite database for competitor price storage and retrieval."""

    def __init__(self, db_path: str = "data/concurrentiecheck.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    competitor_name TEXT NOT NULL,
                    accommodation_type TEXT NOT NULL,
                    check_in_date TEXT NOT NULL,
                    check_out_date TEXT NOT NULL,
                    price REAL,
                    available INTEGER NOT NULL DEFAULT 1,
                    min_nights INTEGER,
                    special_offers TEXT,
                    persons INTEGER DEFAULT 4,
                    scrape_timestamp TEXT NOT NULL,
                    scrape_date TEXT NOT NULL,
                    UNIQUE(competitor_name, check_in_date, check_out_date, scrape_date)
                );

                CREATE INDEX IF NOT EXISTS idx_prices_competitor
                    ON prices(competitor_name);
                CREATE INDEX IF NOT EXISTS idx_prices_checkin
                    ON prices(check_in_date);
                CREATE INDEX IF NOT EXISTS idx_prices_scrape_date
                    ON prices(scrape_date);

                CREATE TABLE IF NOT EXISTS scrape_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    competitor_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    records_scraped INTEGER DEFAULT 0,
                    error_message TEXT,
                    duration_seconds REAL
                );
            """)
            conn.commit()
        finally:
            conn.close()

    def save_price(self, competitor_name: str, accommodation_type: str,
                   check_in_date: str, check_out_date: str, price: float,
                   available: bool = True, min_nights: int = None,
                   special_offers: str = None, persons: int = 4):
        """Save a single price record. Updates if same competitor+dates+scrape_date exists."""
        now = datetime.now()
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO prices (
                    competitor_name, accommodation_type, check_in_date,
                    check_out_date, price, available, min_nights,
                    special_offers, persons, scrape_timestamp, scrape_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(competitor_name, check_in_date, check_out_date, scrape_date)
                DO UPDATE SET
                    price = CASE WHEN excluded.price IS NOT NULL THEN excluded.price ELSE prices.price END,
                    available = CASE WHEN excluded.price IS NOT NULL THEN excluded.available ELSE prices.available END,
                    min_nights = excluded.min_nights,
                    special_offers = excluded.special_offers,
                    accommodation_type = excluded.accommodation_type,
                    persons = excluded.persons,
                    scrape_timestamp = excluded.scrape_timestamp
            """, (
                competitor_name, accommodation_type, check_in_date,
                check_out_date, price, int(available), min_nights,
                special_offers, persons, now.isoformat(), now.strftime("%Y-%m-%d")
            ))
            conn.commit()
        finally:
            conn.close()

    def save_prices_batch(self, records: list[dict]):
        """Save multiple price records at once."""
        for record in records:
            self.save_price(**record)

    def log_scrape(self, competitor_name: str, status: str,
                   records_scraped: int = 0, error_message: str = None,
                   duration_seconds: float = None):
        """Log a scrape attempt."""
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO scrape_log (
                    competitor_name, timestamp, status,
                    records_scraped, error_message, duration_seconds
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                competitor_name, datetime.now().isoformat(), status,
                records_scraped, error_message, duration_seconds
            ))
            conn.commit()
        finally:
            conn.close()

    def get_prices(self, competitor_name: str = None,
                   check_in_from: str = None, check_in_to: str = None,
                   scrape_date: str = None) -> list[dict]:
        """Query prices with optional filters."""
        query = "SELECT * FROM prices WHERE 1=1"
        params = []

        if competitor_name:
            query += " AND competitor_name = ?"
            params.append(competitor_name)
        if check_in_from:
            query += " AND check_in_date >= ?"
            params.append(check_in_from)
        if check_in_to:
            query += " AND check_in_date <= ?"
            params.append(check_in_to)
        if scrape_date:
            query += " AND scrape_date = ?"
            params.append(scrape_date)

        query += " ORDER BY check_in_date, competitor_name"

        conn = self._get_conn()
        try:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_latest_prices(self, competitor_name: str = None) -> list[dict]:
        """Get the most recent prices for each competitor and date combination."""
        query = """
            SELECT p.* FROM prices p
            INNER JOIN (
                SELECT competitor_name, check_in_date, check_out_date,
                       MAX(scrape_timestamp) as max_ts
                FROM prices
                GROUP BY competitor_name, check_in_date, check_out_date
            ) latest ON p.competitor_name = latest.competitor_name
                AND p.check_in_date = latest.check_in_date
                AND p.check_out_date = latest.check_out_date
                AND p.scrape_timestamp = latest.max_ts
            WHERE 1=1
        """
        params = []
        if competitor_name:
            query += " AND p.competitor_name = ?"
            params.append(competitor_name)

        query += " ORDER BY p.check_in_date, p.competitor_name"

        conn = self._get_conn()
        try:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_price_history(self, competitor_name: str,
                          check_in_date: str) -> list[dict]:
        """Get price history for a specific competitor and check-in date."""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT * FROM prices
                WHERE competitor_name = ? AND check_in_date = ?
                ORDER BY scrape_timestamp
            """, (competitor_name, check_in_date)).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_latest_scrape_date(self) -> str | None:
        """Get the most recent scrape_date in the database."""
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT MAX(scrape_date) FROM prices").fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def get_comparison_data(self, scrape_date: str,
                            durations: list[int] = None) -> list[dict]:
        """Get all prices for a scrape date, filtered to canonical durations."""
        if durations is None:
            durations = [2, 3, 4, 7]

        placeholders = ",".join("?" * len(durations))
        query = f"""
            SELECT
                competitor_name,
                accommodation_type,
                check_in_date,
                check_out_date,
                price,
                available,
                special_offers,
                CAST(ROUND(julianday(check_out_date) - julianday(check_in_date)) AS INTEGER) AS nights
            FROM prices
            WHERE scrape_date = ?
              AND price IS NOT NULL
              AND CAST(ROUND(julianday(check_out_date) - julianday(check_in_date)) AS INTEGER) IN ({placeholders})
            ORDER BY check_in_date, competitor_name
        """
        conn = self._get_conn()
        try:
            rows = conn.execute(query, [scrape_date] + durations).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_scrape_stats(self, days: int = 30) -> list[dict]:
        """Get scrape success/failure stats for the last N days."""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT competitor_name, status, COUNT(*) as count,
                       AVG(records_scraped) as avg_records,
                       AVG(duration_seconds) as avg_duration
                FROM scrape_log
                WHERE timestamp >= datetime('now', ? || ' days')
                GROUP BY competitor_name, status
                ORDER BY competitor_name
            """, (f"-{days}",)).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_scrape_summary(self, date: str = None) -> dict:
        """Get scrape summary for a given date (default: today).

        Returns dict keyed by competitor_name with latest log entry:
            {competitor: {status, records_scraped, duration_seconds, timestamp}}
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT sl.competitor_name, sl.status, sl.records_scraped,
                       sl.duration_seconds, sl.error_message, sl.timestamp
                FROM scrape_log sl
                INNER JOIN (
                    SELECT competitor_name, MAX(timestamp) as max_ts
                    FROM scrape_log
                    WHERE DATE(timestamp) = ?
                    GROUP BY competitor_name
                ) latest ON sl.competitor_name = latest.competitor_name
                    AND sl.timestamp = latest.max_ts
                ORDER BY sl.competitor_name
            """, (date,)).fetchall()
            return {row["competitor_name"]: dict(row) for row in rows}
        finally:
            conn.close()

    def get_all_scrape_dates(self) -> list[str]:
        """Get all distinct scrape dates, most recent first."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT DISTINCT scrape_date FROM prices ORDER BY scrape_date DESC"
            ).fetchall()
            return [row[0] for row in rows]
        finally:
            conn.close()
