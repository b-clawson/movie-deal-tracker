"""
Database layer for subscriber management and search caching.
Supports PostgreSQL (production) and SQLite (local development).
"""

import os
import json
import sqlite3
import secrets
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Check if we're using PostgreSQL (only if DATABASE_URL has a value)
DATABASE_URL = os.getenv("DATABASE_URL", "")
USE_POSTGRES = bool(DATABASE_URL)

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor


@dataclass
class Subscriber:
    """Represents a subscriber."""
    id: int
    email: str
    list_url: str
    created_at: str
    last_checked: Optional[str]
    unsubscribe_token: str
    active: bool
    max_price: float = 20.0
    check_frequency: str = "daily"  # daily, weekly, monthly


class Database:
    """Database for managing subscribers and deal history."""

    def __init__(self, db_path: Optional[Path] = None):
        self.use_postgres = USE_POSTGRES

        if self.use_postgres:
            self.database_url = DATABASE_URL
            logger.info("Using PostgreSQL database")
        else:
            if db_path is None:
                db_path = Path(__file__).parent.parent / "data" / "subscribers.db"
            self.db_path = db_path
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using SQLite database at {self.db_path}")

        self._init_db()

    def _get_connection(self):
        """Get a database connection."""
        if self.use_postgres:
            conn = psycopg2.connect(self.database_url)
            return conn
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn

    def _placeholder(self, index: int = 1) -> str:
        """Return the appropriate placeholder for the database type."""
        return "%s" if self.use_postgres else "?"

    def _init_db(self):
        """Initialize database tables."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            if self.use_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS subscribers (
                        id SERIAL PRIMARY KEY,
                        email TEXT UNIQUE NOT NULL,
                        list_url TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        last_checked TEXT,
                        unsubscribe_token TEXT UNIQUE NOT NULL,
                        active INTEGER DEFAULT 1,
                        max_price REAL DEFAULT 20.0,
                        check_frequency TEXT DEFAULT 'daily'
                    )
                """)

                # Migration: add columns if they don't exist
                cursor.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                      WHERE table_name='subscribers' AND column_name='max_price') THEN
                            ALTER TABLE subscribers ADD COLUMN max_price REAL DEFAULT 20.0;
                        END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                      WHERE table_name='subscribers' AND column_name='check_frequency') THEN
                            ALTER TABLE subscribers ADD COLUMN check_frequency TEXT DEFAULT 'daily';
                        END IF;
                    END $$;
                """)

                # Migration: allow multiple lists per user
                # Drop unique constraint on email, add unique on (email, list_url)
                cursor.execute("""
                    DO $$
                    BEGIN
                        -- Drop the unique constraint on email if it exists
                        IF EXISTS (SELECT 1 FROM information_schema.table_constraints
                                  WHERE table_name='subscribers' AND constraint_type='UNIQUE'
                                  AND constraint_name='subscribers_email_key') THEN
                            ALTER TABLE subscribers DROP CONSTRAINT subscribers_email_key;
                        END IF;
                        -- Add unique constraint on email+list_url if it doesn't exist
                        IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints
                                      WHERE table_name='subscribers'
                                      AND constraint_name='subscribers_email_list_url_key') THEN
                            ALTER TABLE subscribers ADD CONSTRAINT subscribers_email_list_url_key UNIQUE (email, list_url);
                        END IF;
                    END $$;
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notified_deals (
                        id SERIAL PRIMARY KEY,
                        subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
                        deal_hash TEXT NOT NULL,
                        notified_at TEXT NOT NULL,
                        UNIQUE(subscriber_id, deal_hash)
                    )
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_subscribers_active
                    ON subscribers(active)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_notified_deals_subscriber
                    ON notified_deals(subscriber_id)
                """)

                # Search cache table for reducing API calls
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS search_cache (
                        id SERIAL PRIMARY KEY,
                        cache_key TEXT UNIQUE NOT NULL,
                        results_json TEXT NOT NULL,
                        cached_at TIMESTAMP NOT NULL,
                        expires_at TIMESTAMP NOT NULL
                    )
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_search_cache_key
                    ON search_cache(cache_key)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_search_cache_expires
                    ON search_cache(expires_at)
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS subscribers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        email TEXT UNIQUE NOT NULL,
                        list_url TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        last_checked TEXT,
                        unsubscribe_token TEXT UNIQUE NOT NULL,
                        active INTEGER DEFAULT 1,
                        max_price REAL DEFAULT 20.0,
                        check_frequency TEXT DEFAULT 'daily'
                    )
                """)

                # Migration: add columns if they don't exist (SQLite)
                try:
                    cursor.execute("ALTER TABLE subscribers ADD COLUMN max_price REAL DEFAULT 20.0")
                except:
                    pass  # Column already exists
                try:
                    cursor.execute("ALTER TABLE subscribers ADD COLUMN check_frequency TEXT DEFAULT 'daily'")
                except:
                    pass  # Column already exists

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notified_deals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        subscriber_id INTEGER NOT NULL,
                        deal_hash TEXT NOT NULL,
                        notified_at TEXT NOT NULL,
                        FOREIGN KEY (subscriber_id) REFERENCES subscribers(id),
                        UNIQUE(subscriber_id, deal_hash)
                    )
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_subscribers_active
                    ON subscribers(active)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_notified_deals_subscriber
                    ON notified_deals(subscriber_id)
                """)

                # Search cache table for reducing API calls
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS search_cache (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        cache_key TEXT UNIQUE NOT NULL,
                        results_json TEXT NOT NULL,
                        cached_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL
                    )
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_search_cache_key
                    ON search_cache(cache_key)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_search_cache_expires
                    ON search_cache(expires_at)
                """)

            conn.commit()
        finally:
            conn.close()

    def _row_to_subscriber(self, row) -> Subscriber:
        """Convert a database row to a Subscriber object."""
        if self.use_postgres:
            return Subscriber(
                id=row[0],
                email=row[1],
                list_url=row[2],
                created_at=row[3],
                last_checked=row[4],
                unsubscribe_token=row[5],
                active=bool(row[6]),
                max_price=float(row[7]) if row[7] is not None else 20.0,
                check_frequency=row[8] if row[8] else "daily",
            )
        else:
            return Subscriber(
                id=row["id"],
                email=row["email"],
                list_url=row["list_url"],
                created_at=row["created_at"],
                last_checked=row["last_checked"],
                unsubscribe_token=row["unsubscribe_token"],
                active=bool(row["active"]),
                max_price=float(row["max_price"]) if row["max_price"] is not None else 20.0,
                check_frequency=row["check_frequency"] if row["check_frequency"] else "daily",
            )

    def add_subscriber(
        self,
        email: str,
        list_url: str,
        max_price: float = 20.0,
        check_frequency: str = "daily"
    ) -> Optional[Subscriber]:
        """Add a new subscription or update existing one.

        A user can have multiple subscriptions (one per list URL).
        If the same email+list_url combo exists, update preferences.
        """
        token = secrets.token_urlsafe(32)
        now = datetime.now().isoformat()
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Check if this email+list_url combo already exists
            cursor.execute(
                f"SELECT * FROM subscribers WHERE email = {p} AND list_url = {p}",
                (email, list_url)
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing subscription's preferences
                cursor.execute(f"""
                    UPDATE subscribers
                    SET active = 1, max_price = {p}, check_frequency = {p}
                    WHERE email = {p} AND list_url = {p}
                """, (max_price, check_frequency, email, list_url))
                conn.commit()
            else:
                # Create new subscription (same user can have multiple lists)
                cursor.execute(f"""
                    INSERT INTO subscribers (email, list_url, created_at, unsubscribe_token, active, max_price, check_frequency)
                    VALUES ({p}, {p}, {p}, {p}, 1, {p}, {p})
                """, (email, list_url, now, token, max_price, check_frequency))
                conn.commit()

            return self.get_subscription(email, list_url)

        except Exception as e:
            logger.error(f"Failed to add subscriber: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()

    def get_subscription(self, email: str, list_url: str) -> Optional[Subscriber]:
        """Get a specific subscription by email and list URL."""
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM subscribers WHERE email = {p} AND list_url = {p}",
                (email, list_url)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_subscriber(row)
            return None
        finally:
            conn.close()

    def get_subscriber_by_email(self, email: str) -> Optional[Subscriber]:
        """Get subscriber by email."""
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM subscribers WHERE email = {p}",
                (email,)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_subscriber(row)
            return None
        finally:
            conn.close()

    def get_subscriber_by_id(self, subscriber_id: int) -> Optional[Subscriber]:
        """Get subscriber by ID."""
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM subscribers WHERE id = {p}",
                (subscriber_id,)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_subscriber(row)
            return None
        finally:
            conn.close()

    def get_subscriber_by_token(self, token: str) -> Optional[Subscriber]:
        """Get subscriber by unsubscribe token."""
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM subscribers WHERE unsubscribe_token = {p}",
                (token,)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_subscriber(row)
            return None
        finally:
            conn.close()

    def get_active_subscribers(self) -> List[Subscriber]:
        """Get all active subscribers."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM subscribers WHERE active = 1")
            rows = cursor.fetchall()

            return [self._row_to_subscriber(row) for row in rows]
        finally:
            conn.close()

    def unsubscribe(self, token: str) -> bool:
        """Unsubscribe a user by their token."""
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE subscribers
                SET active = 0
                WHERE unsubscribe_token = {p}
            """, (token,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def update_last_checked(self, subscriber_id: int):
        """Update the last_checked timestamp for a subscriber."""
        now = datetime.now().isoformat()
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE subscribers
                SET last_checked = {p}
                WHERE id = {p}
            """, (now, subscriber_id))
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to update last_checked: {e}")
            conn.rollback()
        finally:
            conn.close()

    def is_deal_notified(self, subscriber_id: int, deal_hash: str) -> bool:
        """Check if a deal has already been notified to this subscriber."""
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT 1 FROM notified_deals
                WHERE subscriber_id = {p} AND deal_hash = {p}
            """, (subscriber_id, deal_hash))
            row = cursor.fetchone()
            return row is not None
        finally:
            conn.close()

    def mark_deal_notified(self, subscriber_id: int, deal_hash: str):
        """Mark a deal as notified for a subscriber."""
        now = datetime.now().isoformat()
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            if self.use_postgres:
                cursor.execute(f"""
                    INSERT INTO notified_deals (subscriber_id, deal_hash, notified_at)
                    VALUES ({p}, {p}, {p})
                    ON CONFLICT (subscriber_id, deal_hash) DO NOTHING
                """, (subscriber_id, deal_hash, now))
            else:
                cursor.execute(f"""
                    INSERT OR IGNORE INTO notified_deals (subscriber_id, deal_hash, notified_at)
                    VALUES ({p}, {p}, {p})
                """, (subscriber_id, deal_hash, now))
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to mark deal notified: {e}")
            conn.rollback()
        finally:
            conn.close()

    def filter_new_deals(self, subscriber_id: int, deals: list) -> list:
        """Filter deals to only those not yet notified to this subscriber."""
        new_deals = []
        for deal in deals:
            if not self.is_deal_notified(subscriber_id, deal.deal_hash):
                new_deals.append(deal)
                self.mark_deal_notified(subscriber_id, deal.deal_hash)
        return new_deals

    def get_subscriber_count(self) -> int:
        """Get total count of active subscribers."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM subscribers WHERE active = 1")
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def get_all_subscribers(self) -> List[Subscriber]:
        """Get all subscribers (including inactive)."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM subscribers ORDER BY created_at DESC")
            rows = cursor.fetchall()
            return [self._row_to_subscriber(row) for row in rows]
        finally:
            conn.close()

    def get_notified_deals_count(self, subscriber_id: int) -> int:
        """Get count of deals notified to a subscriber."""
        p = self._placeholder()
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM notified_deals WHERE subscriber_id = {p}",
                (subscriber_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    # ===== Search Cache Methods =====

    def _make_cache_key(self, movie_title: str, max_price: float) -> str:
        """Generate a cache key for a search query."""
        # Normalize the title for consistent caching
        normalized_title = movie_title.lower().strip()
        return f"{normalized_title}|{max_price:.2f}"

    def get_cached_results(
        self,
        movie_title: str,
        max_price: float
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached search results if they exist and haven't expired.

        Args:
            movie_title: Movie title that was searched
            max_price: Max price used in search

        Returns:
            List of deal dictionaries if cache hit, None if miss/expired
        """
        cache_key = self._make_cache_key(movie_title, max_price)
        now = datetime.now().isoformat()
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT results_json FROM search_cache
                WHERE cache_key = {p} AND expires_at > {p}
            """, (cache_key, now))
            row = cursor.fetchone()

            if row:
                results_json = row[0] if self.use_postgres else row["results_json"]
                logger.debug(f"Cache hit for '{movie_title}'")
                return json.loads(results_json)

            logger.debug(f"Cache miss for '{movie_title}'")
            return None
        except Exception as e:
            logger.error(f"Failed to get cached results: {e}")
            return None
        finally:
            conn.close()

    def set_cached_results(
        self,
        movie_title: str,
        max_price: float,
        results: List[Dict[str, Any]],
        ttl_hours: int = 48
    ):
        """
        Cache search results.

        Args:
            movie_title: Movie title that was searched
            max_price: Max price used in search
            results: List of deal dictionaries to cache
            ttl_hours: Time-to-live in hours (0 means don't cache)
        """
        if ttl_hours <= 0:
            logger.debug(f"Skipping cache for '{movie_title}' (TTL is 0)")
            return

        cache_key = self._make_cache_key(movie_title, max_price)
        now = datetime.now()
        expires_at = now + timedelta(hours=ttl_hours)
        results_json = json.dumps(results)
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            if self.use_postgres:
                cursor.execute(f"""
                    INSERT INTO search_cache (cache_key, results_json, cached_at, expires_at)
                    VALUES ({p}, {p}, {p}, {p})
                    ON CONFLICT (cache_key) DO UPDATE SET
                        results_json = EXCLUDED.results_json,
                        cached_at = EXCLUDED.cached_at,
                        expires_at = EXCLUDED.expires_at
                """, (cache_key, results_json, now.isoformat(), expires_at.isoformat()))
            else:
                cursor.execute(f"""
                    INSERT OR REPLACE INTO search_cache (cache_key, results_json, cached_at, expires_at)
                    VALUES ({p}, {p}, {p}, {p})
                """, (cache_key, results_json, now.isoformat(), expires_at.isoformat()))

            conn.commit()
            logger.debug(f"Cached results for '{movie_title}' (expires in {ttl_hours}h)")
        except Exception as e:
            logger.error(f"Failed to cache results: {e}")
            conn.rollback()
        finally:
            conn.close()

    def clear_expired_cache(self) -> int:
        """
        Remove expired cache entries.

        Returns:
            Number of entries removed
        """
        now = datetime.now().isoformat()
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM search_cache WHERE expires_at < {p}", (now,))
            deleted = cursor.rowcount
            conn.commit()
            if deleted > 0:
                logger.info(f"Cleared {deleted} expired cache entries")
            return deleted
        except Exception as e:
            logger.error(f"Failed to clear expired cache: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def clear_all_cache(self) -> int:
        """
        Clear all cache entries (useful for forcing fresh searches).

        Returns:
            Number of entries removed
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM search_cache")
            deleted = cursor.rowcount
            conn.commit()
            logger.info(f"Cleared all {deleted} cache entries")
            return deleted
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        now = datetime.now().isoformat()
        p = self._placeholder()

        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Total entries
            cursor.execute("SELECT COUNT(*) FROM search_cache")
            total = cursor.fetchone()[0]

            # Valid (non-expired) entries
            cursor.execute(f"SELECT COUNT(*) FROM search_cache WHERE expires_at > {p}", (now,))
            valid = cursor.fetchone()[0]

            # Expired entries
            expired = total - valid

            return {
                "total_entries": total,
                "valid_entries": valid,
                "expired_entries": expired,
            }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {"error": str(e)}
        finally:
            conn.close()


# Global database instance
_db: Optional[Database] = None


def get_db() -> Database:
    """Get or create the global database instance."""
    global _db
    if _db is None:
        _db = Database()
    return _db
