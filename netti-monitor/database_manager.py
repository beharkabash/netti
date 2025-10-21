
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_file="data/nettiauto.db"):
        import os
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        self.db_file = db_file
        self.conn = None
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.create_tables()
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.conn = None

    def create_tables(self):
        if not self.conn:
            return
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS listings (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    price INTEGER,
                    url TEXT,
                    first_seen TEXT,
                    last_seen TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    listing_id TEXT,
                    price INTEGER,
                    timestamp TEXT,
                    FOREIGN KEY(listing_id) REFERENCES listings(id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    method TEXT,
                    success BOOLEAN,
                    duration REAL,
                    listings_found INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    message TEXT,
                    success BOOLEAN
                )
            """)
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")

    def get_listing_price(self, listing_id: str) -> int | None:
        if not self.conn:
            return None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT price FROM prices WHERE listing_id = ? ORDER BY timestamp DESC LIMIT 1", (listing_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Error getting listing price: {e}")
            return None

    def save_price_change(self, listing_id: str, old_price: int, new_price: int):
        if not self.conn:
            return
        try:
            cursor = self.conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute("INSERT INTO prices (listing_id, price, timestamp) VALUES (?, ?, ?)", (listing_id, new_price, timestamp))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error saving price change: {e}")

    def log_method_attempt(self, method: str, success: bool, duration: float, listings_found: int):
        if not self.conn:
            return
        try:
            cursor = self.conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute(
                "INSERT INTO logs (timestamp, method, success, duration, listings_found) VALUES (?, ?, ?, ?, ?)",
                (timestamp, method, success, duration, listings_found)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error logging method attempt: {e}")

    def log_notification(self, message: str, success: bool):
        if not self.conn:
            return
        try:
            cursor = self.conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute("INSERT INTO notifications (timestamp, message, success) VALUES (?, ?, ?)", (timestamp, message, success))
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error logging notification: {e}")

    def save_seen_listing(self, listing_id: str, title: str, price: str, url: str):
        if not self.conn:
            return
        try:
            cursor = self.conn.cursor()
            now = datetime.now().isoformat()
            # price is a string like '12 345 €', so we need to parse it
            price_int = int(''.join(filter(str.isdigit, price))) if price else 0
            
            cursor.execute("SELECT id FROM listings WHERE id = ?", (listing_id,))
            result = cursor.fetchone()
            if result:
                cursor.execute("UPDATE listings SET last_seen = ? WHERE id = ?", (now, listing_id))
            else:
                cursor.execute("INSERT INTO listings (id, title, price, url, first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?)",
                               (listing_id, title, price_int, url, now, now))
            
            # Also save the initial price
            self.save_price_change(listing_id, 0, price_int)
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error saving seen listing: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

def get_database_manager():
    """Returns a database manager instance."""
    return DatabaseManager()
