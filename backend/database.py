"""
MongoDB Atlas connection manager for MHGI.

Collections:
- index_history  : Daily index values (OHLC, divisor, ff_mcap)
- stock_prices   : Historical daily prices per ticker
- stock_info     : Shares outstanding, sector, etc.
- engine_state   : Divisor, last calculation date
"""
import logging
import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

logger = logging.getLogger("mhgi.database")

MONGODB_URI = os.getenv("MONGODB_URI", "")
DB_NAME = os.getenv("MONGODB_DB", "mhgi")


class MongoDBManager:
    """Async MongoDB connection manager using motor."""

    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None

    async def connect(self):
        """Connect to MongoDB Atlas."""
        if not MONGODB_URI:
            logger.warning("MONGODB_URI not set — running without database persistence")
            return False

        try:
            self.client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
            # Ping to verify connection
            await self.client.admin.command("ping")
            self.db = self.client[DB_NAME]

            # Create indexes
            await self._create_indexes()

            logger.info(f"✅ Connected to MongoDB Atlas (database: {DB_NAME})")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            self.client = None
            self.db = None
            return False

    async def _create_indexes(self):
        """Create collection indexes for performance."""
        try:
            await self.db.index_history.create_index("date", unique=True)
            await self.db.stock_prices.create_index(
                [("ticker", 1), ("date", 1)], unique=True
            )
            await self.db.stock_info.create_index("ticker", unique=True)
            await self.db.engine_state.create_index("key", unique=True)
            logger.info("  MongoDB indexes created/verified")
        except Exception as e:
            logger.warning(f"  Index creation warning: {e}")

    async def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    @property
    def is_connected(self) -> bool:
        return self.db is not None

    # ─────────── INDEX HISTORY ───────────

    async def save_index_history_entry(self, entry: dict):
        """Upsert a single index history entry by date."""
        if not self.is_connected:
            return
        try:
            await self.db.index_history.update_one(
                {"date": entry["date"]},
                {"$set": entry},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error saving index history: {e}")

    async def save_index_history_bulk(self, entries: list[dict]):
        """Bulk upsert index history entries."""
        if not self.is_connected or not entries:
            return
        try:
            from pymongo import UpdateOne

            ops = [
                UpdateOne({"date": e["date"]}, {"$set": e}, upsert=True)
                for e in entries
            ]
            result = await self.db.index_history.bulk_write(ops, ordered=False)
            logger.info(
                f"  Saved {result.upserted_count} new + {result.modified_count} updated index history entries"
            )
        except Exception as e:
            logger.error(f"Error in bulk save index history: {e}")

    async def load_index_history(self) -> list[dict]:
        """Load all index history, sorted by date."""
        if not self.is_connected:
            return []
        try:
            cursor = self.db.index_history.find(
                {}, {"_id": 0}
            ).sort("date", 1)
            return await cursor.to_list(length=10000)
        except Exception as e:
            logger.error(f"Error loading index history: {e}")
            return []

    async def get_last_history_date(self) -> Optional[str]:
        """Get the last date in index history."""
        if not self.is_connected:
            return None
        try:
            doc = await self.db.index_history.find_one(
                {}, {"date": 1, "_id": 0}, sort=[("date", -1)]
            )
            return doc["date"] if doc else None
        except Exception as e:
            logger.error(f"Error getting last history date: {e}")
            return None

    # ─────────── STOCK PRICES ───────────

    async def save_stock_prices(self, ticker: str, prices: list[dict]):
        """Bulk upsert daily prices for a ticker."""
        if not self.is_connected or not prices:
            return
        try:
            from pymongo import UpdateOne

            ops = [
                UpdateOne(
                    {"ticker": ticker, "date": p["date"]},
                    {"$set": {**p, "ticker": ticker}},
                    upsert=True,
                )
                for p in prices
            ]
            await self.db.stock_prices.bulk_write(ops, ordered=False)
        except Exception as e:
            logger.error(f"Error saving stock prices for {ticker}: {e}")

    async def get_stock_prices(self, ticker: str, start_date: str = None) -> list[dict]:
        """Get stored stock prices for a ticker, optionally from a start date."""
        if not self.is_connected:
            return []
        try:
            query = {"ticker": ticker}
            if start_date:
                query["date"] = {"$gte": start_date}

            cursor = self.db.stock_prices.find(
                query, {"_id": 0}
            ).sort("date", 1)
            return await cursor.to_list(length=10000)
        except Exception as e:
            logger.error(f"Error loading stock prices for {ticker}: {e}")
            return []

    async def get_last_price_date(self, ticker: str) -> Optional[str]:
        """Get the last stored price date for a ticker."""
        if not self.is_connected:
            return None
        try:
            doc = await self.db.stock_prices.find_one(
                {"ticker": ticker}, {"date": 1, "_id": 0}, sort=[("date", -1)]
            )
            return doc["date"] if doc else None
        except Exception as e:
            logger.error(f"Error getting last price date for {ticker}: {e}")
            return None

    # ─────────── STOCK INFO ───────────

    async def save_stock_info(self, ticker: str, info: dict):
        """Upsert stock info."""
        if not self.is_connected:
            return
        try:
            await self.db.stock_info.update_one(
                {"ticker": ticker},
                {"$set": {**info, "ticker": ticker, "updated_at": datetime.now().isoformat()}},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error saving stock info for {ticker}: {e}")

    async def load_stock_info(self, ticker: str) -> Optional[dict]:
        """Load stock info, returns None if not found or stale (>24h)."""
        if not self.is_connected:
            return None
        try:
            doc = await self.db.stock_info.find_one(
                {"ticker": ticker}, {"_id": 0}
            )
            if doc:
                # Check if info is fresh (< 24 hours)
                updated = doc.get("updated_at")
                if updated:
                    updated_dt = datetime.fromisoformat(updated)
                    age_hours = (datetime.now() - updated_dt).total_seconds() / 3600
                    if age_hours > 24:
                        return None  # Stale, needs refresh
                return doc
            return None
        except Exception as e:
            logger.error(f"Error loading stock info for {ticker}: {e}")
            return None

    # ─────────── ENGINE STATE ───────────

    async def save_engine_state(self, state: dict):
        """Save engine state (divisor, etc.)."""
        if not self.is_connected:
            return
        try:
            await self.db.engine_state.update_one(
                {"key": "mhgi_engine"},
                {"$set": {**state, "key": "mhgi_engine", "updated_at": datetime.now().isoformat()}},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error saving engine state: {e}")

    async def load_engine_state(self) -> Optional[dict]:
        """Load engine state."""
        if not self.is_connected:
            return None
        try:
            doc = await self.db.engine_state.find_one(
                {"key": "mhgi_engine"}, {"_id": 0}
            )
            return doc
        except Exception as e:
            logger.error(f"Error loading engine state: {e}")
            return None


# Singleton
mongodb = MongoDBManager()
