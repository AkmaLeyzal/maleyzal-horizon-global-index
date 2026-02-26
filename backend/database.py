"""
MongoDB Atlas connection manager for MHGI.

Handles a single timeseries collection (Standard_Index) with timeField=Date.
Also maintains regular collections for stock_info and engine_state.
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
DB_NAME = os.getenv("MONGODB_DB", "Maleyzal_Horizon_Global_Index")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION", "Standard_Index")
TIME_FIELD = os.getenv("MONGODB_TIMEFIELD", "Date")


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
            await self.client.admin.command("ping")
            self.db = self.client[DB_NAME]

            # Create indexes for regular collections
            await self._create_indexes()

            logger.info(f"✅ Connected to MongoDB Atlas")
            logger.info(f"   Database: {DB_NAME}")
            logger.info(f"   Timeseries collection: {COLLECTION_NAME} (timeField: {TIME_FIELD})")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            self.client = None
            self.db = None
            return False

    async def _create_indexes(self):
        """Create indexes for non-timeseries collections."""
        try:
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

    # ─────────── STOCK PRICES (Timeseries Collection) ───────────

    async def get_stock_prices(self, ticker: str, start_date: str = None) -> list[dict]:
        """Get stored stock prices from the timeseries collection."""
        if not self.is_connected:
            return []
        try:
            query = {"Ticker": ticker}
            if start_date:
                query[TIME_FIELD] = {"$gte": datetime.strptime(start_date, "%Y-%m-%d")}

            cursor = self.db[COLLECTION_NAME].find(
                query, {"_id": 0}
            ).sort(TIME_FIELD, 1)
            docs = await cursor.to_list(length=50000)

            # Convert Date back to string for internal use
            for doc in docs:
                if TIME_FIELD in doc and isinstance(doc[TIME_FIELD], datetime):
                    doc["date"] = doc[TIME_FIELD].strftime("%Y-%m-%d")
            return docs
        except Exception as e:
            logger.error(f"Error loading stock prices for {ticker}: {e}")
            return []

    async def get_last_price_date(self, ticker: str) -> Optional[str]:
        """Get the last stored price date for a ticker."""
        if not self.is_connected:
            return None
        try:
            doc = await self.db[COLLECTION_NAME].find_one(
                {"Ticker": ticker},
                {TIME_FIELD: 1, "_id": 0},
                sort=[(TIME_FIELD, -1)],
            )
            if doc and TIME_FIELD in doc:
                if isinstance(doc[TIME_FIELD], datetime):
                    return doc[TIME_FIELD].strftime("%Y-%m-%d")
                return str(doc[TIME_FIELD])
            return None
        except Exception as e:
            logger.error(f"Error getting last price date for {ticker}: {e}")
            return None

    async def save_stock_prices(self, ticker: str, prices: list[dict]):
        """
        Insert stock prices into the timeseries collection.
        Checks for existing dates to avoid duplicates.
        """
        if not self.is_connected or not prices:
            return

        try:
            # Get existing dates for this ticker
            existing_dates = set()
            cursor = self.db[COLLECTION_NAME].find(
                {"Ticker": ticker},
                {TIME_FIELD: 1, "_id": 0},
            )
            async for doc in cursor:
                if TIME_FIELD in doc:
                    if isinstance(doc[TIME_FIELD], datetime):
                        existing_dates.add(doc[TIME_FIELD].strftime("%Y-%m-%d"))
                    else:
                        existing_dates.add(str(doc[TIME_FIELD]))

            # Filter out duplicates and build documents
            docs_to_insert = []
            for p in prices:
                date_str = p.get("date", "")
                if date_str in existing_dates:
                    continue

                doc = {
                    TIME_FIELD: datetime.strptime(date_str, "%Y-%m-%d"),
                    "Ticker": ticker,
                    "Open": p.get("open", 0),
                    "High": p.get("high", 0),
                    "Low": p.get("low", 0),
                    "Close": p.get("close", 0),
                    "Volume": p.get("volume", 0),
                }
                docs_to_insert.append(doc)

            if docs_to_insert:
                await self.db[COLLECTION_NAME].insert_many(docs_to_insert, ordered=False)
                logger.info(f"  {ticker}: Inserted {len(docs_to_insert)} prices (skipped {len(prices) - len(docs_to_insert)} existing)")
            else:
                logger.info(f"  {ticker}: All prices already in DB, nothing to insert")

        except Exception as e:
            logger.error(f"Error saving stock prices for {ticker}: {e}")

    async def get_all_tickers_summary(self) -> dict:
        """Get a summary of stored data: {ticker: {count, first_date, last_date}}."""
        if not self.is_connected:
            return {}
        try:
            pipeline = [
                {"$group": {
                    "_id": "$Ticker",
                    "count": {"$sum": 1},
                    "first_date": {"$min": f"${TIME_FIELD}"},
                    "last_date": {"$max": f"${TIME_FIELD}"},
                }},
                {"$sort": {"_id": 1}},
            ]
            cursor = self.db[COLLECTION_NAME].aggregate(pipeline)
            result = {}
            async for doc in cursor:
                ticker = doc["_id"]
                if ticker:
                    result[ticker] = {
                        "count": doc["count"],
                        "first_date": doc["first_date"].strftime("%Y-%m-%d") if isinstance(doc["first_date"], datetime) else str(doc["first_date"]),
                        "last_date": doc["last_date"].strftime("%Y-%m-%d") if isinstance(doc["last_date"], datetime) else str(doc["last_date"]),
                    }
            return result
        except Exception as e:
            logger.error(f"Error getting tickers summary: {e}")
            return {}

    # ─────────── INDEX HISTORY (regular collection) ───────────

    async def save_index_history_entry(self, entry: dict):
        """Upsert a single index history entry."""
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
        """Load all index history."""
        if not self.is_connected:
            return []
        try:
            cursor = self.db.index_history.find({}, {"_id": 0}).sort("date", 1)
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
        """Load stock info (returns None if stale >24h)."""
        if not self.is_connected:
            return None
        try:
            doc = await self.db.stock_info.find_one({"ticker": ticker}, {"_id": 0})
            if doc:
                updated = doc.get("updated_at")
                if updated:
                    updated_dt = datetime.fromisoformat(updated)
                    age_hours = (datetime.now() - updated_dt).total_seconds() / 3600
                    if age_hours > 24:
                        return None
                return doc
            return None
        except Exception as e:
            logger.error(f"Error loading stock info for {ticker}: {e}")
            return None

    # ─────────── ENGINE STATE ───────────

    async def save_engine_state(self, state: dict):
        """Save engine state."""
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
            return await self.db.engine_state.find_one(
                {"key": "mhgi_engine"}, {"_id": 0}
            )
        except Exception as e:
            logger.error(f"Error loading engine state: {e}")
            return None


# Singleton
mongodb = MongoDBManager()
