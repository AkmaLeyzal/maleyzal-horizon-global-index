"""
Seed Database — One-time script to populate MongoDB Atlas with full historical
stock price data for all MHGI constituents.

Usage:
    cd backend
    python seed_database.py

This fetches the maximum available history from Yahoo Finance for every
constituent and inserts it into the MongoDB timeseries collection.
Existing dates are automatically skipped (no duplicates).

Subsequent runs of the main application will only fetch NEW data.
"""
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import yfinance as yf
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mhgi.seed")


async def seed_database():
    """Main seeding function."""
    from database import mongodb

    # ── Load config ──
    config_path = Path(__file__).parent / "constituents.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    tickers = list(config.get("constituents", {}).keys())
    logger.info("=" * 70)
    logger.info("  MHGI Database Seeder")
    logger.info(f"  Stocks: {len(tickers)}")
    logger.info(f"  Strategy: Fetch max history → Insert to MongoDB Atlas")
    logger.info("=" * 70)

    # ── Connect to MongoDB ──
    connected = await mongodb.connect()
    if not connected:
        logger.error("Failed to connect to MongoDB Atlas. Check your .env credentials.")
        sys.exit(1)

    # ── Check existing data ──
    summary = await mongodb.get_all_tickers_summary()
    if summary:
        logger.info(f"\n  Existing data in MongoDB: {len(summary)} tickers")
        for ticker, info in sorted(summary.items()):
            logger.info(
                f"    {ticker:<12} | {info['count']:>5} records | "
                f"{info['first_date']} → {info['last_date']}"
            )
    else:
        logger.info("  No existing data in MongoDB — fresh start")

    # ── Fetch and insert ──
    total_inserted = 0
    total_skipped = 0
    failed_tickers = []

    # Process in batches of 5 to avoid overwhelming yfinance
    batch_size = 5
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        logger.info(f"\n── Batch {batch_num}/{total_batches}: {', '.join([t.replace('.JK', '') for t in batch])} ──")

        for ticker in batch:
            short = ticker.replace(".JK", "")
            try:
                # Check last date in MongoDB
                last_date = await mongodb.get_last_price_date(ticker)

                if last_date:
                    # Incremental: only fetch from day after last stored date
                    from datetime import timedelta
                    start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    today = datetime.now().strftime("%Y-%m-%d")

                    if start > today:
                        logger.info(f"  {short:<8} ✓ Already up to date (last: {last_date})")
                        total_skipped += 1
                        continue

                    logger.info(f"  {short:<8} Fetching from {start} to {today}...")
                    data = yf.download(
                        ticker, start=start, end=today,
                        interval="1d", progress=False, threads=True,
                    )
                else:
                    # Full fetch
                    logger.info(f"  {short:<8} Fetching full history (max)...")
                    data = yf.download(
                        ticker, period="max", interval="1d",
                        progress=False, threads=True,
                    )

                if data.empty:
                    logger.warning(f"  {short:<8} ✗ No data returned from yfinance")
                    failed_tickers.append(ticker)
                    continue

                # Flatten MultiIndex columns (yfinance returns multi-level for single ticker too)
                if isinstance(data.columns, pd.MultiIndex):
                    # Level 0 = column names (Close, Open, etc.), Level 1 = ticker
                    data.columns = data.columns.get_level_values(0)

                # Ensure standard column names exist
                col_map = {}
                for col in data.columns:
                    col_lower = str(col).lower().strip()
                    if col_lower == "close":
                        col_map[col] = "Close"
                    elif col_lower == "open":
                        col_map[col] = "Open"
                    elif col_lower == "high":
                        col_map[col] = "High"
                    elif col_lower == "low":
                        col_map[col] = "Low"
                    elif col_lower == "volume":
                        col_map[col] = "Volume"
                if col_map:
                    data = data.rename(columns=col_map)

                if "Close" not in data.columns:
                    logger.warning(f"  {short:<8} ✗ No 'Close' column. Columns: {list(data.columns)}")
                    failed_tickers.append(ticker)
                    continue

                data = data.dropna(subset=["Close"])
                data.index = pd.to_datetime(data.index)

                # Convert to price documents
                prices = []
                for idx, row in data.iterrows():
                    prices.append({
                        "date": idx.strftime("%Y-%m-%d"),
                        "open": round(float(row.get("Open", 0)), 2),
                        "high": round(float(row.get("High", 0)), 2),
                        "low": round(float(row.get("Low", 0)), 2),
                        "close": round(float(row.get("Close", 0)), 2),
                        "volume": int(row.get("Volume", 0)),
                    })

                # Save to MongoDB
                before_count = len(prices)
                await mongodb.save_stock_prices(ticker, prices)
                total_inserted += before_count

                logger.info(f"  {short:<8} ✓ {len(prices)} records processed")

            except Exception as e:
                logger.error(f"  {short:<8} ✗ Error: {e}")
                failed_tickers.append(ticker)
                continue

        # Small delay between batches to be nice to yfinance
        if i + batch_size < len(tickers):
            await asyncio.sleep(2)

    # ── Summary ──
    logger.info("\n" + "=" * 70)
    logger.info("  SEEDING COMPLETE")
    logger.info(f"  Total records processed: {total_inserted:,}")
    logger.info(f"  Tickers up to date:      {total_skipped}")
    logger.info(f"  Failed tickers:          {len(failed_tickers)}")
    if failed_tickers:
        logger.info(f"  Failed list: {', '.join(failed_tickers)}")
    logger.info("=" * 70)

    # ── Final summary from MongoDB ──
    final_summary = await mongodb.get_all_tickers_summary()
    logger.info(f"\n  MongoDB now contains data for {len(final_summary)} tickers:")
    total_docs = 0
    for ticker, info in sorted(final_summary.items()):
        total_docs += info["count"]
        logger.info(
            f"    {ticker:<12} | {info['count']:>5} records | "
            f"{info['first_date']} → {info['last_date']}"
        )
    logger.info(f"\n  Total documents in collection: {total_docs:,}")

    await mongodb.close()


if __name__ == "__main__":
    asyncio.run(seed_database())
