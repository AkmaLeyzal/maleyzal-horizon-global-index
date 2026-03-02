"""Seed stock info to MongoDB — run locally to populate cache for Render."""
import asyncio
import json
import logging
from pathlib import Path

import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("seed_info")


async def seed_stock_info():
    from database import mongodb

    config_path = Path(__file__).parent / "constituents.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    tickers = list(config.get("constituents", {}).keys())
    logger.info(f"Seeding stock info for {len(tickers)} tickers...")

    connected = await mongodb.connect()
    if not connected:
        logger.error("Failed to connect to MongoDB")
        return

    success = 0
    failed = []

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            doc = {
                "shares_outstanding": info.get("sharesOutstanding", 0),
                "market_cap": info.get("marketCap", 0),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "currency": info.get("currency", "IDR"),
                "name": info.get("longName") or info.get("shortName") or ticker,
                "exchange": info.get("exchange", ""),
            }

            if doc["shares_outstanding"] and doc["shares_outstanding"] > 0:
                await mongodb.save_stock_info(ticker, doc)
                short = ticker.replace(".JK", "")
                logger.info(f"  {short:8s} ✓ shares={doc['shares_outstanding']:>15,}")
                success += 1
            else:
                logger.warning(f"  {ticker}: No shares_outstanding data")
                failed.append(ticker)

        except Exception as e:
            logger.error(f"  {ticker}: Error — {e}")
            failed.append(ticker)

    logger.info(f"\nDone: {success} success, {len(failed)} failed")
    if failed:
        logger.info(f"Failed: {', '.join(failed)}")

    await mongodb.close()


if __name__ == "__main__":
    asyncio.run(seed_stock_info())
