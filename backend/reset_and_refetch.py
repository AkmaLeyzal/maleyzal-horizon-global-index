"""
Reset & Refetch — Clears ALL MongoDB data and re-seeds from 2019-01-01.

Usage:
    cd backend
    python reset_and_refetch.py

Drops all collections, recreates the Standard_Index as a timeseries collection,
then fetches stock prices from 2019-01-01 for all MHGI constituents.
Stocks that IPO'd after 2019 will have data starting from their available date.

Features:
    - Retry logic (up to 3 attempts per ticker with exponential backoff)
    - Small batch sizes (3 tickers) to avoid rate limiting
    - Detailed progress tracking and error reporting
"""
import asyncio
import json
import logging
import sys
import time
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
logger = logging.getLogger("mhgi.reset")

FETCH_START_DATE = "2019-01-01"
BATCH_SIZE = 3          # Smaller batches to avoid rate limiting
BATCH_DELAY = 5         # Seconds between batches
MAX_RETRIES = 3         # Max retry attempts per ticker
RETRY_BASE_DELAY = 10   # Base delay for exponential backoff (seconds)


def fetch_ticker_data(ticker: str) -> pd.DataFrame:
    """Fetch historical data for a single ticker from yfinance."""
    data = yf.download(
        ticker,
        start=FETCH_START_DATE,
        interval="1d",
        progress=False,
        threads=True,
    )

    if data.empty:
        return pd.DataFrame()

    # Flatten MultiIndex columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    # Ensure standard column names
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
        return pd.DataFrame()

    data = data.dropna(subset=["Close"])
    data.index = pd.to_datetime(data.index)
    return data


def dataframe_to_price_docs(data: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of price documents for MongoDB."""
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
    return prices


async def process_ticker(mongodb, ticker: str, ticker_num: int, total: int) -> dict:
    """
    Process a single ticker with retry logic.
    Returns a result dict with status info.
    """
    short = ticker.replace(".JK", "")
    progress = f"[{ticker_num}/{total}]"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 2))
                logger.info(f"  {progress} {short:<8} Retry {attempt}/{MAX_RETRIES} (waiting {delay}s)...")
                await asyncio.sleep(delay)

            logger.info(f"  {progress} {short:<8} Fetching from {FETCH_START_DATE} (attempt {attempt})...")

            data = fetch_ticker_data(ticker)

            if data.empty:
                if attempt < MAX_RETRIES:
                    logger.warning(f"  {progress} {short:<8} No data returned, will retry...")
                    continue
                else:
                    logger.warning(f"  {progress} {short:<8} [FAIL] No data after {MAX_RETRIES} attempts")
                    return {
                        "ticker": ticker,
                        "status": "no_data",
                        "records": 0,
                        "first_date": "N/A",
                        "last_date": "N/A",
                    }

            # Convert to price documents
            prices = dataframe_to_price_docs(data)

            # Save to MongoDB
            await mongodb.save_stock_prices(ticker, prices)

            first_date = prices[0]["date"] if prices else "N/A"
            last_date = prices[-1]["date"] if prices else "N/A"

            logger.info(
                f"  {progress} {short:<8} [OK] {len(prices)} records | {first_date} -> {last_date}"
            )

            return {
                "ticker": ticker,
                "status": "success",
                "records": len(prices),
                "first_date": first_date,
                "last_date": last_date,
            }

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"  {progress} {short:<8} Error: {e} — will retry...")
            else:
                logger.error(f"  {progress} {short:<8} [ERROR] Failed after {MAX_RETRIES} attempts: {e}")
                return {
                    "ticker": ticker,
                    "status": "error",
                    "records": 0,
                    "first_date": "N/A",
                    "last_date": "N/A",
                    "error": str(e),
                }

    # Should not reach here, but just in case
    return {"ticker": ticker, "status": "error", "records": 0, "first_date": "N/A", "last_date": "N/A"}


async def reset_and_refetch():
    """Main function: drop all collections and refetch from 2019-01-01."""
    from database import mongodb, DB_NAME, COLLECTION_NAME, TIME_FIELD

    # -- Load config --
    config_path = Path(__file__).parent / "constituents.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    tickers = list(config.get("constituents", {}).keys())

    logger.info("=" * 70)
    logger.info("  MHGI Database Reset & Refetch")
    logger.info(f"  Tickers: {len(tickers)}")
    logger.info(f"  Fetch start: {FETCH_START_DATE}")
    logger.info(f"  Database: {DB_NAME}")
    logger.info(f"  Timeseries collection: {COLLECTION_NAME} (timeField: {TIME_FIELD})")
    logger.info(f"  Batch size: {BATCH_SIZE} | Batch delay: {BATCH_DELAY}s")
    logger.info(f"  Max retries per ticker: {MAX_RETRIES}")
    logger.info("=" * 70)

    # -- Connect to MongoDB --
    connected = await mongodb.connect()
    if not connected:
        logger.error("Failed to connect to MongoDB Atlas. Check .env credentials.")
        sys.exit(1)

    db = mongodb.db

    # -- Step 1: Drop ALL collections --
    logger.info("\n  [STEP 1] Dropping ALL collections...")
    existing_cols = await db.list_collection_names()
    logger.info(f"    Existing collections: {existing_cols}")

    for col_name in existing_cols:
        if not col_name.startswith("system."):
            try:
                await db.drop_collection(col_name)
                logger.info(f"    Dropped: {col_name}")
            except Exception as e:
                logger.error(f"    Error dropping {col_name}: {e}")

    remaining = await db.list_collection_names()
    logger.info(f"    Remaining after drop: {remaining}")

    # -- Step 2: Recreate Standard_Index as TIMESERIES collection --
    logger.info(f"\n  [STEP 2] Creating timeseries collection: {COLLECTION_NAME}")
    try:
        await db.create_collection(
            COLLECTION_NAME,
            timeseries={
                "timeField": TIME_FIELD,
                "metaField": "Ticker",
                "granularity": "hours",
            },
        )
        logger.info("    Timeseries collection created successfully")
    except Exception as e:
        logger.error(f"    Error creating timeseries collection: {e}")
        logger.info("    Continuing anyway...")

    # -- Step 3: Recreate indexes for regular collections --
    logger.info("\n  [STEP 3] Recreating indexes...")
    await mongodb._create_indexes()

    # -- Step 4: Fetch from yfinance and insert --
    logger.info(
        f"\n  [STEP 4] Fetching data from {FETCH_START_DATE} for {len(tickers)} tickers (interval=1d)..."
    )

    results = []
    total_inserted = 0
    success_count = 0
    fail_count = 0
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        logger.info(
            f"\n  -- Batch {batch_num}/{total_batches}: "
            f"{', '.join([t.replace('.JK', '') for t in batch])} --"
        )

        for j, ticker in enumerate(batch):
            ticker_num = i + j + 1
            result = await process_ticker(mongodb, ticker, ticker_num, len(tickers))
            results.append(result)

            if result["status"] == "success":
                total_inserted += result["records"]
                success_count += 1
            else:
                fail_count += 1

            # Small delay between tickers within a batch
            if j < len(batch) - 1:
                await asyncio.sleep(2)

        # Delay between batches
        if i + BATCH_SIZE < len(tickers):
            logger.info(f"    (waiting {BATCH_DELAY}s before next batch...)")
            await asyncio.sleep(BATCH_DELAY)

    # -- Step 5: Clear local index_history.json --
    logger.info("\n  [STEP 5] Clearing local index_history.json...")
    history_path = Path(__file__).parent / "index_history.json"
    try:
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump([], f)
        logger.info("    index_history.json reset to []")
    except Exception as e:
        logger.error(f"    Error clearing index_history.json: {e}")

    # -- Step 6: Final verification --
    logger.info("\n" + "=" * 70)
    logger.info("  RESET & REFETCH COMPLETE")
    logger.info(f"  Total records inserted: {total_inserted:,}")
    logger.info(f"  Successful tickers: {success_count}")
    logger.info(f"  Failed tickers: {fail_count}")
    logger.info("=" * 70)

    # Build failed tickers list
    failed_tickers = [r["ticker"] for r in results if r["status"] != "success"]
    if failed_tickers:
        logger.info(f"  Failed list: {', '.join(failed_tickers)}")

    # Write detailed results to file
    final_summary = await mongodb.get_all_tickers_summary()
    output_lines = [
        "MHGI Reset & Refetch Results",
        "=" * 50,
        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Fetch start: {FETCH_START_DATE}",
        f"Total tickers in DB: {len(final_summary)}",
        f"Total records inserted: {total_inserted:,}",
        f"Successful: {success_count}",
        f"Failed: {fail_count}",
        "",
        "Per-ticker detail:",
    ]

    total_docs = 0
    for ticker, info in sorted(final_summary.items()):
        total_docs += info["count"]
        first = info["first_date"]
        flag = ""
        if first > "2019-01-10":
            flag = "  <-- IPO after 2019"
        if first < "2019-01-01":
            flag = "  <-- WARNING: before 2019!"
        output_lines.append(
            f"  {ticker:<12} | {info['count']:>5} records | {first} -> {info['last_date']}{flag}"
        )

    output_lines.append(f"\nTotal documents: {total_docs:,}")
    if failed_tickers:
        output_lines.append(f"Failed: {', '.join(failed_tickers)}")

    results_path = Path(__file__).parent / "refetch_results.txt"
    with open(results_path, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    logger.info(f"  Results saved to refetch_results.txt")

    # -- Step 7: Retry failed tickers one more time --
    if failed_tickers:
        logger.info(f"\n  [STEP 7] Final retry for {len(failed_tickers)} failed tickers...")
        await asyncio.sleep(15)  # Wait 15 seconds before retrying

        retry_success = 0
        for ticker in failed_tickers:
            short = ticker.replace(".JK", "")
            try:
                logger.info(f"    {short:<8} Final retry...")
                data = fetch_ticker_data(ticker)
                if not data.empty:
                    prices = dataframe_to_price_docs(data)
                    await mongodb.save_stock_prices(ticker, prices)
                    logger.info(f"    {short:<8} [OK] {len(prices)} records recovered!")
                    retry_success += 1
                else:
                    logger.warning(f"    {short:<8} [FAIL] Still no data")
            except Exception as e:
                logger.error(f"    {short:<8} [ERROR] {e}")
            await asyncio.sleep(5)

        if retry_success > 0:
            logger.info(f"    Recovered {retry_success} tickers in final retry!")

    await mongodb.close()
    logger.info("  Done. Restart the backend to rebuild the index.")


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(reset_and_refetch())
    elapsed = time.time() - start_time
    print(f"\nTotal time: {elapsed:.1f} seconds ({elapsed / 60:.1f} minutes)")
