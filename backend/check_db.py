"""Quick check of MongoDB data — detailed."""
import asyncio
from database import mongodb


async def check():
    await mongodb.connect()
    summary = await mongodb.get_all_tickers_summary()
    print(f"Tickers in DB: {len(summary)}")
    print()
    total = 0
    for ticker, info in sorted(summary.items()):
        total += info["count"]
        short = ticker.replace(".JK", "")
        print(f"  {short:8s} | {info['count']:5d} records | {info['first_date']} -> {info['last_date']}")
    print()
    print(f"Total records: {total:,}")

    # Check which tickers from config are missing
    import json
    from pathlib import Path
    config_path = Path(__file__).parent / "constituents.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    configured = set(config.get("constituents", {}).keys())
    in_db = set(summary.keys())
    missing = configured - in_db
    if missing:
        print(f"\nMISSING from DB: {missing}")
    else:
        print(f"\nAll {len(configured)} configured tickers present in DB ✅")

    await mongodb.close()


if __name__ == "__main__":
    asyncio.run(check())
