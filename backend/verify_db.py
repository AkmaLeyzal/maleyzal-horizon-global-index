"""Verify MongoDB state - outputs to verify_output.txt."""
import asyncio
from database import mongodb

async def verify():
    lines = []
    connected = await mongodb.connect()
    if not connected:
        lines.append("FAILED to connect to MongoDB")
        with open("verify_output.txt", "w") as f:
            f.write("\n".join(lines))
        return

    db = mongodb.db
    cols = await db.list_collection_names()
    lines.append(f"Collections: {cols}")

    summary = await mongodb.get_all_tickers_summary()
    total_docs = 0
    post_2019_ipo = 0
    pre_2019 = 0

    lines.append(f"\nTicker summary ({len(summary)} tickers):")
    for ticker, info in sorted(summary.items()):
        total_docs += info["count"]
        first = info["first_date"]
        flag = ""
        if first > "2019-01-10":
            flag = "  <-- IPO after 2019"
            post_2019_ipo += 1
        if first < "2019-01-01":
            flag = "  <-- WARNING: data before 2019!"
            pre_2019 += 1
        lines.append(f"  {ticker:<12} | {info['count']:>5} records | {first} -> {info['last_date']}{flag}")

    lines.append(f"\nTotal tickers: {len(summary)}")
    lines.append(f"Total documents: {total_docs:,}")
    lines.append(f"Tickers with IPO after 2019: {post_2019_ipo}")
    if pre_2019 > 0:
        lines.append(f"WARNING: {pre_2019} tickers have data before 2019!")
    else:
        lines.append("OK: All data starts from 2019 or later")

    for col_name in ["stock_info", "engine_state", "index_history"]:
        if col_name in cols:
            count = await db[col_name].count_documents({})
            lines.append(f"  {col_name}: {count} docs")
        else:
            lines.append(f"  {col_name}: (not found)")

    await mongodb.close()
    
    with open("verify_output.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("Output written to verify_output.txt")

asyncio.run(verify())
