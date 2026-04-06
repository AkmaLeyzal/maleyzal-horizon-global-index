"""Quick script to drop ALL collections in MongoDB."""
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("mhgi.drop")

async def drop_all():
    from database import mongodb
    connected = await mongodb.connect()
    if not connected:
        logger.error("Failed to connect")
        return
    
    db = mongodb.db
    cols = await db.list_collection_names()
    logger.info(f"Existing collections: {cols}")
    
    for col in cols:
        if not col.startswith("system."):
            await db.drop_collection(col)
            logger.info(f"  Dropped: {col}")
    
    remaining = await db.list_collection_names()
    logger.info(f"Remaining: {remaining}")
    logger.info("All collections dropped!")
    await mongodb.close()

asyncio.run(drop_all())
