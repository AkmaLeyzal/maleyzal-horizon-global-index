"""Quick MongoDB connection test."""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

async def test():
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB", "Maleyzal_Horizon_Global_Index")
    col_name = os.getenv("MONGODB_COLLECTION", "Standard_Index")
    
    print(f"Connecting to MongoDB Atlas...")
    print(f"DB: {db_name}, Collection: {col_name}")
    
    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=10000)
    
    try:
        # Test connection
        await client.admin.command("ping")
        print("[OK] MongoDB Atlas connected successfully!")
        
        db = client[db_name]
        cols = await db.list_collection_names()
        print(f"Collections: {cols}")
        
        col = db[col_name]
        count = await col.count_documents({})
        print(f"Documents in {col_name}: {count}")
        
        if count > 0:
            sample = await col.find_one()
            print(f"Sample doc keys: {list(sample.keys())}")
            # Show last 3 entries sorted by date
            cursor = col.find().sort("Date", -1).limit(3)
            docs = await cursor.to_list(length=3)
            print(f"Last 3 entries:")
            for d in docs:
                date_val = d.get("Date", "N/A")
                value_val = d.get("Value", d.get("value", d.get("Close", "N/A")))
                print(f"  {date_val} - Value: {value_val}")
        else:
            print("[WARN] Collection is empty.")
            
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
    finally:
        client.close()

asyncio.run(test())
