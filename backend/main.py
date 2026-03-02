"""
MHGI — Maleyzal Horizon Global Index API
FastAPI application with REST endpoints and WebSocket.
Index calculated daily at 17:00 WIB (after IHSG close).
Persistence: MongoDB Atlas (with JSON fallback).
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from database import mongodb
from data_fetcher import data_fetcher
from index_engine import index_engine
from websocket_manager import ws_manager
from scheduler import index_scheduler
from models import IndexMeta

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-24s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mhgi.main")

# Track initialization state
_init_complete = False


async def _background_init():
    """Heavy initialization that runs AFTER server is already accepting connections."""
    global _init_complete
    try:
        logger.info("  [Background] Starting heavy initialization...")

        # Step 3: Initialize index engine (fetch stock info, calculate divisor)
        try:
            await index_engine.initialize()
            logger.info("  [Background] ✅ Step 3: Engine initialized")
        except Exception as e:
            logger.error(f"  [Background] ❌ Step 3 failed: {e}")
            _init_complete = True
            return

        # Step 4: Build historical index data (incremental if MongoDB)
        try:
            await index_engine.build_historical_index()
            logger.info("  [Background] ✅ Step 4: Historical index built")
        except Exception as e:
            logger.error(f"  [Background] ❌ Step 4 failed: {e}")

        # Step 5: Calculate latest EOD value
        try:
            await index_engine.calculate_eod_index()
            logger.info("  [Background] ✅ Step 5: EOD calculated")
        except Exception as e:
            logger.error(f"  [Background] ❌ Step 5 failed: {e}")

        # Step 6: Start daily scheduler
        try:
            await index_scheduler.start(index_engine, ws_manager)
            logger.info("  [Background] ✅ Step 6: Scheduler started")
        except Exception as e:
            logger.error(f"  [Background] ❌ Step 6 failed: {e}")

        _init_complete = True
        has_snapshot = index_engine.last_snapshot is not None
        history_count = len(index_engine.index_history)
        logger.info(f"  [Background] Init done — snapshot: {has_snapshot}, history: {history_count}")
    except Exception as e:
        logger.error(f"  [Background] ❌ Initialization failed: {e}")
        _init_complete = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic.
    Only fast operations happen here — heavy init is deferred to background.
    """
    logger.info("=" * 60)
    logger.info("  MHGI — Maleyzal Horizon Global Index")
    logger.info("  Starting up...")
    logger.info("=" * 60)

    # Step 1: Connect to MongoDB Atlas (fast, ~2-5 seconds)
    db_connected = await mongodb.connect()

    # Step 2: Inject database into engine and data_fetcher
    if db_connected:
        index_engine.set_db(mongodb)
        data_fetcher.set_db(mongodb)
        logger.info("  MongoDB Atlas connected ✅")
    else:
        logger.warning("  Running without MongoDB — using JSON fallback ⚠️")

    # Server will bind to port NOW — heavy init happens in background
    logger.info("  Server ready — starting background initialization...")
    asyncio.create_task(_background_init())

    yield

    # Shutdown
    await index_scheduler.stop()
    await mongodb.close()
    logger.info("MHGI shut down.")


app = FastAPI(
    title="MHGI API",
    description="Maleyzal Horizon Global Index — Free-float Market Cap Weighted Index for IHSG",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ───────── REST API ENDPOINTS ─────────


@app.get("/api/health")
async def get_health():
    """Health check — returns quickly even during initialization."""
    return {
        "status": "healthy",
        "initialized": _init_complete,
        "timestamp": datetime.now().isoformat(),
        "database": "MongoDB Atlas ✅" if mongodb.is_connected else "JSON fallback ⚠️",
        "index_ready": index_engine.last_snapshot is not None,
        "history_count": len(index_engine.index_history),
        "divisor": index_engine.divisor,
        "stocks_info_count": len(getattr(index_engine, '_stocks_info', {})),
    }


@app.get("/api/debug")
async def get_debug():
    """Debug endpoint — shows engine internals for troubleshooting."""
    info = getattr(index_engine, '_stocks_info', {})
    missing_shares = [
        t for t in index_engine.tickers
        if info.get(t, {}).get("shares_outstanding", 0) == 0
    ]
    return {
        "tickers_total": len(index_engine.tickers),
        "stocks_info_loaded": len(info),
        "missing_shares": missing_shares,
        "missing_shares_count": len(missing_shares),
        "divisor": index_engine.divisor,
        "history_count": len(index_engine.index_history),
        "has_snapshot": index_engine.last_snapshot is not None,
        "last_history_date": index_engine.index_history[-1]["date"] if index_engine.index_history else None,
    }


@app.get("/api/index")
async def get_index():
    """Get current index value and stats."""
    snapshot = index_engine.last_snapshot
    if not snapshot:
        return JSONResponse(
            status_code=503,
            content={"error": "Index not yet calculated. Please wait..."},
        )
    return {
        "index": snapshot.index.model_dump(),
        "updated_at": datetime.now().isoformat(),
    }


@app.get("/api/constituents")
async def get_constituents():
    """Get all constituent stocks with weights and prices."""
    snapshot = index_engine.last_snapshot
    if not snapshot:
        return JSONResponse(
            status_code=503,
            content={"error": "Index not yet calculated."},
        )
    return {
        "constituents": [c.model_dump() for c in snapshot.constituents],
        "total": len(snapshot.constituents),
    }


@app.get("/api/history")
async def get_history(
    days: int = Query(default=365, ge=1, le=3650, description="Number of days of history"),
):
    """Get historical index values for charting."""
    history = index_engine.get_history(days=days)
    return {
        "history": [h.model_dump() for h in history],
        "count": len(history),
    }


@app.get("/api/history/full")
async def get_full_history():
    """Get full historical index data from base date."""
    data = index_engine.index_history
    return {
        "history": data,
        "count": len(data),
        "base_date": index_engine.base_date,
        "base_value": index_engine.base_value,
    }


@app.get("/api/meta")
async def get_meta():
    """Get index metadata."""
    meta = index_engine.get_index_meta()
    return meta


@app.get("/api/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "clients_connected": ws_manager.client_count,
        "index_ready": index_engine.last_snapshot is not None,
        "database": "MongoDB Atlas ✅" if mongodb.is_connected else "JSON fallback ⚠️",
        "calculation_frequency": "Daily at 17:00 WIB (Mon-Fri)",
        "last_calculated": index_scheduler.last_calc_date or "N/A",
        "next_calculation": index_scheduler.get_next_calculation(),
    }


# ───────── WEBSOCKET ENDPOINT ─────────


@app.websocket("/ws/index")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time index updates."""
    await ws_manager.connect(websocket)

    # Send current snapshot on connect
    snapshot = index_engine.last_snapshot
    if snapshot:
        import json
        initial_data = {
            "type": "initial",
            "index": snapshot.index.model_dump(),
            "constituents": [c.model_dump() for c in snapshot.constituents],
            "history": index_engine.index_history[-100:],
        }
        await websocket.send_text(json.dumps(initial_data, default=str))

    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text('{"type": "pong"}')
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
