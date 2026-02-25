"""
MHGI — Maleyzal Horizon Global Index API
FastAPI application with REST endpoints and WebSocket.
Index calculated daily at 17:00 WIB (after IHSG close).
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    logger.info("=" * 60)
    logger.info("  MHGI — Maleyzal Horizon Global Index")
    logger.info("  Starting up...")
    logger.info("=" * 60)

    # Initialize index engine (fetch stock info, calculate divisor)
    await index_engine.initialize()

    # Build historical index data from base_date
    await index_engine.build_historical_index()

    # Calculate latest EOD value
    await index_engine.calculate_eod_index()

    # Start daily scheduler (triggers at 17:00 WIB on weekdays)
    await index_scheduler.start(index_engine, ws_manager)

    logger.info("MHGI is live!")
    yield

    # Shutdown
    await index_scheduler.stop()
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
            "history": index_engine.index_history[-100:],  # Last 100 points
        }
        await websocket.send_text(json.dumps(initial_data, default=str))

    try:
        while True:
            # Keep connection alive, handle incoming messages
            data = await websocket.receive_text()
            # Client can send "ping" to keep alive
            if data == "ping":
                await websocket.send_text('{"type": "pong"}')
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
