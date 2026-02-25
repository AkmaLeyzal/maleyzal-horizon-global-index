"""
Daily End-of-Day Scheduler.

Calculates the MHGI index once per trading day at 17:00 WIB (after IHSG close).
IHSG trading hours: 09:00–15:00 WIB (Mon–Fri), so 17:00 allows for final
settlement prices to be published.

The scheduler:
1. Checks every 60 seconds if it's time to calculate
2. Only runs on weekdays (Monday–Friday)
3. Only runs once per day (tracks last calculation date)
4. Broadcasts the result via WebSocket to connected clients
"""
import asyncio
import logging
from datetime import datetime, time, timedelta

logger = logging.getLogger("mhgi.scheduler")

# Schedule: 17:00 WIB (UTC+7) on weekdays
CALCULATION_HOUR = 17
CALCULATION_MINUTE = 0
CHECK_INTERVAL = 60  # seconds


class DailyEODScheduler:
    """
    Runs index calculation once per trading day at 17:00 WIB.
    """

    def __init__(self):
        self._task: asyncio.Task = None
        self._running = False
        self._last_calc_date: str = ""

    async def start(self, engine, ws_manager):
        """Start the daily scheduler."""
        self._running = True

        # Recover last calc date from history
        if engine.index_history:
            self._last_calc_date = engine.index_history[-1].get("date", "")

        self._task = asyncio.create_task(self._run_loop(engine, ws_manager))
        logger.info(
            f"Daily EOD Scheduler started"
            f" (trigger at {CALCULATION_HOUR:02d}:{CALCULATION_MINUTE:02d} WIB, Mon–Fri)"
        )
        if self._last_calc_date:
            logger.info(f"  Last calculation: {self._last_calc_date}")

    async def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Daily EOD Scheduler stopped")

    def _is_weekday(self, dt: datetime) -> bool:
        return dt.weekday() < 5  # Mon=0, Fri=4

    def _is_calculation_time(self, now: datetime) -> bool:
        """Check if current time matches the 17:00 schedule window."""
        if not self._is_weekday(now):
            return False

        target = time(CALCULATION_HOUR, CALCULATION_MINUTE)
        # Allow a 2-minute window (17:00 – 17:01)
        current_time = now.time()
        if current_time.hour == target.hour and current_time.minute <= target.minute + 1:
            today_str = now.strftime("%Y-%m-%d")
            return today_str != self._last_calc_date
        return False

    async def _run_loop(self, engine, ws_manager):
        """Main loop — checks every minute if it's time to calculate."""
        while self._running:
            try:
                now = datetime.now()

                if self._is_calculation_time(now):
                    today_str = now.strftime("%Y-%m-%d")
                    logger.info(f"⏰ Trigger: Daily EOD calculation for {today_str}")

                    snapshot = await engine.calculate_eod_index()

                    if snapshot:
                        self._last_calc_date = today_str

                        # Broadcast to WebSocket clients
                        payload = self._build_broadcast_payload(snapshot, today_str)
                        await ws_manager.broadcast(payload)

                        logger.info(
                            f"✅ EOD complete: {snapshot.index.value:.2f} "
                            f"({snapshot.index.change_percent:+.4f}%) "
                            f"| Clients notified: {ws_manager.client_count}"
                        )
                    else:
                        logger.error("❌ EOD calculation returned None")

            except Exception as e:
                logger.error(f"Scheduler error: {e}", exc_info=True)

            await asyncio.sleep(CHECK_INTERVAL)

    def _build_broadcast_payload(self, snapshot, date_str: str) -> dict:
        """Build WebSocket broadcast payload."""
        return {
            "type": "eod_update",
            "date": date_str,
            "timestamp": datetime.now().isoformat(),
            "index": {
                "value": snapshot.index.value,
                "change": snapshot.index.change,
                "change_percent": snapshot.index.change_percent,
                "high": snapshot.index.high,
                "low": snapshot.index.low,
                "open": snapshot.index.open,
                "previous_close": snapshot.index.previous_close,
                "total_market_cap": snapshot.index.total_market_cap,
                "total_free_float_market_cap": snapshot.index.total_free_float_market_cap,
            },
            "constituents": [
                {
                    "ticker": c.ticker,
                    "name": c.name,
                    "sector": c.sector,
                    "price": c.price,
                    "change_percent": c.change_percent,
                    "weight": c.weight,
                    "market_cap": c.market_cap,
                    "free_float_market_cap": c.free_float_market_cap,
                    "volume": c.volume,
                }
                for c in snapshot.constituents
            ],
        }

    def get_next_calculation(self) -> str:
        """Get the next scheduled calculation datetime."""
        now = datetime.now()
        target = now.replace(hour=CALCULATION_HOUR, minute=CALCULATION_MINUTE, second=0)

        if now >= target or now.strftime("%Y-%m-%d") == self._last_calc_date:
            target += timedelta(days=1)

        while target.weekday() >= 5:
            target += timedelta(days=1)

        return target.strftime("%Y-%m-%d %H:%M WIB")

    @property
    def last_calc_date(self) -> str:
        return self._last_calc_date


# Singleton
index_scheduler = DailyEODScheduler()
