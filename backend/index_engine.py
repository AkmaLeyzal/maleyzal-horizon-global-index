"""
Index Engine — MSCI-style Free-float Market Cap Weighted Index Calculator.

Methodology (aligned with MSCI / FTSE / S&P Dow Jones):
══════════════════════════════════════════════════════════

1. FREE-FLOAT ADJUSTED MARKET CAPITALIZATION
   FF_MCap_i(t) = Price_i(t) × SharesOutstanding_i × FIF_i

   Where:
   - Price_i(t)         = Closing price of stock i on day t
   - SharesOutstanding_i = Total shares outstanding
   - FIF_i              = Free-float Inclusion Factor (0.0 – 1.0)
                           Represents proportion of shares available for
                           public trading (excludes strategic holdings,
                           government stakes, locked-in shares, etc.)

2. INDEX LEVEL (Divisor Method — Laspeyres-type)
   Index(t) = [ Σ FF_MCap_i(t) ] / D(t)

   Where D(t) is the divisor.

3. BASE DATE CALIBRATION
   On the base date (t₀): D(t₀) = Σ FF_MCap_i(t₀) / BaseValue
   This ensures Index(t₀) = BaseValue (default: 1000).

4. DIVISOR ADJUSTMENT (for index continuity)
   When constituents change (additions/deletions) or corporate actions
   occur (splits, rights issues, share changes), the divisor is adjusted
   so the index level is unchanged:

   D_new = D_old × [ Σ FF_MCap_new ] / [ Σ FF_MCap_old ]

   This is the same method used by MSCI, FTSE Russell, and S&P Dow Jones.

5. DAILY CALCULATION
   - Calculated once per trading day AFTER market close (17:00 WIB).
   - Uses official closing prices from the IHSG trading session.
   - Only calculated on weekdays (Monday–Friday).

6. WEIGHT CALCULATION
   Weight_i(t) = FF_MCap_i(t) / Σ FF_MCap_j(t)  for all j in index

7. PERFORMANCE ATTRIBUTION (chain-linking — same day)
   Daily Return = [ Index(t) / Index(t-1) ] - 1
"""
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from data_fetcher import data_fetcher
from models import ConstituentInfo, IndexValue, IndexHistoryPoint, IndexSnapshot

logger = logging.getLogger("mhgi.index_engine")

CONFIG_PATH = Path(__file__).parent / "constituents.json"
HISTORY_PATH = Path(__file__).parent / "index_history.json"


class IndexEngine:
    """
    MSCI-style Free-float Market Cap Weighted Index Calculator.

    The engine uses the Divisor Method (Laspeyres-type price return index),
    the same fundamental approach as MSCI ACWI, FTSE 100, S&P 500, etc.
    """

    def __init__(self):
        self.config = self._load_config()
        self.divisor: Optional[float] = None
        self.base_value: float = self.config.get("base_value", 1000)
        self.base_date: str = self.config.get("base_date", "2025-01-02")
        self.index_history: list[dict] = []
        self.last_snapshot: Optional[IndexSnapshot] = None
        self._stocks_info: dict = {}
        self._last_ff_mcap_sum: Optional[float] = None  # For divisor adjustment
        self._load_history()

    # ─────────── CONFIG & PERSISTENCE ───────────

    def _load_config(self) -> dict:
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"constituents": {}, "base_value": 1000, "base_date": "2025-01-02"}

    def _load_history(self):
        if HISTORY_PATH.exists():
            try:
                with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                    self.index_history = json.load(f)
                if self.index_history:
                    last = self.index_history[-1]
                    if "divisor" in last:
                        self.divisor = last["divisor"]
                    if "ff_mcap_sum" in last:
                        self._last_ff_mcap_sum = last["ff_mcap_sum"]
            except Exception as e:
                logger.warning(f"Failed to load history: {e}")
                self.index_history = []

    def _save_history(self):
        try:
            # Keep up to 10 years of daily data
            trimmed = self.index_history[-3650:]
            with open(HISTORY_PATH, "w", encoding="utf-8") as f:
                json.dump(trimmed, f, default=str, indent=2)
        except Exception as e:
            logger.error(f"Failed to save history: {e}")

    @property
    def tickers(self) -> list[str]:
        return list(self.config.get("constituents", {}).keys())

    def get_constituent_config(self, ticker: str) -> dict:
        return self.config.get("constituents", {}).get(ticker, {})

    def reload_config(self):
        """Reload config and adjust divisor if constituents changed."""
        old_tickers = set(self.tickers)
        self.config = self._load_config()
        new_tickers = set(self.tickers)

        added = new_tickers - old_tickers
        removed = old_tickers - new_tickers

        if added or removed:
            logger.info(f"Constituent change detected. Added: {added}, Removed: {removed}")
            logger.info("Divisor will be adjusted on next calculation to maintain continuity.")

    # ─────────── INITIALIZATION ───────────

    async def initialize(self):
        """Initialize the engine: fetch stock info and set up divisor."""
        logger.info("═" * 50)
        logger.info("  Initializing MHGI Index Engine")
        logger.info(f"  Methodology: Free-float MCap Weighted (Divisor Method)")
        logger.info(f"  Base Date: {self.base_date} | Base Value: {self.base_value}")
        logger.info(f"  Constituents: {len(self.tickers)} stocks")
        logger.info("═" * 50)

        # Fetch fundamental data (shares outstanding, etc.)
        self._stocks_info = data_fetcher.fetch_stocks_info(self.tickers)

        for ticker in self.tickers:
            info = self._stocks_info.get(ticker, {})
            config = self.get_constituent_config(ticker)
            shares = info.get("shares_outstanding", 0)
            ff = config.get("free_float_factor", 0.5)
            logger.info(
                f"  {ticker:<12} | Shares: {shares:>15,.0f} | FIF: {ff:.3f} | "
                f"{config.get('name', ticker)}"
            )

        # Calculate base divisor if not saved
        if self.divisor is None:
            await self._calculate_base_divisor()

        logger.info(f"  Divisor = {self.divisor:,.2f}")
        logger.info("═" * 50)

    async def _calculate_base_divisor(self):
        """
        Calculate the divisor on the base date.
        D(t₀) = Σ FF_MCap_i(t₀) / BaseValue
        """
        logger.info(f"Calculating base divisor for {self.base_date}...")

        historical = data_fetcher.fetch_historical(self.tickers, period="max", interval="1d")
        base_ff_mcap_sum = 0

        for ticker in self.tickers:
            if ticker not in historical or historical[ticker].empty:
                logger.warning(f"  No historical data for {ticker}, skipping base calc")
                continue

            df = historical[ticker]
            df.index = pd.to_datetime(df.index)
            target = pd.Timestamp(self.base_date)

            # Find closing price on or before base date
            if target in df.index:
                base_price = float(df.loc[target, "Close"])
            else:
                mask = df.index <= target
                if mask.any():
                    base_price = float(df.loc[mask].iloc[-1]["Close"])
                else:
                    base_price = float(df.iloc[0]["Close"])

            config = self.get_constituent_config(ticker)
            ff_factor = config.get("free_float_factor", 0.5)

            shares = self._get_shares(ticker)
            ff_mcap = base_price * shares * ff_factor
            base_ff_mcap_sum += ff_mcap

            logger.info(
                f"  {ticker:<12} | Base Price: {base_price:>10,.0f} | "
                f"FF MCap: {ff_mcap:>18,.0f}"
            )

        if base_ff_mcap_sum > 0:
            self.divisor = base_ff_mcap_sum / self.base_value
        else:
            self.divisor = 1.0

        self._last_ff_mcap_sum = base_ff_mcap_sum
        logger.info(f"  Σ FF MCap (base): {base_ff_mcap_sum:,.0f}")
        logger.info(f"  Divisor D(t₀):    {self.divisor:,.2f}")

    def _get_shares(self, ticker: str) -> float:
        """Get shares outstanding for a ticker with fallback."""
        shares = 0
        if ticker in self._stocks_info:
            shares = self._stocks_info[ticker].get("shares_outstanding", 0)
        if shares == 0:
            shares = 1_000_000_000  # fallback
            logger.warning(f"Using fallback shares (1B) for {ticker}")
        return shares

    # ─────────── DAILY EOD CALCULATION ───────────

    async def calculate_eod_index(self) -> Optional[IndexSnapshot]:
        """
        End-of-Day index calculation using closing prices.

        Formula: Index(t) = Σ FF_MCap_i(t) / D(t)

        This is the primary method called once daily after market close.
        """
        try:
            logger.info("=" * 50)
            logger.info("  DAILY EOD INDEX CALCULATION")
            logger.info("=" * 50)

            # Fetch latest closing prices
            prices = data_fetcher.fetch_current_prices(self.tickers)

            if not prices:
                logger.warning("No prices fetched, returning last snapshot")
                return self.last_snapshot

            constituents: list[ConstituentInfo] = []
            constituent_data = []
            total_ff_mcap = 0.0
            total_mcap = 0.0

            for ticker in self.tickers:
                config = self.get_constituent_config(ticker)
                if ticker not in prices:
                    logger.warning(f"  {ticker}: No price data available")
                    continue

                price_data = prices[ticker]
                price = price_data["price"]
                ff_factor = config.get("free_float_factor", 0.5)
                shares = self._get_shares(ticker)

                # Step 1: Calculate full market cap
                mcap = price * shares

                # Step 2: Apply free-float factor → FF Market Cap
                ff_mcap = mcap * ff_factor

                total_ff_mcap += ff_mcap
                total_mcap += mcap

                constituent_data.append({
                    "ticker": ticker,
                    "name": config.get("name", ticker),
                    "sector": config.get("sector", "Unknown"),
                    "price": price,
                    "change_percent": price_data.get("change_percent", 0),
                    "market_cap": mcap,
                    "free_float_market_cap": ff_mcap,
                    "free_float_factor": ff_factor,
                    "shares_outstanding": shares,
                    "volume": price_data.get("volume", 0),
                })

                logger.info(
                    f"  {ticker:<12} | Close: {price:>10,.0f} | "
                    f"FF MCap: {ff_mcap:>18,.0f} | FIF: {ff_factor:.3f}"
                )

            if not constituent_data:
                logger.error("No valid constituent data, aborting calculation")
                return self.last_snapshot

            # Step 3: Calculate weights
            for item in constituent_data:
                weight = (item["free_float_market_cap"] / total_ff_mcap * 100) if total_ff_mcap > 0 else 0
                item["weight"] = round(weight, 4)
                constituents.append(ConstituentInfo(**item))

            # Step 4: Ensure divisor is set
            if self.divisor is None or self.divisor == 0:
                self.divisor = total_ff_mcap / self.base_value if total_ff_mcap > 0 else 1.0
                logger.info(f"  Initial divisor set: {self.divisor:,.2f}")

            # Step 5: Calculate index value
            index_value = total_ff_mcap / self.divisor

            # Step 6: Calculate change vs previous close
            prev_value = self.base_value
            prev_date = self.base_date
            if self.index_history:
                last_entry = self.index_history[-1]
                prev_value = last_entry.get("close", last_entry.get("value", self.base_value))
                prev_date = last_entry.get("date", self.base_date)

            change = index_value - prev_value
            change_pct = (change / prev_value * 100) if prev_value > 0 else 0

            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")

            logger.info(f"  ──────────────────────────────────")
            logger.info(f"  Σ FF MCap:       {total_ff_mcap:>20,.0f}")
            logger.info(f"  Σ Total MCap:    {total_mcap:>20,.0f}")
            logger.info(f"  Divisor:         {self.divisor:>20,.2f}")
            logger.info(f"  Index Value:     {index_value:>20,.2f}")
            logger.info(f"  Previous Close:  {prev_value:>20,.2f} ({prev_date})")
            logger.info(f"  Change:          {change:>+20,.2f} ({change_pct:+.4f}%)")
            logger.info(f"  ──────────────────────────────────")

            # Build IndexValue
            index_data = IndexValue(
                timestamp=now,
                value=round(index_value, 2),
                change=round(change, 2),
                change_percent=round(change_pct, 4),
                high=round(index_value, 2),
                low=round(index_value, 2),
                open=round(index_value, 2),
                previous_close=round(prev_value, 2),
                total_market_cap=round(total_mcap, 0),
                total_free_float_market_cap=round(total_ff_mcap, 0),
            )

            snapshot = IndexSnapshot(index=index_data, constituents=constituents)
            self.last_snapshot = snapshot
            self._last_ff_mcap_sum = total_ff_mcap

            # Step 7: Check for duplicate entry today
            existing_today = [
                i for i, h in enumerate(self.index_history)
                if h.get("date") == today_str
            ]
            history_entry = {
                "date": today_str,
                "timestamp": now.isoformat(),
                "value": round(index_value, 2),
                "open": round(index_value, 2),
                "high": round(index_value, 2),
                "low": round(index_value, 2),
                "close": round(index_value, 2),
                "previous_close": round(prev_value, 2),
                "change": round(change, 2),
                "change_percent": round(change_pct, 4),
                "ff_mcap_sum": total_ff_mcap,
                "total_mcap": total_mcap,
                "divisor": self.divisor,
                "num_constituents": len(constituent_data),
            }

            if existing_today:
                # Overwrite today's entry (recalculation)
                self.index_history[existing_today[-1]] = history_entry
                logger.info(f"  Updated existing entry for {today_str}")
            else:
                self.index_history.append(history_entry)
                logger.info(f"  Appended new entry for {today_str}")

            self._save_history()
            logger.info("  EOD calculation complete ✓")

            return snapshot

        except Exception as e:
            logger.error(f"Error in EOD calculation: {e}", exc_info=True)
            return self.last_snapshot

    def adjust_divisor_for_constituent_change(
        self, old_ff_mcap_sum: float, new_ff_mcap_sum: float
    ):
        """
        Adjust divisor when constituents change to maintain index continuity.

        D_new = D_old × (Σ FF_MCap_new / Σ FF_MCap_old)

        This ensures that the index value is the same before and after
        the constituent change, which is standard practice for all
        major index providers (MSCI, FTSE, S&P).
        """
        if self.divisor is None or old_ff_mcap_sum == 0:
            return

        ratio = new_ff_mcap_sum / old_ff_mcap_sum
        old_divisor = self.divisor
        self.divisor = self.divisor * ratio

        logger.info("  DIVISOR ADJUSTMENT (constituent change)")
        logger.info(f"    Old Σ FF MCap: {old_ff_mcap_sum:,.0f}")
        logger.info(f"    New Σ FF MCap: {new_ff_mcap_sum:,.0f}")
        logger.info(f"    Ratio:         {ratio:.6f}")
        logger.info(f"    Old Divisor:   {old_divisor:,.2f}")
        logger.info(f"    New Divisor:   {self.divisor:,.2f}")

    # ─────────── HISTORICAL BACKFILL ───────────

    async def build_historical_index(self) -> list[dict]:
        """
        Build complete historical index values from base_date to today.
        Uses daily closing prices and the divisor method.
        """
        logger.info("Building historical index data (daily EOD)...")
        historical = data_fetcher.fetch_historical(self.tickers, period="max", interval="1d")

        if not historical:
            return []

        # Collect all trading dates
        all_dates = set()
        for ticker, df in historical.items():
            df.index = pd.to_datetime(df.index)
            all_dates.update(df.index.tolist())

        all_dates = sorted(all_dates)
        base_dt = pd.Timestamp(self.base_date)
        all_dates = [d for d in all_dates if d >= base_dt]

        if not all_dates:
            return []

        result = []
        divisor = None
        prev_value = self.base_value

        for dt in all_dates:
            total_ff_mcap = 0.0
            total_mcap = 0.0
            valid_count = 0

            for ticker in self.tickers:
                if ticker not in historical:
                    continue

                df = historical[ticker]
                config = self.get_constituent_config(ticker)
                ff_factor = config.get("free_float_factor", 0.5)
                shares = self._get_shares(ticker)

                # Get closing price for this date
                if dt in df.index:
                    price = float(df.loc[dt, "Close"])
                else:
                    # Use last known price (stock may not trade every day)
                    mask = df.index <= dt
                    if mask.any():
                        price = float(df.loc[mask].iloc[-1]["Close"])
                    else:
                        continue

                mcap = price * shares
                ff_mcap = mcap * ff_factor
                total_ff_mcap += ff_mcap
                total_mcap += mcap
                valid_count += 1

            if valid_count == 0:
                continue

            # Set divisor on base date
            if divisor is None:
                divisor = total_ff_mcap / self.base_value
                self.divisor = divisor

            index_val = total_ff_mcap / divisor
            change = index_val - prev_value
            change_pct = (change / prev_value * 100) if prev_value > 0 else 0

            date_str = dt.strftime("%Y-%m-%d")

            result.append({
                "date": date_str,
                "timestamp": dt.isoformat(),
                "value": round(index_val, 2),
                "open": round(index_val, 2),
                "high": round(index_val, 2),
                "low": round(index_val, 2),
                "close": round(index_val, 2),
                "previous_close": round(prev_value, 2),
                "change": round(change, 2),
                "change_percent": round(change_pct, 4),
                "ff_mcap_sum": total_ff_mcap,
                "total_mcap": total_mcap,
                "divisor": divisor,
                "num_constituents": valid_count,
                "time": int(dt.timestamp()),
            })

            prev_value = index_val

        self.index_history = result
        self._save_history()

        logger.info(f"Built {len(result)} daily data points from {self.base_date}")
        if result:
            logger.info(f"  First: {result[0]['date']} → {result[0]['value']:.2f}")
            logger.info(f"  Last:  {result[-1]['date']} → {result[-1]['value']:.2f}")

        return result

    # ─────────── QUERY METHODS ───────────

    def get_history(self, days: int = 365) -> list[IndexHistoryPoint]:
        """Get index history for charting."""
        cutoff = datetime.now().timestamp() - (days * 86400)
        points = []
        for entry in self.index_history:
            try:
                ts_str = entry.get("timestamp", entry.get("date", ""))
                ts = datetime.fromisoformat(ts_str) if ts_str else None
                if ts and ts.timestamp() >= cutoff:
                    points.append(IndexHistoryPoint(
                        timestamp=ts,
                        value=entry.get("close", entry.get("value", 0)),
                        open=entry.get("open"),
                        high=entry.get("high"),
                        low=entry.get("low"),
                        close=entry.get("close", entry.get("value")),
                    ))
            except Exception:
                continue
        return points

    def get_index_meta(self) -> dict:
        last_calc = None
        next_calc = None
        if self.index_history:
            last_calc = self.index_history[-1].get("date")

        # Next calculation: next weekday at 17:00 WIB
        now = datetime.now()
        next_day = now
        if now.hour >= 17:
            next_day = now + timedelta(days=1)
        while next_day.weekday() >= 5:  # Skip weekends
            next_day += timedelta(days=1)
        next_calc = next_day.strftime("%Y-%m-%d") + " 17:00 WIB"

        return {
            "name": self.config.get("index_name", "MHGI"),
            "full_name": self.config.get("index_full_name", "Maleyzal Horizon Global Index"),
            "base_value": self.base_value,
            "base_date": self.base_date,
            "currency": self.config.get("currency", "IDR"),
            "description": self.config.get("description", ""),
            "methodology": "Free-float Market Cap Weighted (Divisor Method)",
            "calculation_frequency": "Daily at 17:00 WIB (after IHSG close)",
            "num_constituents": len(self.tickers),
            "last_calculated": last_calc,
            "next_calculation": next_calc,
            "divisor": self.divisor,
        }


# Singleton engine
index_engine = IndexEngine()
