"""
Data fetcher module — retrieves stock data from Yahoo Finance via yfinance.
Supports MongoDB Atlas caching: only fetches data not already stored.
"""
import yfinance as yf
import pandas as pd
import numpy as np
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger("mhgi.data_fetcher")


class DataFetcher:
    """Fetches stock data from Yahoo Finance, with MongoDB-backed caching."""

    def __init__(self):
        self._db = None  # Set via set_db()

    def set_db(self, db_manager):
        """Inject MongoDB manager for persistent caching."""
        self._db = db_manager

    @property
    def has_db(self) -> bool:
        return self._db is not None and self._db.is_connected

    # ─────────── CURRENT PRICES ───────────

    def fetch_current_prices(self, tickers: list[str]) -> dict:
        """
        Fetch current prices for multiple tickers.
        Returns dict of {ticker: {price, change, change_percent, volume, ...}}
        """
        result = {}
        try:
            ticker_str = " ".join(tickers)
            data = yf.download(
                ticker_str,
                period="2d",
                interval="1d",
                group_by="ticker",
                progress=False,
                threads=True,
            )

            for ticker in tickers:
                try:
                    if len(tickers) == 1:
                        ticker_data = data
                    else:
                        ticker_data = data[ticker]

                    if ticker_data.empty:
                        continue

                    if isinstance(ticker_data.columns, pd.MultiIndex):
                        ticker_data.columns = ticker_data.columns.get_level_values(-1)

                    ticker_data = ticker_data.dropna(subset=["Close"])

                    if len(ticker_data) >= 2:
                        current = ticker_data.iloc[-1]
                        previous = ticker_data.iloc[-2]
                        price = float(current["Close"])
                        prev_close = float(previous["Close"])
                        change = price - prev_close
                        change_pct = (change / prev_close) * 100 if prev_close else 0
                    elif len(ticker_data) == 1:
                        current = ticker_data.iloc[-1]
                        price = float(current["Close"])
                        prev_close = float(current.get("Open", price))
                        change = price - prev_close
                        change_pct = (change / prev_close) * 100 if prev_close else 0
                    else:
                        continue

                    result[ticker] = {
                        "price": price,
                        "open": float(current.get("Open", price)),
                        "high": float(current.get("High", price)),
                        "low": float(current.get("Low", price)),
                        "close": price,
                        "previous_close": prev_close,
                        "change": round(change, 2),
                        "change_percent": round(change_pct, 4),
                        "volume": int(current.get("Volume", 0)),
                    }
                except Exception as e:
                    logger.warning(f"Error fetching data for {ticker}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error in batch download: {e}")

        return result

    # ─────────── STOCK INFO (with MongoDB cache) ───────────

    async def fetch_stock_info_async(self, ticker: str) -> Optional[dict]:
        """
        Fetch stock info with MongoDB caching.
        Returns from DB if fresh (<24h), otherwise fetches from yfinance.
        """
        # Try MongoDB first
        if self.has_db:
            cached = await self._db.load_stock_info(ticker)
            if cached:
                logger.info(f"  {ticker}: Stock info loaded from MongoDB (cached)")
                return cached

        # Fetch from yfinance
        result = self._fetch_stock_info_yf(ticker)
        if result is None:
            return None

        # Save to MongoDB
        if self.has_db:
            await self._db.save_stock_info(ticker, result)

        return result

    def _fetch_stock_info_yf(self, ticker: str) -> Optional[dict]:
        """Fetch fundamental info from yfinance."""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            return {
                "shares_outstanding": info.get("sharesOutstanding", 0) or 0,
                "float_shares": info.get("floatShares", 0) or 0,
                "market_cap": info.get("marketCap", 0) or 0,
                "name": info.get("shortName", ticker),
                "sector": info.get("sector", "Unknown"),
                "currency": info.get("currency", "IDR"),
            }
        except Exception as e:
            logger.error(f"Error fetching info for {ticker}: {e}")
            return None

    def fetch_stock_info(self, ticker: str) -> Optional[dict]:
        """Sync version — fetch stock info from yfinance only."""
        return self._fetch_stock_info_yf(ticker)

    async def fetch_stocks_info_async(self, tickers: list[str]) -> dict:
        """Fetch info for multiple stocks with MongoDB caching."""
        result = {}
        for ticker in tickers:
            info = await self.fetch_stock_info_async(ticker)
            if info:
                result[ticker] = info
        return result

    def fetch_stocks_info(self, tickers: list[str]) -> dict:
        """Sync version — fetch info for multiple stocks."""
        result = {}
        for ticker in tickers:
            info = self.fetch_stock_info(ticker)
            if info:
                result[ticker] = info
        return result

    # ─────────── HISTORICAL PRICES (with incremental MongoDB fetch) ───────────

    async def fetch_historical_incremental(
        self, tickers: list[str], base_date: str = "2025-01-02"
    ) -> dict:
        """
        Fetch historical data incrementally:
        1. Load existing data from MongoDB
        2. Determine last stored date per ticker
        3. Only fetch new dates from yfinance
        4. Save new data to MongoDB
        5. Return complete dataset as DataFrames
        """
        result = {}

        for ticker in tickers:
            try:
                existing_data = []
                last_date = None

                # Step 1: Load from MongoDB
                if self.has_db:
                    last_date = await self._db.get_last_price_date(ticker)
                    if last_date:
                        existing_data = await self._db.get_stock_prices(ticker, start_date=base_date)
                        logger.info(
                            f"  {ticker}: {len(existing_data)} prices from MongoDB (last: {last_date})"
                        )

                # Step 2: Determine what to fetch from yfinance
                if last_date:
                    # Fetch from day after last stored date
                    start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    today = datetime.now().strftime("%Y-%m-%d")

                    if start <= today:
                        logger.info(f"  {ticker}: Fetching new data from {start} to {today}")
                        new_data = self._fetch_historical_range(ticker, start, today)
                    else:
                        new_data = pd.DataFrame()
                        logger.info(f"  {ticker}: Already up to date")
                else:
                    # No data in MongoDB — fetch everything
                    logger.info(f"  {ticker}: No MongoDB data, fetching full history")
                    new_data = self._fetch_historical_full(ticker)

                # Step 3: Save new data to MongoDB
                if not new_data.empty and self.has_db:
                    prices_to_save = self._df_to_price_docs(new_data)
                    await self._db.save_stock_prices(ticker, prices_to_save)
                    logger.info(f"  {ticker}: Saved {len(prices_to_save)} new prices to MongoDB")

                # Step 4: Build complete DataFrame
                if existing_data and not new_data.empty:
                    # Merge MongoDB data + new yfinance data
                    existing_df = self._price_docs_to_df(existing_data)
                    combined = pd.concat([existing_df, new_data])
                    combined = combined[~combined.index.duplicated(keep="last")]
                    combined = combined.sort_index()
                    result[ticker] = combined
                elif existing_data:
                    result[ticker] = self._price_docs_to_df(existing_data)
                elif not new_data.empty:
                    result[ticker] = new_data
                else:
                    logger.warning(f"  {ticker}: No data available")

            except Exception as e:
                logger.error(f"Error in incremental fetch for {ticker}: {e}")
                continue

        return result

    def _fetch_historical_range(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """Fetch historical data for a date range from yfinance."""
        try:
            data = yf.download(
                ticker, start=start, end=end,
                interval="1d", progress=False, threads=True,
            )
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data = data.dropna(subset=["Close"])
            data.index = pd.to_datetime(data.index)
            return data
        except Exception as e:
            logger.error(f"Error fetching range for {ticker}: {e}")
            return pd.DataFrame()

    def _fetch_historical_full(self, ticker: str) -> pd.DataFrame:
        """Fetch full historical data from yfinance."""
        try:
            data = yf.download(
                ticker, period="max", interval="1d",
                progress=False, threads=True,
            )
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data = data.dropna(subset=["Close"])
            data.index = pd.to_datetime(data.index)
            return data
        except Exception as e:
            logger.error(f"Error fetching full history for {ticker}: {e}")
            return pd.DataFrame()

    def _df_to_price_docs(self, df: pd.DataFrame) -> list[dict]:
        """Convert a DataFrame to list of price documents for MongoDB."""
        docs = []
        for idx, row in df.iterrows():
            docs.append({
                "date": idx.strftime("%Y-%m-%d"),
                "open": float(row.get("Open", 0)),
                "high": float(row.get("High", 0)),
                "low": float(row.get("Low", 0)),
                "close": float(row.get("Close", 0)),
                "volume": int(row.get("Volume", 0)),
            })
        return docs

    def _price_docs_to_df(self, docs: list[dict]) -> pd.DataFrame:
        """Convert MongoDB price docs back to a DataFrame.
        Handles both timeseries format (capitalized) and lowercase format.
        """
        df = pd.DataFrame(docs)

        # The timeseries collection uses 'Date' (datetime) as the timeField
        # and capitalized column names (Open, High, Low, Close, Volume)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")

        # Rename lowercase to uppercase if needed
        rename_map = {}
        for col_lower, col_upper in [("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close"), ("volume", "Volume")]:
            if col_lower in df.columns and col_upper not in df.columns:
                rename_map[col_lower] = col_upper
        if rename_map:
            df = df.rename(columns=rename_map)

        # Remove non-OHLCV columns
        keep_cols = {"Open", "High", "Low", "Close", "Volume"}
        drop_cols = [c for c in df.columns if c not in keep_cols]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        return df

    def fetch_historical(
        self, tickers: list[str], period: str = "1y", interval: str = "1d"
    ) -> dict:
        """
        Sync fallback — fetch historical data without MongoDB.
        Used when MongoDB is not available.
        """
        result = {}
        try:
            ticker_str = " ".join(tickers)
            data = yf.download(
                ticker_str, period=period, interval=interval,
                group_by="ticker", progress=False, threads=True,
            )

            for ticker in tickers:
                try:
                    if len(tickers) == 1:
                        ticker_data = data
                    else:
                        ticker_data = data[ticker]

                    if isinstance(ticker_data.columns, pd.MultiIndex):
                        ticker_data.columns = ticker_data.columns.get_level_values(-1)

                    ticker_data = ticker_data.dropna(subset=["Close"])
                    result[ticker] = ticker_data
                except Exception as e:
                    logger.warning(f"Error processing historical {ticker}: {e}")
                    continue
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")

        return result


# Singleton instance
data_fetcher = DataFetcher()
