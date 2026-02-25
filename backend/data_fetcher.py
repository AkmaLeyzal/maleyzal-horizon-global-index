"""
Data fetcher module — retrieves stock data from Yahoo Finance via yfinance.
Handles Indonesian stocks with .JK suffix.
"""
import yfinance as yf
import pandas as pd
import numpy as np
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger("mhgi.data_fetcher")


class DataFetcher:
    """Fetches stock data from Yahoo Finance for IHSG constituents."""

    def __init__(self):
        self._cache: dict = {}
        self._cache_expiry: dict = {}
        self._cache_ttl = 55  # seconds

    def _is_cache_valid(self, key: str) -> bool:
        if key in self._cache_expiry:
            return datetime.now() < self._cache_expiry[key]
        return False

    def _set_cache(self, key: str, data):
        self._cache[key] = data
        self._cache_expiry[key] = datetime.now() + timedelta(seconds=self._cache_ttl)

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

                    # Flatten MultiIndex columns if necessary
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

    def fetch_stock_info(self, ticker: str) -> Optional[dict]:
        """
        Fetch fundamental info for a single stock.
        Returns shares outstanding, float shares, market cap.
        """
        cache_key = f"info_{ticker}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            result = {
                "shares_outstanding": info.get("sharesOutstanding", 0) or 0,
                "float_shares": info.get("floatShares", 0) or 0,
                "market_cap": info.get("marketCap", 0) or 0,
                "name": info.get("shortName", ticker),
                "sector": info.get("sector", "Unknown"),
                "currency": info.get("currency", "IDR"),
            }
            self._set_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error fetching info for {ticker}: {e}")
            return None

    def fetch_stocks_info(self, tickers: list[str]) -> dict:
        """Fetch info for multiple stocks."""
        result = {}
        for ticker in tickers:
            info = self.fetch_stock_info(ticker)
            if info:
                result[ticker] = info
        return result

    def fetch_historical(
        self, tickers: list[str], period: str = "1y", interval: str = "1d"
    ) -> dict:
        """
        Fetch historical data for multiple tickers.
        Returns dict of {ticker: DataFrame}
        """
        result = {}
        try:
            ticker_str = " ".join(tickers)
            data = yf.download(
                ticker_str,
                period=period,
                interval=interval,
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
