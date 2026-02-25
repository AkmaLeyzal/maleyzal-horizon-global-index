# MHGI — Maleyzal Horizon Global Index

Custom equity index tracking selected IHSG constituents using **Free-float Market Capitalization Weighting** methodology. Inspired by Vanguard & MSCI index products.

## Tech Stack

- **Backend**: FastAPI + Python (yfinance, pandas, numpy)
- **Frontend**: React.js + Vite (TradingView Lightweight Charts, Recharts)
- **Real-time**: WebSocket push updates every 60 seconds

## Quick Start

### 1. Backend
```bash
cd backend
pip install -r requirements.txt
python main.py
```
Server will start at `http://localhost:8000`

### 2. Frontend
```bash
cd frontend
npm install
npm run dev
```
App will open at `http://localhost:5173`

## Methodology

**Index Value** = Σ(Free-float Market Cap) / Divisor

Where:
- Free-float Market Cap = Price × Shares Outstanding × Free-float Factor
- Divisor is calibrated so that Index = 1000 on the base date

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/index` | Current index value and stats |
| `GET /api/constituents` | All constituent stocks with weights |
| `GET /api/history/full` | Full historical index data |
| `GET /api/meta` | Index metadata |
| `WS /ws/index` | Real-time WebSocket feed |

## Constituents

Configure stock constituents in `backend/constituents.json`. Each entry includes:
- Ticker symbol (`.JK` suffix for IHSG)
- Company name and sector
- Free-float factor (0.0 - 1.0)