# Maleyzal Horizon Global Index (MHGI)

## 📌 Project Overview
MHGI is a custom equity index tracking application that follows selected IHSG constituents using the **Free-float Market Capitalization Weighting** methodology. It is inspired by Vanguard & MSCI index products, focusing on delivering a professional, institutional-grade financial dashboard.

## 🏗️ Architecture & Tech Stack

This is a full-stack application divided into a backend and frontend:

### Backend (`/backend`)
- **Framework**: FastAPI (Python)
- **Server**: Uvicorn
- **Data & Processing**: `yfinance` (for stock data fetching), `pandas`, `numpy`
- **Real-time Engine**: `websockets` (pushing updates every 60 seconds)
- **Database**: MongoDB (using `motor` async driver)
- **Task Scheduling**: `apscheduler`
- **Environment**: Managed via `.env` files (loaded via `python-dotenv`)

### Frontend (`/frontend`)
- **Framework**: React.js v19
- **Build Tool**: Vite
- **Data Visualization**: `recharts`, TradingView Lightweight Charts
- **Icons**: `lucide-react`
- **Styling**: Vanilla CSS (No Tailwind CSS currently installed)

## 🧮 Core Methodology
**Index Value** = Σ(Free-float Market Cap) / Divisor
- *Free-float Market Cap* = Price × Shares Outstanding × Free-float Factor
- *Divisor* is calibrated so that Index = 1000 on the base date.
- Constituents are configured via a JSON file (e.g., `backend/constituents.json`) using the `.JK` suffix for IHSG stocks.

## 🚀 Key Commands

**Running the Backend:**
```bash
cd backend
python main.py
# Runs on http://localhost:8000
```

**Running the Frontend:**
```bash
cd frontend
npm run dev
# Runs on http://localhost:5173
```

## 📋 Current Strategic Goals & Tasks
- **UI Redesign (`feat/stitch-ui` branch)**: Transform the dashboard into a data-dense, highly professional interface inspired by institutional platforms like MSCI and Vanguard.
- **Workflow**: Generate high-fidelity screens via Stitch and implement them cleanly.

## 🧠 Coding Guidelines for this Project
- Maintain a clean, professional aesthetic (dark modes, tailored HSL colors, crisp typography like Inter/Roboto).
- Ensure backend endpoints (`/api/index`, `/api/constituents`, etc.) accurately reflect the mathematical methodology without precision loss.
- Treat WebSocket performance carefully, ensuring the UI handles 60-second ticks smoothly without memory leaks.
