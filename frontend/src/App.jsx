import { useState, useEffect } from 'react';
import { Clock, WifiOff } from 'lucide-react';

import IndexChart from './components/IndexChart';
import StatsCards from './components/StatsCards';
import PerformanceReturns from './components/PerformanceReturns';
import ConstituentsTable from './components/ConstituentsTable';
import WeightChart from './components/WeightChart';
import { useWebSocket } from './hooks/useWebSocket';
import { fetchFullHistory, fetchIndex, fetchConstituents } from './services/api';

export default function App() {
  const { indexData, constituents, history, connected, reconnect } = useWebSocket();
  const [initialHistory, setInitialHistory] = useState([]);
  const [initialLoading, setInitialLoading] = useState(true);
  const [fallbackIndex, setFallbackIndex] = useState(null);
  const [fallbackConstituents, setFallbackConstituents] = useState([]);

  // Fetch initial data via REST with retry
  useEffect(() => {
    let retries = 0;
    const maxRetries = 5;
    let timer = null;

    async function loadInitial() {
      try {
        const [histRes, idxRes, constRes] = await Promise.all([
          fetchFullHistory().catch(() => null),
          fetchIndex().catch(() => null),
          fetchConstituents().catch(() => null),
        ]);

        let gotData = false;
        if (histRes?.history) {
          setInitialHistory(histRes.history);
          gotData = true;
        }
        if (idxRes?.index) {
          setFallbackIndex(idxRes.index);
          gotData = true;
        }
        if (constRes?.constituents) {
          setFallbackConstituents(constRes.constituents);
          gotData = true;
        }

        if (!gotData && retries < maxRetries) {
          retries++;
          timer = setTimeout(loadInitial, 3000);
          return;
        }
      } catch (e) {
        console.warn('Initial fetch failed:', e);
        if (retries < maxRetries) {
          retries++;
          timer = setTimeout(loadInitial, 3000);
          return;
        }
      }
      setInitialLoading(false);
    }
    loadInitial();
    return () => clearTimeout(timer);
  }, []);

  // Merge history sources
  const mergedHistory = history.length > 0 ? history : initialHistory;
  const activeIndex = indexData || fallbackIndex;
  const activeConstituents = constituents.length > 0 ? constituents : fallbackConstituents;

  const isPositive = (activeIndex?.change_percent || 0) >= 0;

  const formatTime = (ts) => {
    if (!ts) return '';
    const d = new Date(ts);
    return d.toLocaleString('id-ID', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  if (initialLoading) {
    return (
      <div className="loading-container">
        <div className="loader-ring" />
        <div className="loading-text">Loading MHGI Index...</div>
        <div className="loading-sub">Connecting to data feed</div>
      </div>
    );
  }

  return (
    <>
      {/* Header */}
      <header className="header">
        <div className="header-inner">
          <div className="header-brand">
            <div className="header-logo">M</div>
            <div>
              <div className="header-title">MHGI</div>
              <div className="header-subtitle">Maleyzal Horizon Global Index</div>
            </div>
          </div>

          <div className="header-status">
            {connected ? (
              <div className="live-indicator" style={{ background: 'var(--accent-cyan-dim)', borderColor: 'rgba(6,182,212,0.2)' }}>
                <Clock size={14} style={{ color: 'var(--accent-cyan)' }} />
                End of Day
              </div>
            ) : (
              <button
                className="live-indicator"
                style={{ cursor: 'pointer', background: 'var(--negative-bg)', borderColor: 'rgba(239,68,68,0.2)' }}
                onClick={reconnect}
              >
                <WifiOff size={14} />
                Reconnect
              </button>
            )}
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="app-container">
        {/* Hero Section */}
        <section className="hero-section">
          <div className="index-value-hero">
            <span className="index-value-number">
              {activeIndex?.value?.toFixed(2) || '—'}
            </span>
            {activeIndex && (
              <div className={`index-change ${isPositive ? 'positive' : 'negative'}`}>
                <span className="index-change-value">
                  {isPositive ? '+' : ''}{activeIndex.change?.toFixed(2)}
                </span>
                <span className="index-change-percent">
                  {isPositive ? '+' : ''}{activeIndex.change_percent?.toFixed(2)}%
                </span>
              </div>
            )}
          </div>
          <div className="index-meta-line">
            Free-float Market Cap Weighted Index (Divisor Method) • IHSG Constituents •
            Calculated daily at 17:00 WIB •
            Updated {formatTime(activeIndex?.timestamp)}
          </div>
        </section>

        {/* Stats Cards */}
        <StatsCards indexData={activeIndex} />

        {/* Chart */}
        <IndexChart history={mergedHistory} />

        {/* Performance Returns */}
        <PerformanceReturns history={mergedHistory} />

        {/* Constituents + Weight Distribution */}
        <div className="panels-grid">
          <ConstituentsTable constituents={activeConstituents} />
          <WeightChart constituents={activeConstituents} />
        </div>
      </main>

      {/* Footer */}
      <footer className="footer">
        <div className="footer-inner">
          <div className="footer-text">
            © 2025 Maleyzal Horizon Global Index. Calculated daily using closing prices.
          </div>
          <div className="footer-links">
            <a href="#">Methodology</a>
            <a href="#">API Docs</a>
            <a href="#">GitHub</a>
          </div>
        </div>
      </footer>
    </>
  );
}
