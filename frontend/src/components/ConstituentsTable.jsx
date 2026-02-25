import { useState } from 'react';
import { List } from 'lucide-react';

export default function ConstituentsTable({ constituents }) {
    if (!constituents || constituents.length === 0) return null;

    const maxWeight = Math.max(...constituents.map(c => c.weight || 0));

    const formatPrice = (price) => {
        if (!price && price !== 0) return '—';
        return price.toLocaleString('id-ID');
    };

    const formatMCap = (num) => {
        if (!num) return '—';
        if (num >= 1e15) return `${(num / 1e15).toFixed(2)}P`;
        if (num >= 1e12) return `${(num / 1e12).toFixed(1)}T`;
        if (num >= 1e9) return `${(num / 1e9).toFixed(1)}B`;
        if (num >= 1e6) return `${(num / 1e6).toFixed(1)}M`;
        return num.toLocaleString('id-ID');
    };

    const sorted = [...constituents].sort((a, b) => (b.weight || 0) - (a.weight || 0));

    const colors = [
        'linear-gradient(135deg, #10b981, #059669)',
        'linear-gradient(135deg, #3b82f6, #2563eb)',
        'linear-gradient(135deg, #8b5cf6, #7c3aed)',
        'linear-gradient(135deg, #f59e0b, #d97706)',
        'linear-gradient(135deg, #ef4444, #dc2626)',
        'linear-gradient(135deg, #ec4899, #db2777)',
        'linear-gradient(135deg, #06b6d4, #0891b2)',
        'linear-gradient(135deg, #14b8a6, #0d9488)',
        'linear-gradient(135deg, #f97316, #ea580c)',
        'linear-gradient(135deg, #6366f1, #4f46e5)',
    ];

    return (
        <div className="table-panel">
            <div className="table-header">
                <div className="table-title">
                    <List size={18} />
                    <span>Constituents</span>
                </div>
                <span style={{
                    fontSize: '11px',
                    color: 'var(--text-muted)',
                    background: 'var(--bg-surface)',
                    padding: '3px 10px',
                    borderRadius: '12px',
                    fontWeight: 600,
                    border: '1px solid var(--border-dim)',
                }}>{constituents.length} stocks</span>
            </div>

            <div style={{ overflowX: 'auto' }}>
                <table className="constituent-table">
                    <thead>
                        <tr>
                            <th style={{ minWidth: '200px', paddingLeft: '20px' }}>Stock</th>
                            <th style={{ minWidth: '130px' }}>Sector</th>
                            <th style={{ minWidth: '90px', textAlign: 'right' }}>Price</th>
                            <th style={{ minWidth: '80px', textAlign: 'center' }}>Change</th>
                            <th style={{ minWidth: '80px', textAlign: 'right' }}>FF MCap</th>
                            <th style={{ minWidth: '150px', textAlign: 'right', paddingRight: '20px' }}>Weight</th>
                        </tr>
                    </thead>
                    <tbody>
                        {sorted.map((stock, idx) => {
                            const ticker = stock.ticker?.replace('.JK', '') || '';
                            const isUp = (stock.change_percent || 0) >= 0;

                            return (
                                <tr key={stock.ticker}>
                                    <td style={{ paddingLeft: '20px' }}>
                                        <div className="ticker-cell">
                                            <TickerLogo ticker={ticker} fallbackColor={colors[idx % colors.length]} />
                                            <div className="ticker-info">
                                                <div className="ticker-name">{stock.name}</div>
                                                <div className="ticker-code">{stock.ticker}</div>
                                            </div>
                                        </div>
                                    </td>
                                    <td className="sector-cell">{stock.sector}</td>
                                    <td style={{ textAlign: 'right' }}>
                                        <span className="price-cell">Rp {formatPrice(stock.price)}</span>
                                    </td>
                                    <td style={{ textAlign: 'center' }}>
                                        <span className={`change-badge ${isUp ? 'positive' : 'negative'}`}>
                                            {isUp ? '+' : ''}{stock.change_percent?.toFixed(2)}%
                                        </span>
                                    </td>
                                    <td style={{ textAlign: 'right' }}>
                                        <span className="mcap-cell">{formatMCap(stock.free_float_market_cap)}</span>
                                    </td>
                                    <td style={{ textAlign: 'right', paddingRight: '20px' }}>
                                        <div className="weight-cell">
                                            <div className="weight-bar-bg">
                                                <div
                                                    className="weight-bar-fill"
                                                    style={{ width: `${maxWeight ? (stock.weight / maxWeight) * 100 : 0}%` }}
                                                />
                                            </div>
                                            <span className="weight-value">{stock.weight?.toFixed(1)}%</span>
                                        </div>
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

/**
 * TickerLogo — shows the company logo from /logos/{TICKER}.png
 * Falls back to a colored initial avatar if the image doesn't exist.
 * 
 * To add a real logo:
 *   1. Save the image to: frontend/public/logos/{TICKER}.png
 *      Example: frontend/public/logos/BBCA.png
 *   2. Recommended: 64x64 or 128x128 PNG with transparent background
 *   3. The component will automatically detect and display it
 */
function TickerLogo({ ticker, fallbackColor }) {
    const [imgError, setImgError] = useState(false);

    if (imgError) {
        return (
            <div className="ticker-avatar" style={{ background: fallbackColor }}>
                {ticker.slice(0, 2)}
            </div>
        );
    }

    return (
        <img
            className="ticker-logo"
            src={`/logos/${ticker}.png`}
            alt={ticker}
            onError={() => setImgError(true)}
        />
    );
}
