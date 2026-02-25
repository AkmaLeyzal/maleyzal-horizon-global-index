import { useEffect, useRef, useState, useMemo } from 'react';
import { TrendingUp, Info } from 'lucide-react';

const TIMEFRAMES = [
    { label: '1W', days: 7 },
    { label: '1M', days: 30 },
    { label: '3M', days: 90 },
    { label: '6M', days: 180 },
    { label: 'YTD', days: 'ytd' },
    { label: '1Y', days: 365 },
    { label: 'ALL', days: 'all' },
];

const CHART_PADDING = { top: 24, right: 60, bottom: 36, left: 12 };

function filterByTimeframe(history, tf) {
    if (!history || history.length === 0) return [];
    if (tf === 'ALL') return history;

    const now = new Date();
    let cutoff;

    if (tf === 'YTD') {
        cutoff = new Date(now.getFullYear(), 0, 1);
    } else {
        const tfConfig = TIMEFRAMES.find(t => t.label === tf);
        const days = tfConfig?.days || 365;
        cutoff = new Date(now.getTime() - days * 86400 * 1000);
    }

    return history.filter(point => {
        try {
            const d = point.date ? new Date(point.date) :
                point.time ? new Date(point.time * 1000) :
                    new Date(point.timestamp);
            return !isNaN(d.getTime()) && d >= cutoff;
        } catch { return false; }
    });
}

function processData(history) {
    if (!history || history.length === 0) return [];

    const dataMap = new Map();
    for (const point of history) {
        let dateStr;
        if (point.date) {
            dateStr = point.date;
        } else if (point.time) {
            const d = new Date(point.time * 1000);
            if (isNaN(d.getTime())) continue;
            dateStr = d.toISOString().slice(0, 10);
        } else if (point.timestamp) {
            const d = new Date(point.timestamp);
            if (isNaN(d.getTime())) continue;
            dateStr = d.toISOString().slice(0, 10);
        } else continue;

        const val = point.close || point.value || 0;
        if (val <= 0) continue;

        dataMap.set(dateStr, {
            date: dateStr,
            value: val,
            change: point.change || 0,
            changePct: point.change_percent || 0,
        });
    }

    return Array.from(dataMap.values()).sort((a, b) => a.date.localeCompare(b.date));
}

function formatDateLabel(dateStr, totalDays) {
    const d = new Date(dateStr);
    if (totalDays <= 31) return d.toLocaleDateString('id-ID', { day: 'numeric', month: 'short' });
    if (totalDays <= 180) return d.toLocaleDateString('id-ID', { month: 'short', year: '2-digit' });
    return d.toLocaleDateString('id-ID', { month: 'short', year: 'numeric' });
}

function formatValue(val) {
    return val.toLocaleString('id-ID', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export default function IndexChart({ history }) {
    const containerRef = useRef(null);
    const [dimensions, setDimensions] = useState({ width: 900, height: 400 });
    const [activeTF, setActiveTF] = useState('1Y');
    const [hoverIndex, setHoverIndex] = useState(null);
    const [showMethodology, setShowMethodology] = useState(false);

    // Resize observer
    useEffect(() => {
        if (!containerRef.current) return;
        const ro = new ResizeObserver(entries => {
            for (const entry of entries) {
                const { width } = entry.contentRect;
                setDimensions({ width: Math.max(width, 300), height: 400 });
            }
        });
        ro.observe(containerRef.current);
        return () => ro.disconnect();
    }, []);

    const data = useMemo(() => {
        const filtered = filterByTimeframe(history, activeTF);
        return processData(filtered);
    }, [history, activeTF]);

    const chartArea = useMemo(() => ({
        x: CHART_PADDING.left,
        y: CHART_PADDING.top,
        width: dimensions.width - CHART_PADDING.left - CHART_PADDING.right,
        height: dimensions.height - CHART_PADDING.top - CHART_PADDING.bottom,
    }), [dimensions]);

    // Scale calculations
    const { minVal, maxVal, path, areaPath, gridLines, xLabels, currentPoint } = useMemo(() => {
        if (data.length === 0) return { minVal: 0, maxVal: 0, path: '', areaPath: '', gridLines: [], xLabels: [], currentPoint: null };

        const values = data.map(d => d.value);
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const padding = range * 0.08;
        const adjMin = min - padding;
        const adjMax = max + padding;
        const adjRange = adjMax - adjMin;

        const scaleX = (i) => chartArea.x + (i / (data.length - 1 || 1)) * chartArea.width;
        const scaleY = (v) => chartArea.y + chartArea.height - ((v - adjMin) / adjRange) * chartArea.height;

        // Build SVG path
        const points = data.map((d, i) => ({ x: scaleX(i), y: scaleY(d.value) }));
        const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
        const areaPathStr = linePath +
            ` L${points[points.length - 1].x.toFixed(1)},${chartArea.y + chartArea.height}` +
            ` L${points[0].x.toFixed(1)},${chartArea.y + chartArea.height} Z`;

        // Horizontal grid lines (5 lines)
        const gridCount = 5;
        const grids = [];
        for (let i = 0; i <= gridCount; i++) {
            const val = adjMin + (adjRange / gridCount) * i;
            const y = scaleY(val);
            grids.push({ y, value: val });
        }

        // X-axis labels
        const totalDays = data.length;
        const labelCount = Math.min(6, totalDays);
        const step = Math.max(1, Math.floor(totalDays / labelCount));
        const labels = [];
        for (let i = 0; i < totalDays; i += step) {
            labels.push({ x: scaleX(i), label: formatDateLabel(data[i].date, totalDays) });
        }

        const last = data[data.length - 1];
        const lastPoint = points[points.length - 1];

        return {
            minVal: adjMin,
            maxVal: adjMax,
            path: linePath,
            areaPath: areaPathStr,
            gridLines: grids,
            xLabels: labels,
            currentPoint: last ? { ...last, x: lastPoint.x, y: lastPoint.y } : null,
        };
    }, [data, chartArea]);

    // Mouse tracking
    const handleMouseMove = (e) => {
        if (data.length === 0 || !containerRef.current) return;
        const rect = containerRef.current.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const relX = (mouseX - chartArea.x) / chartArea.width;
        const idx = Math.round(relX * (data.length - 1));
        setHoverIndex(Math.max(0, Math.min(data.length - 1, idx)));
    };

    const handleMouseLeave = () => setHoverIndex(null);

    const hoverData = hoverIndex !== null ? data[hoverIndex] : null;
    const hoverX = hoverIndex !== null ? chartArea.x + (hoverIndex / (data.length - 1 || 1)) * chartArea.width : 0;
    const hoverY = hoverData ? (() => {
        const values = data.map(d => d.value);
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const padding = range * 0.08;
        const adjMin = min - padding;
        const adjMax = max + padding;
        return chartArea.y + chartArea.height - ((hoverData.value - adjMin) / (adjMax - adjMin)) * chartArea.height;
    })() : 0;

    // Performance metrics
    const perfData = useMemo(() => {
        if (data.length < 2) return null;
        const first = data[0].value;
        const last = data[data.length - 1].value;
        const change = last - first;
        const changePct = (change / first) * 100;
        return { first, last, change, changePct };
    }, [data]);

    const isPositive = perfData ? perfData.change >= 0 : true;
    const lineColor = isPositive ? '#10b981' : '#ef4444';
    const gradientId = `chart-gradient-${isPositive ? 'up' : 'down'}`;

    return (
        <div className="chart-section">
            <div className="chart-container">
                <div className="chart-header">
                    <div className="chart-title">
                        <TrendingUp size={18} />
                        <span>MHGI Performance</span>
                        {perfData && (
                            <span className={`chart-perf-badge ${isPositive ? 'up' : 'down'}`}>
                                {isPositive ? '▲' : '▼'} {Math.abs(perfData.changePct).toFixed(2)}%
                            </span>
                        )}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <button
                            className="methodology-btn"
                            onClick={() => setShowMethodology(!showMethodology)}
                            title="How is this chart calculated?"
                        >
                            <Info size={14} />
                        </button>
                        <div className="chart-timeframe">
                            {TIMEFRAMES.map(({ label }) => (
                                <button
                                    key={label}
                                    className={`tf-btn ${activeTF === label ? 'active' : ''}`}
                                    onClick={() => setActiveTF(label)}
                                >
                                    {label}
                                </button>
                            ))}
                        </div>
                    </div>
                </div>

                {showMethodology && (
                    <div className="methodology-tooltip">
                        <strong>Index Methodology (Divisor Method)</strong>
                        <p>
                            This chart shows the <em>normalized</em> MHGI index value — not raw stock prices.
                            Each stock's contribution is weighted by its <strong>Free-float Market Capitalization</strong>,
                            so a stock priced at Rp 9,000 and one at Rp 1,600 are both fairly represented
                            based on their total investable market value, not their per-share price.
                        </p>
                        <p style={{ marginTop: '6px' }}>
                            <code>Index = Σ(Price × Shares × FreeFloat) / Divisor</code>
                        </p>
                    </div>
                )}

                {/* Hover tooltip */}
                {hoverData && (
                    <div className="chart-tooltip" style={{
                        left: `${Math.min(hoverX + 12, dimensions.width - 180)}px`,
                        top: `${Math.max(hoverY - 60, CHART_PADDING.top)}px`
                    }}>
                        <div className="tooltip-date">{new Date(hoverData.date).toLocaleDateString('id-ID', {
                            weekday: 'short', day: 'numeric', month: 'long', year: 'numeric'
                        })}</div>
                        <div className="tooltip-value">{formatValue(hoverData.value)}</div>
                        <div className={`tooltip-change ${hoverData.changePct >= 0 ? 'up' : 'down'}`}>
                            {hoverData.changePct >= 0 ? '+' : ''}{hoverData.changePct.toFixed(4)}%
                        </div>
                    </div>
                )}

                <div className="chart-wrapper" ref={containerRef} onMouseMove={handleMouseMove} onMouseLeave={handleMouseLeave}>
                    {data.length === 0 ? (
                        <div className="chart-empty">
                            <div className="loader-ring" style={{ margin: '0 auto 12px' }} />
                            <span>Loading index data...</span>
                        </div>
                    ) : (
                        <svg width={dimensions.width} height={dimensions.height} style={{ display: 'block' }}>
                            <defs>
                                <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={lineColor} stopOpacity="0.25" />
                                    <stop offset="50%" stopColor={lineColor} stopOpacity="0.08" />
                                    <stop offset="100%" stopColor={lineColor} stopOpacity="0" />
                                </linearGradient>
                                <filter id="glow">
                                    <feGaussianBlur stdDeviation="3" result="blur" />
                                    <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
                                </filter>
                            </defs>

                            {/* Grid lines */}
                            {gridLines.map((g, i) => (
                                <g key={i}>
                                    <line
                                        x1={chartArea.x} y1={g.y}
                                        x2={chartArea.x + chartArea.width} y2={g.y}
                                        stroke="rgba(255,255,255,0.04)" strokeWidth="1"
                                    />
                                    <text
                                        x={chartArea.x + chartArea.width + 8} y={g.y + 4}
                                        fill="#475569" fontSize="10" fontFamily="'JetBrains Mono', monospace"
                                    >
                                        {g.value.toFixed(0)}
                                    </text>
                                </g>
                            ))}

                            {/* X labels */}
                            {xLabels.map((l, i) => (
                                <text key={i} x={l.x} y={chartArea.y + chartArea.height + 20}
                                    fill="#475569" fontSize="10" textAnchor="middle"
                                    fontFamily="'Inter', sans-serif"
                                >
                                    {l.label}
                                </text>
                            ))}

                            {/* Area fill */}
                            <path d={areaPath} fill={`url(#${gradientId})`} />

                            {/* Main line */}
                            <path d={path} fill="none" stroke={lineColor} strokeWidth="2"
                                strokeLinejoin="round" strokeLinecap="round"
                            />

                            {/* Glow line behind */}
                            <path d={path} fill="none" stroke={lineColor} strokeWidth="4"
                                strokeLinejoin="round" opacity="0.15" filter="url(#glow)"
                            />

                            {/* Base value reference line */}
                            {data.length > 1 && (() => {
                                const baseVal = data[0].value;
                                const values = data.map(d => d.value);
                                const min = Math.min(...values);
                                const max = Math.max(...values);
                                const range = max - min || 1;
                                const padding = range * 0.08;
                                const adjMin = min - padding;
                                const adjMax = max + padding;
                                const baseY = chartArea.y + chartArea.height - ((baseVal - adjMin) / (adjMax - adjMin)) * chartArea.height;
                                return (
                                    <g>
                                        <line
                                            x1={chartArea.x} y1={baseY}
                                            x2={chartArea.x + chartArea.width} y2={baseY}
                                            stroke="rgba(148,163,184,0.2)" strokeWidth="1" strokeDasharray="4 4"
                                        />
                                        <text x={chartArea.x + chartArea.width + 8} y={baseY + 4}
                                            fill="#64748b" fontSize="9" fontFamily="'JetBrains Mono', monospace"
                                        >
                                            {baseVal.toFixed(0)}
                                        </text>
                                    </g>
                                );
                            })()}

                            {/* Current value indicator */}
                            {currentPoint && (
                                <g>
                                    <circle cx={currentPoint.x} cy={currentPoint.y} r="5"
                                        fill={lineColor} stroke="#0a0e1a" strokeWidth="2"
                                    />
                                    <circle cx={currentPoint.x} cy={currentPoint.y} r="9"
                                        fill="none" stroke={lineColor} strokeWidth="1" opacity="0.3"
                                    >
                                        <animate attributeName="r" from="6" to="14" dur="2s" repeatCount="indefinite" />
                                        <animate attributeName="opacity" from="0.5" to="0" dur="2s" repeatCount="indefinite" />
                                    </circle>
                                </g>
                            )}

                            {/* Hover crosshair */}
                            {hoverData && (
                                <g>
                                    <line x1={hoverX} y1={chartArea.y} x2={hoverX} y2={chartArea.y + chartArea.height}
                                        stroke="rgba(148,163,184,0.3)" strokeWidth="1" strokeDasharray="3 3"
                                    />
                                    <line x1={chartArea.x} y1={hoverY} x2={chartArea.x + chartArea.width} y2={hoverY}
                                        stroke="rgba(148,163,184,0.2)" strokeWidth="1" strokeDasharray="3 3"
                                    />
                                    <circle cx={hoverX} cy={hoverY} r="5"
                                        fill="#0a0e1a" stroke={lineColor} strokeWidth="2"
                                    />
                                </g>
                            )}
                        </svg>
                    )}
                </div>
            </div>
        </div>
    );
}
