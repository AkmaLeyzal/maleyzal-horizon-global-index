import { useMemo } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';
import { PieChart as PieIcon } from 'lucide-react';

const COLORS = [
    '#10b981', '#06b6d4', '#3b82f6', '#8b5cf6', '#f59e0b',
    '#ef4444', '#ec4899', '#14b8a6', '#f97316', '#6366f1',
    '#22d3ee', '#a78bfa', '#fbbf24', '#f87171', '#fb923c',
    '#34d399', '#60a5fa', '#c084fc', '#facc15', '#4ade80',
];

const TOP_N = 20;

export default function WeightChart({ constituents }) {
    if (!constituents || constituents.length === 0) return null;

    const { chartData, legendData } = useMemo(() => {
        const sorted = [...constituents]
            .sort((a, b) => (b.weight || 0) - (a.weight || 0));

        const top = sorted.slice(0, TOP_N).map((c, i) => ({
            name: c.ticker?.replace('.JK', '') || c.name,
            fullName: c.name,
            value: parseFloat(c.weight?.toFixed(2) || 0),
            color: COLORS[i % COLORS.length],
        }));

        // Group the rest as "Lainnya"
        const rest = sorted.slice(TOP_N);
        const othersWeight = rest.reduce((sum, c) => sum + (c.weight || 0), 0);

        const othersEntry = rest.length > 0 ? {
            name: 'Lainnya',
            fullName: `Lainnya (${rest.length} saham)`,
            value: parseFloat(othersWeight.toFixed(2)),
            color: '#475569',
        } : null;

        const chart = othersEntry ? [...top, othersEntry] : top;
        const legend = othersEntry ? [...top, othersEntry] : top;

        return { chartData: chart, legendData: legend };
    }, [constituents]);

    const CustomTooltip = ({ active, payload }) => {
        if (active && payload && payload.length) {
            const data = payload[0].payload;
            return (
                <div style={{
                    background: 'rgba(17, 24, 39, 0.95)',
                    border: '1px solid rgba(255,255,255,0.1)',
                    borderRadius: '8px',
                    padding: '10px 14px',
                    fontSize: '12px',
                    backdropFilter: 'blur(8px)',
                }}>
                    <div style={{ fontWeight: 600, marginBottom: 4 }}>{data.fullName}</div>
                    <div style={{ color: data.color, fontFamily: "'JetBrains Mono', monospace" }}>
                        {data.value.toFixed(2)}%
                    </div>
                </div>
            );
        }
        return null;
    };

    return (
        <div className="panel weight-panel">
            <div className="panel-header">
                <div className="panel-title">
                    <PieIcon size={18} />
                    Weight Distribution
                </div>
                <span style={{
                    fontSize: '11px',
                    color: 'var(--text-muted)',
                    background: 'var(--bg-surface)',
                    padding: '3px 10px',
                    borderRadius: '12px',
                    fontWeight: 600,
                    border: '1px solid var(--border-dim)',
                }}>Top {TOP_N}</span>
            </div>

            <div className="weight-chart-container">
                <ResponsiveContainer width="100%" height={220}>
                    <PieChart>
                        <Pie
                            data={chartData}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={95}
                            paddingAngle={1.5}
                            dataKey="value"
                            animationBegin={0}
                            animationDuration={800}
                            animationEasing="ease-out"
                        >
                            {chartData.map((entry, index) => (
                                <Cell
                                    key={`cell-${index}`}
                                    fill={entry.color}
                                    strokeWidth={0}
                                />
                            ))}
                        </Pie>
                        <Tooltip content={<CustomTooltip />} />
                    </PieChart>
                </ResponsiveContainer>
            </div>

            {/* Scrollable legend */}
            <div className="weight-legend-scroll">
                <div className="weight-legend-grid">
                    {legendData.map((item, i) => (
                        <div className={`weight-legend-item ${item.name === 'Lainnya' ? 'others' : ''}`} key={i}>
                            <div className="legend-dot" style={{ background: item.color }} />
                            <span className="legend-name">{item.name}</span>
                            <span className="legend-value">{item.value.toFixed(1)}%</span>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
