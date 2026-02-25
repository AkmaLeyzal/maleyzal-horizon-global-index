import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';
import { PieChart as PieIcon } from 'lucide-react';

const COLORS = [
    '#10b981', '#06b6d4', '#3b82f6', '#8b5cf6', '#f59e0b',
    '#ef4444', '#ec4899', '#14b8a6', '#f97316', '#6366f1',
];

export default function WeightChart({ constituents }) {
    if (!constituents || constituents.length === 0) return null;

    const sorted = [...constituents]
        .sort((a, b) => (b.weight || 0) - (a.weight || 0))
        .map((c, i) => ({
            name: c.ticker?.replace('.JK', '') || c.name,
            fullName: c.name,
            value: parseFloat(c.weight?.toFixed(2) || 0),
            color: COLORS[i % COLORS.length],
        }));

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
        <div className="panel">
            <div className="panel-header">
                <div className="panel-title">
                    <PieIcon size={18} />
                    Weight Distribution
                </div>
            </div>

            <div className="weight-chart-container">
                <ResponsiveContainer width="100%" height={240}>
                    <PieChart>
                        <Pie
                            data={sorted}
                            cx="50%"
                            cy="50%"
                            innerRadius={65}
                            outerRadius={100}
                            paddingAngle={2}
                            dataKey="value"
                            animationBegin={0}
                            animationDuration={800}
                            animationEasing="ease-out"
                        >
                            {sorted.map((entry, index) => (
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

                <div className="weight-legend">
                    {sorted.map((item, i) => (
                        <div className="weight-legend-item" key={i}>
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
