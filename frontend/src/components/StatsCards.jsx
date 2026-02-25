import { TrendingUp, TrendingDown, BarChart3, DollarSign, Activity, Layers } from 'lucide-react';

export default function StatsCards({ indexData }) {
    if (!indexData) return null;

    const isPositive = indexData.change_percent >= 0;

    const formatNum = (num) => {
        if (!num) return '—';
        if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T`;
        if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
        if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
        return num.toLocaleString('id-ID');
    };

    const stats = [
        {
            label: 'Open',
            value: indexData.open?.toFixed(2) || '—',
            icon: <Activity size={14} />,
        },
        {
            label: 'Day High',
            value: indexData.high?.toFixed(2) || '—',
            icon: <TrendingUp size={14} />,
            highlight: true,
        },
        {
            label: 'Day Low',
            value: indexData.low?.toFixed(2) || '—',
            icon: <TrendingDown size={14} />,
        },
        {
            label: 'Prev Close',
            value: indexData.previous_close?.toFixed(2) || '—',
            icon: <BarChart3 size={14} />,
        },
        {
            label: 'Total Market Cap',
            value: `Rp ${formatNum(indexData.total_market_cap)}`,
            icon: <DollarSign size={14} />,
            sub: 'All constituents',
        },
        {
            label: 'FF Market Cap',
            value: `Rp ${formatNum(indexData.total_free_float_market_cap)}`,
            icon: <Layers size={14} />,
            sub: 'Free-float adjusted',
        },
    ];

    return (
        <div className="stats-grid">
            {stats.map((stat, i) => (
                <div className="stat-card" key={i}>
                    <div className="stat-label">
                        {stat.icon}
                        {stat.label}
                    </div>
                    <div className="stat-value">{stat.value}</div>
                    {stat.sub && <div className="stat-sub">{stat.sub}</div>}
                </div>
            ))}
        </div>
    );
}
