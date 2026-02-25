import { useMemo } from 'react';
import { TrendingUp, Calendar } from 'lucide-react';

const PERIODS = [
    { label: '1D', days: 1 },
    { label: '1W', days: 7 },
    { label: '1M', days: 30 },
    { label: '3M', days: 90 },
    { label: '6M', days: 180 },
    { label: 'YTD', days: 'ytd' },
    { label: '1Y', days: 365 },
    { label: 'ALL', days: 'all' },
];

export default function PerformanceReturns({ history }) {
    const returns = useMemo(() => {
        if (!history || history.length < 2) return [];

        const sorted = [...history].sort((a, b) => {
            const da = a.date || a.timestamp || '';
            const db = b.date || b.timestamp || '';
            return da.localeCompare(db);
        });

        const latest = sorted[sorted.length - 1];
        const latestVal = latest.close || latest.value || 0;
        const now = new Date();

        return PERIODS.map(({ label, days }) => {
            let cutoff;
            if (days === 'all') {
                const first = sorted[0];
                const firstVal = first.close || first.value || 0;
                if (firstVal <= 0) return { label, change: null };
                const pct = ((latestVal - firstVal) / firstVal) * 100;
                const daysDiff = Math.round((new Date(latest.date || latest.timestamp) - new Date(first.date || first.timestamp)) / 86400000);
                return { label, change: pct, days: daysDiff };
            } else if (days === 'ytd') {
                cutoff = new Date(now.getFullYear(), 0, 1);
            } else {
                cutoff = new Date(now.getTime() - days * 86400 * 1000);
            }

            // Find entry closest to cutoff
            let ref = null;
            for (const entry of sorted) {
                const d = new Date(entry.date || entry.timestamp);
                if (d >= cutoff) {
                    ref = entry;
                    break;
                }
            }

            if (!ref) return { label, change: null };

            const refVal = ref.close || ref.value || 0;
            if (refVal <= 0) return { label, change: null };

            const pct = ((latestVal - refVal) / refVal) * 100;
            return { label, change: pct };
        });
    }, [history]);

    if (returns.length === 0) return null;

    return (
        <div className="perf-section">
            <div className="perf-header">
                <Calendar size={18} />
                <span>Performance Returns</span>
            </div>
            <div className="perf-grid">
                {returns.map(({ label, change }) => {
                    const isUp = change !== null && change >= 0;
                    const isDown = change !== null && change < 0;
                    const displayVal = change !== null ? `${isUp ? '+' : ''}${change.toFixed(2)}%` : '—';

                    return (
                        <div key={label} className="perf-card">
                            <div className="perf-period">{label}</div>
                            <div className={`perf-value ${isUp ? 'up' : ''} ${isDown ? 'down' : ''}`}>
                                {displayVal}
                            </div>
                            {change !== null && (
                                <div className={`perf-bar ${isUp ? 'up' : 'down'}`}>
                                    <div
                                        className="perf-bar-fill"
                                        style={{ width: `${Math.min(Math.abs(change) * 3, 100)}%` }}
                                    />
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
