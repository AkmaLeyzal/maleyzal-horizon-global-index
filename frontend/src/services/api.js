const API_BASE = 'http://localhost:8000';

export async function fetchIndex() {
    const res = await fetch(`${API_BASE}/api/index`);
    if (!res.ok) throw new Error('Failed to fetch index');
    return res.json();
}

export async function fetchConstituents() {
    const res = await fetch(`${API_BASE}/api/constituents`);
    if (!res.ok) throw new Error('Failed to fetch constituents');
    return res.json();
}

export async function fetchHistory(days = 365) {
    const res = await fetch(`${API_BASE}/api/history?days=${days}`);
    if (!res.ok) throw new Error('Failed to fetch history');
    return res.json();
}

export async function fetchFullHistory() {
    const res = await fetch(`${API_BASE}/api/history/full`);
    if (!res.ok) throw new Error('Failed to fetch full history');
    return res.json();
}

export async function fetchMeta() {
    const res = await fetch(`${API_BASE}/api/meta`);
    if (!res.ok) throw new Error('Failed to fetch meta');
    return res.json();
}

export async function fetchHealth() {
    const res = await fetch(`${API_BASE}/api/health`);
    if (!res.ok) throw new Error('Failed to fetch health');
    return res.json();
}
