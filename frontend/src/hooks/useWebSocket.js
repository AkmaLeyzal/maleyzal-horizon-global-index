import { useState, useEffect, useRef, useCallback } from 'react';

const WS_URL = 'ws://localhost:8000/ws/index';
const RECONNECT_DELAY = 3000;
const MAX_RECONNECT = 10;

export function useWebSocket() {
    const [indexData, setIndexData] = useState(null);
    const [constituents, setConstituents] = useState([]);
    const [history, setHistory] = useState([]);
    const [connected, setConnected] = useState(false);
    const [error, setError] = useState(null);

    const wsRef = useRef(null);
    const reconnectCount = useRef(0);
    const reconnectTimer = useRef(null);
    const pingTimer = useRef(null);

    const connect = useCallback(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) return;

        try {
            const ws = new WebSocket(WS_URL);
            wsRef.current = ws;

            ws.onopen = () => {
                setConnected(true);
                setError(null);
                reconnectCount.current = 0;

                // Ping every 30s to keep connection alive
                pingTimer.current = setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send('ping');
                    }
                }, 30000);
            };

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);

                    if (data.type === 'initial' || data.type === 'index_update' || data.type === 'eod_update') {
                        if (data.index) {
                            setIndexData(data.index);
                        }
                        if (data.constituents) {
                            setConstituents(data.constituents);
                        }
                        if (data.history) {
                            setHistory(prev => {
                                const existing = new Set(prev.map(p => p.date || p.timestamp));
                                const newPoints = data.history.filter(p => !existing.has(p.date || p.timestamp));
                                return [...prev, ...newPoints];
                            });
                        }

                        // For EOD updates, append the new daily point
                        if (data.type === 'eod_update' && data.index && data.date) {
                            setHistory(prev => {
                                // Replace or append today's entry
                                const filtered = prev.filter(p => (p.date || '') !== data.date);
                                return [...filtered, {
                                    date: data.date,
                                    timestamp: data.timestamp,
                                    value: data.index.value,
                                    close: data.index.value,
                                    time: Math.floor(Date.now() / 1000),
                                }];
                            });
                        }
                    }
                } catch (e) {
                    console.warn('WS parse error:', e);
                }
            };

            ws.onclose = () => {
                setConnected(false);
                clearInterval(pingTimer.current);

                if (reconnectCount.current < MAX_RECONNECT) {
                    reconnectCount.current++;
                    reconnectTimer.current = setTimeout(connect, RECONNECT_DELAY);
                }
            };

            ws.onerror = (e) => {
                setError('Connection error');
                console.error('WebSocket error:', e);
            };
        } catch (e) {
            setError('Failed to connect');
            console.error('WS connect error:', e);
        }
    }, []);

    useEffect(() => {
        connect();

        return () => {
            clearInterval(pingTimer.current);
            clearTimeout(reconnectTimer.current);
            if (wsRef.current) {
                wsRef.current.close();
            }
        };
    }, [connect]);

    return {
        indexData,
        constituents,
        history,
        connected,
        error,
        reconnect: connect,
    };
}
