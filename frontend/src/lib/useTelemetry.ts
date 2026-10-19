import { useEffect, useRef, useState } from 'react';
import type { TelemetryData } from './types';
import { api } from './api';

export function useTelemetry() {
  const [telemetry, setTelemetry] = useState<TelemetryData | null>(null);
  const [isLive, setIsLive] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const fallbackIntervalRef = useRef<any>(null);

  const fetchPolling = async () => {
    try {
      const data = await api.getTelemetry();
      setTelemetry(data);
      setError(null);
    } catch (e: any) {
      setError(e.message || 'Telemetry polling failed');
    }
  };

  useEffect(() => {
    let active = true;
    let reconnectTimeout: any = null;

    const startPollingFallback = () => {
      if (!fallbackIntervalRef.current) {
        fetchPolling();
        fallbackIntervalRef.current = setInterval(fetchPolling, 2000);
      }
    };

    const stopPollingFallback = () => {
      if (fallbackIntervalRef.current) {
        clearInterval(fallbackIntervalRef.current);
        fallbackIntervalRef.current = null;
      }
    };

    const connectWebSocket = () => {
      if (!active) return;
      try {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.host;
        const wsUrl = `${protocol}//${host}/ws/telemetry`;

        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
          if (!active) return;
          setIsLive(true);
          setError(null);
          stopPollingFallback();
        };

        ws.onmessage = (event) => {
          if (!active) return;
          try {
            const data: TelemetryData = JSON.parse(event.data);
            setTelemetry(data);
            setIsLive(true);
          } catch {
            // Ignore parse errors
          }
        };

        ws.onerror = () => {
          if (!active) return;
          setIsLive(false);
          startPollingFallback();
        };

        ws.onclose = () => {
          if (!active) return;
          setIsLive(false);
          startPollingFallback();
          reconnectTimeout = setTimeout(connectWebSocket, 3000);
        };
      } catch {
        setIsLive(false);
        startPollingFallback();
      }
    };

    connectWebSocket();

    return () => {
      active = false;
      stopPollingFallback();
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, []);

  return { telemetry, isLive, error };
}
