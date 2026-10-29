import React, { useEffect, useRef, useState } from 'react';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
import {
  Terminal as TerminalIcon,
  RefreshCw,
  Trash2,
  GitBranch,
  Copy,
  Check,
} from 'lucide-react';

export const TerminalView: React.FC = () => {
  const terminalRef = useRef<HTMLDivElement>(null);
  const xtermInstance = useRef<Terminal | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);
  const socketRef = useRef<WebSocket | null>(null);

  const [connectionStatus, setConnectionStatus] = useState<
    'connecting' | 'connected' | 'disconnected'
  >('connecting');
  const [copied, setCopied] = useState(false);
  const [dimensions, setDimensions] = useState<{ cols: number; rows: number }>({ cols: 80, rows: 24 });

  const connectWebSocket = () => {
    if (socketRef.current) {
      try {
        socketRef.current.close();
      } catch {}
      socketRef.current = null;
    }

    setConnectionStatus('connecting');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/api/terminal/ws`;

    const ws = new WebSocket(wsUrl);
    socketRef.current = ws;

    ws.onopen = () => {
      setConnectionStatus('connected');
      if (xtermInstance.current && fitAddonRef.current) {
        fitAddonRef.current.fit();
        const dims = fitAddonRef.current.proposeDimensions();
        if (dims) {
          setDimensions({ cols: dims.cols, rows: dims.rows });
          ws.send(JSON.stringify({ type: 'resize', cols: dims.cols, rows: dims.rows }));
        }
      }
    };

    ws.onmessage = (event) => {
      if (xtermInstance.current) {
        xtermInstance.current.write(event.data);
      }
    };

    ws.onclose = () => {
      setConnectionStatus('disconnected');
    };

    ws.onerror = () => {
      setConnectionStatus('disconnected');
    };
  };

  useEffect(() => {
    if (!terminalRef.current) return;

    // Initialize xterm
    const term = new Terminal({
      cursorBlink: true,
      cursorStyle: 'bar',
      fontSize: 13,
      lineHeight: 1.25,
      scrollback: 5000,
      fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, monospace",
      theme: {
        background: '#090d16',
        foreground: '#f8fafc',
        cursor: '#38bdf8',
        cursorAccent: '#090d16',
        selectionBackground: 'rgba(56, 189, 248, 0.25)',
        black: '#1e293b',
        red: '#f87171',
        green: '#4ade80',
        yellow: '#fbbf24',
        blue: '#60a5fa',
        magenta: '#c084fc',
        cyan: '#38bdf8',
        white: '#f1f5f9',
        brightBlack: '#64748b',
        brightRed: '#ef4444',
        brightGreen: '#22c55e',
        brightYellow: '#eab308',
        brightBlue: '#3b82f6',
        brightMagenta: '#a855f7',
        brightCyan: '#06b6d4',
        brightWhite: '#ffffff',
      },
      convertEol: true,
    });

    const fitAddon = new FitAddon();
    term.loadAddon(fitAddon);

    term.open(terminalRef.current);
    fitAddon.fit();

    xtermInstance.current = term;
    fitAddonRef.current = fitAddon;

    // Connect WebSocket
    connectWebSocket();

    // Stream user keystrokes into WebSocket
    const onDataDisposable = term.onData((data) => {
      if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
        socketRef.current.send(data);
      }
    });

    // Handle resize
    const handleResize = () => {
      if (fitAddonRef.current && term) {
        try {
          fitAddonRef.current.fit();
          const dims = fitAddonRef.current.proposeDimensions();
          if (dims) {
            setDimensions({ cols: dims.cols, rows: dims.rows });
            if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
              socketRef.current.send(
                JSON.stringify({ type: 'resize', cols: dims.cols, rows: dims.rows })
              );
            }
          }
        } catch {}
      }
    };

    const resizeObserver = new ResizeObserver(() => {
      handleResize();
    });
    resizeObserver.observe(terminalRef.current);
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      resizeObserver.disconnect();
      onDataDisposable.dispose();
      term.dispose();
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, []);

  const handleClear = () => {
    if (xtermInstance.current) {
      xtermInstance.current.clear();
    }
  };

  const handleCopySelection = () => {
    if (xtermInstance.current) {
      const selection = xtermInstance.current.getSelection();
      if (selection) {
        navigator.clipboard.writeText(selection);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      }
    }
  };

  return (
    <div className="flex-1 flex flex-col h-full min-h-0 bg-[#090d16] text-slate-100 overflow-hidden select-none">
      {/* Terminal Top Control Bar */}
      <div className="h-11 border-b border-slate-800 bg-[#0c121e] px-4 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <TerminalIcon className="w-4 h-4 text-sky-400" />
            <span className="font-semibold text-xs text-slate-200 tracking-wide">
              Developer Terminal
            </span>
          </div>

          <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-slate-800/80 border border-slate-700/60 text-[11px] font-mono text-slate-300">
            <GitBranch className="w-3 h-3 text-sky-400" />
            <span>git_repo/</span>
          </div>

          {/* Connection Status Pill */}
          <div className="flex items-center gap-1.5 text-[11px] font-medium">
            {connectionStatus === 'connected' && (
              <span className="flex items-center gap-1 text-emerald-400">
                <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                Live Shell
              </span>
            )}
            {connectionStatus === 'connecting' && (
              <span className="flex items-center gap-1 text-amber-400">
                <span className="w-2 h-2 rounded-full bg-amber-500 animate-ping" />
                Connecting...
              </span>
            )}
            {connectionStatus === 'disconnected' && (
              <span className="flex items-center gap-1 text-rose-400">
                <span className="w-2 h-2 rounded-full bg-rose-500" />
                Disconnected
              </span>
            )}
          </div>
        </div>

        {/* Action Controls */}
        <div className="flex items-center gap-2">
          <button
            onClick={handleCopySelection}
            title="Copy Selection"
            className="p-1.5 rounded hover:bg-slate-800 text-slate-400 hover:text-slate-200 transition-colors"
          >
            {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
          </button>

          <button
            onClick={handleClear}
            title="Clear Terminal Output"
            className="p-1.5 rounded hover:bg-slate-800 text-slate-400 hover:text-slate-200 transition-colors"
          >
            <Trash2 className="w-3.5 h-3.5" />
          </button>

          <button
            onClick={connectWebSocket}
            title="Reconnect Terminal"
            className="p-1.5 rounded hover:bg-slate-800 text-slate-400 hover:text-sky-400 transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Terminal Viewport Container */}
      <div className="flex-1 min-h-0 px-4 pt-3 pb-2 overflow-hidden bg-[#090d16] flex flex-col">
        <div className="flex-1 min-h-0 w-full overflow-hidden" ref={terminalRef} />
      </div>

      {/* Terminal Bottom Status Bar (Protects prompt from Windows taskbar & provides IDE feel) */}
      <div className="h-8 border-t border-slate-800/90 bg-[#0c121e] px-4 flex items-center justify-between text-[11px] text-slate-400 shrink-0 font-mono select-none">
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-1.5 text-slate-300">
            <span className="w-1.5 h-1.5 rounded-full bg-sky-400" />
            <span>PowerShell / bash</span>
          </span>
          <span className="text-slate-700">•</span>
          <span>UTF-8</span>
          <span className="text-slate-700">•</span>
          <span className="text-slate-500">Shortcuts: Ctrl+L (clear) • Ctrl+C (abort)</span>
        </div>
        <div className="flex items-center gap-3 text-slate-500 text-[10px]">
          <span>{dimensions.cols} × {dimensions.rows}</span>
          <span className="text-slate-700">•</span>
          <span>Obelisk Dev Terminal</span>
        </div>
      </div>
    </div>
  );
};

