import React from 'react';
import { CheckCircle2, AlertTriangle, Loader2, XCircle } from 'lucide-react';
import { useStore } from '../../store';

/**
 * Bottom status bar showing WASM validation state.
 *
 * Positioned at the bottom-left of the canvas area.
 * Shows: loading spinner, validating spinner, green check (valid),
 * amber warning (errors with tooltip), or red X (unavailable).
 */
const StatusBar: React.FC = () => {
  const wasmStatus = useStore((s) => s.wasmStatus);
  const semanticError = useStore((s) => s.semanticError);
  const wasmError = useStore((s) => s.wasmError);
  const darkMode = useStore((s) => s.darkMode);

  const bg = darkMode
    ? 'rgba(17, 24, 39, 0.85)'
    : 'rgba(248, 250, 252, 0.9)';
  const textColor = darkMode ? '#d1d5db' : '#64748b';

  const containerStyle: React.CSSProperties = {
    position: 'absolute',
    bottom: 8,
    left: 8,
    zIndex: 10,
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '4px 10px',
    borderRadius: 6,
    background: bg,
    border: darkMode ? '1px solid #374151' : '1px solid #e2e8f0',
    fontSize: 12,
    color: textColor,
    pointerEvents: 'auto',
    userSelect: 'none',
    backdropFilter: 'blur(4px)',
  };

  if (wasmStatus === 'loading') {
    return (
      <div style={containerStyle} data-testid="status-bar" data-status="loading">
        <Loader2 size={14} style={{ animation: 'spin 1s linear infinite' }} />
        <span>Loading validator...</span>
      </div>
    );
  }

  if (wasmStatus === 'validating') {
    return (
      <div style={containerStyle} data-testid="status-bar" data-status="validating">
        <Loader2 size={14} style={{ animation: 'spin 1s linear infinite' }} />
        <span>Validating...</span>
      </div>
    );
  }

  if (wasmStatus === 'error') {
    return (
      <div style={containerStyle} data-testid="status-bar" data-status="error" title={wasmError ?? 'Validator failed to load'}>
        <XCircle size={14} color="#ef4444" />
        <span>Validator unavailable</span>
      </div>
    );
  }

  // wasmStatus === 'ready'
  if (semanticError === null) {
    // Not yet validated
    return (
      <div style={containerStyle} data-testid="status-bar" data-status="ready">
        <CheckCircle2 size={14} color="#22c55e" />
      </div>
    );
  }

  if (semanticError === '') {
    return (
      <div style={containerStyle} data-testid="status-bar" data-status="valid">
        <CheckCircle2 size={14} color="#22c55e" />
        <span>Valid</span>
      </div>
    );
  }

  // Non-empty semantic error
  return (
    <div style={containerStyle} data-testid="status-bar" data-status="invalid" title={semanticError}>
      <AlertTriangle size={14} color="#f59e0b" />
      <span style={{ color: '#f59e0b' }}>Validation error</span>
    </div>
  );
};

export default StatusBar;
