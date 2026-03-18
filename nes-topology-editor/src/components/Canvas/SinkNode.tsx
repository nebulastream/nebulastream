import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { FileOutput, Circle, Printer, AlertTriangle } from 'lucide-react';
import type { Sink } from '../../lib/types';
import type { ValidationError } from '../../lib/validation';

interface SinkNodeData {
  sink: Sink;
  validationErrors?: ValidationError[];
  [key: string]: unknown;
}

const iconMap: Record<string, React.FC<{ size?: number }>> = {
  File: FileOutput,
  Void: Circle,
  Print: Printer,
};

const SinkNode: React.FC<NodeProps> = ({ data }) => {
  const { sink, validationErrors } = data as unknown as SinkNodeData;
  const Icon = iconMap[sink.type] ?? Circle;
  const hasErrors = validationErrors && validationErrors.length > 0;
  const hasError = hasErrors && validationErrors.some((e) => e.severity === 'error');
  const iconColor = hasError ? '#ef4444' : '#f59e0b';
  const tooltipText = hasErrors ? validationErrors.map((e) => e.message).join('\n') : '';

  return (
    <div className="sink-node" style={{ position: 'relative' }}>
      {hasErrors && (
        <div className="validation-badge" style={{ '--badge-color': iconColor } as React.CSSProperties}>
          <AlertTriangle size={14} color={iconColor} fill={iconColor} fillOpacity={0.2} />
          <span className="validation-tooltip">{tooltipText}</span>
        </div>
      )}
      <Icon size={16} />
      <span className="sink-node__label">{sink.type}</span>
      <Handle type="target" position={Position.Left} id="attach" />
    </div>
  );
};

export default React.memo(SinkNode);
