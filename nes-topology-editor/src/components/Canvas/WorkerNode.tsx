import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { AlertTriangle } from 'lucide-react';
import type { Worker } from '../../lib/types';
import type { ValidationError } from '../../lib/validation';

interface WorkerNodeData {
  worker: Worker;
  validationErrors?: ValidationError[];
  [key: string]: unknown;
}

const WorkerNode: React.FC<NodeProps> = ({ data }) => {
  const { worker, validationErrors } = data as unknown as WorkerNodeData;
  const hasErrors = validationErrors && validationErrors.length > 0;
  const hasError = hasErrors && validationErrors.some((e) => e.severity === 'error');
  const iconColor = hasError ? '#ef4444' : '#f59e0b';
  const tooltipText = hasErrors ? validationErrors.map((e) => e.message).join('\n') : '';

  return (
    <div className="worker-node" style={{ position: 'relative' }}>
      {hasErrors && (
        <div className="validation-badge" style={{ '--badge-color': iconColor } as React.CSSProperties}>
          <AlertTriangle size={14} color={iconColor} fill={iconColor} fillOpacity={0.2} />
          <span className="validation-tooltip">{tooltipText}</span>
        </div>
      )}
      <div className="worker-node__label">{worker.host}</div>
      <Handle type="target" position={Position.Top} id="in" />
      <Handle type="source" position={Position.Bottom} id="out" />
      <Handle type="target" position={Position.Left} id="sources" />
      <Handle type="source" position={Position.Right} id="sinks" />
    </div>
  );
};

export default React.memo(WorkerNode);
