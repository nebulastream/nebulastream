import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { Database, FileText, AlertTriangle } from 'lucide-react';
import type { PhysicalSource } from '../../lib/types';
import type { ValidationError } from '../../lib/validation';

interface SourceNodeData {
  source: PhysicalSource;
  color: string;
  validationErrors?: ValidationError[];
  [key: string]: unknown;
}

const iconMap: Record<string, React.FC<{ size?: number }>> = {
  Generator: Database,
  CSV: FileText,
};

const SourceNode: React.FC<NodeProps> = ({ data }) => {
  const { source, color, validationErrors } = data as unknown as SourceNodeData;
  const Icon = iconMap[source.type] ?? Database;
  const hasErrors = validationErrors && validationErrors.length > 0;
  const hasError = hasErrors && validationErrors.some((e) => e.severity === 'error');
  const iconColor = hasError ? '#ef4444' : '#f59e0b';
  const tooltipText = hasErrors ? validationErrors.map((e) => e.message).join('\n') : '';

  return (
    <div
      className="source-node"
      style={{
        borderColor: color,
        backgroundColor: `${color}18`,
        position: 'relative',
      }}
    >
      {hasErrors && (
        <div className="validation-badge" style={{ '--badge-color': iconColor } as React.CSSProperties}>
          <AlertTriangle size={14} color={iconColor} fill={iconColor} fillOpacity={0.2} />
          <span className="validation-tooltip">{tooltipText}</span>
        </div>
      )}
      <Icon size={16} />
      <span className="source-node__label">{source.type}</span>
      <Handle type="source" position={Position.Right} id="attach" />
    </div>
  );
};

export default React.memo(SourceNode);
