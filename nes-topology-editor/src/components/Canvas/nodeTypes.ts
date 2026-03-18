import type { NodeTypes } from '@xyflow/react';
import WorkerNode from './WorkerNode';
import SourceNode from './SourceNode';
import SinkNode from './SinkNode';

export const nodeTypes: NodeTypes = {
  worker: WorkerNode,
  source: SourceNode,
  sink: SinkNode,
};
