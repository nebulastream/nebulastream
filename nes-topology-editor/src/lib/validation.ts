import type { Node, Edge } from '@xyflow/react';
import type { Worker, LogicalSource, PhysicalSource, Sink } from './types';
import { SOURCE_CONFIGS, SINK_CONFIGS } from './sourceConfigs';

export interface ValidationError {
  nodeId: string;
  severity: 'warning' | 'error';
  message: string;
}

/**
 * Validates the entire topology and returns an array of validation errors.
 * Checks: orphan nodes, missing logical sources, empty schemas, missing required config.
 * All issues are informational -- none block YAML export.
 */
export function validateTopology(
  _workers: Worker[],
  logicalSources: LogicalSource[],
  physicalSources: PhysicalSource[],
  sinks: Sink[],
): ValidationError[] {
  const errors: ValidationError[] = [];
  const logicalSourceMap = new Map(logicalSources.map((ls) => [ls.id, ls]));

  for (const ps of physicalSources) {
    // Orphan source (no worker assigned)
    if (!ps.hostWorkerId) {
      errors.push({
        nodeId: ps.id,
        severity: 'warning',
        message: 'Source not assigned to a worker',
      });
    }

    // No logical source linked
    if (!ps.logicalSourceId) {
      errors.push({
        nodeId: ps.id,
        severity: 'error',
        message: 'No logical source linked',
      });
    } else {
      const ls = logicalSourceMap.get(ps.logicalSourceId);
      if (!ls) {
        // References non-existent logical source
        errors.push({
          nodeId: ps.id,
          severity: 'error',
          message: `Logical source "${ps.logicalSourceId}" not found`,
        });
      } else if (ls.schema.length === 0) {
        // Logical source has empty schema
        errors.push({
          nodeId: ps.id,
          severity: 'warning',
          message: `Logical source "${ls.name}" has empty schema`,
        });
      }
    }

    // Missing required config fields
    const configDefs = SOURCE_CONFIGS[ps.type];
    if (configDefs) {
      for (const field of configDefs) {
        if (field.required && !ps.sourceConfig[field.key]) {
          errors.push({
            nodeId: ps.id,
            severity: 'warning',
            message: `Missing required config field: ${field.label}`,
          });
        }
      }
    }
  }

  for (const sink of sinks) {
    // Missing name
    if (!sink.name || !sink.name.trim()) {
      errors.push({
        nodeId: sink.id,
        severity: 'warning',
        message: 'Sink has no name',
      });
    }

    // Orphan sink (no worker assigned)
    if (!sink.hostWorkerId) {
      errors.push({
        nodeId: sink.id,
        severity: 'warning',
        message: 'Sink not assigned to a worker',
      });
    }

    // Missing required config fields
    const configDefs = SINK_CONFIGS[sink.type];
    if (configDefs) {
      for (const field of configDefs) {
        if (field.required && !sink.config[field.key]) {
          errors.push({
            nodeId: sink.id,
            severity: 'warning',
            message: `Missing required config field: ${field.label}`,
          });
        }
      }
    }
  }

  return errors;
}

/**
 * Checks if a connection between two node types is valid.
 * Valid: worker->worker (downstream), source->worker, worker->sink
 * Invalid: all other combinations
 */
export function isValidConnectionType(
  sourceType: string,
  targetType: string,
): boolean {
  if (targetType === 'worker') {
    return sourceType === 'worker' || sourceType === 'source';
  }
  if (targetType === 'sink') {
    return sourceType === 'worker';
  }
  return false;
}

/**
 * Checks if adding an edge from source to target would create a cycle.
 * Uses BFS from the proposed target to see if it can reach the proposed source.
 */
export function wouldCreateCycle(
  _nodes: Node[],
  edges: Edge[],
  proposed: { source: string; target: string },
): boolean {
  // Self-loop
  if (proposed.source === proposed.target) return true;

  // BFS from proposed.target following existing edges to see if we reach proposed.source
  const adjacency = new Map<string, string[]>();
  for (const edge of edges) {
    const list = adjacency.get(edge.source) ?? [];
    list.push(edge.target);
    adjacency.set(edge.source, list);
  }

  const visited = new Set<string>();
  const queue = [proposed.target];

  while (queue.length > 0) {
    const current = queue.shift()!;
    if (current === proposed.source) return true;
    if (visited.has(current)) continue;
    visited.add(current);

    const neighbors = adjacency.get(current) ?? [];
    for (const neighbor of neighbors) {
      if (!visited.has(neighbor)) {
        queue.push(neighbor);
      }
    }
  }

  return false;
}
