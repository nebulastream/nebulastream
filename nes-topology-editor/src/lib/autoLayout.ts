import { useStore } from '../store';
import { selectReactFlowNodes, selectReactFlowEdges } from '../store/selectors';
import { getLayoutedElements } from './layout';

/**
 * Read current topology from store, compute dagre layout, write positions back.
 * This is a standalone function (no hooks) -- safe to call from event handlers.
 */
export function applyAutoLayout(): void {
  const state = useStore.getState();
  const nodes = selectReactFlowNodes(state);
  const edges = selectReactFlowEdges(state);
  if (nodes.length === 0) return;
  const { nodes: layouted } = getLayoutedElements(nodes, edges, 'LR');
  state.setAllNodePositions(layouted.map((n) => ({ id: n.id, position: n.position })));
}
