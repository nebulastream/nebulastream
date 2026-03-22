use super::endpoint::HostAddr;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("cycle detected among workers: {participants:?}")]
pub struct CycleDetected {
    pub participants: Vec<HostAddr>,
}

#[derive(Debug)]
pub struct Node {
    pub worker: super::Model,
    pub upstream: HashSet<HostAddr>,
    pub downstream: HashSet<HostAddr>,
}

#[derive(Debug)]
pub struct TopologyGraph {
    nodes: HashMap<HostAddr, Node>,
}

impl TopologyGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Build a graph from DB data. Returns an error if the edges contain a cycle.
    pub fn from_parts(
        workers: Vec<super::Model>,
        edges: Vec<(HostAddr, HostAddr)>,
    ) -> Result<Self, CycleDetected> {
        let mut graph = Self {
            nodes: workers
                .into_iter()
                .map(|w| {
                    let addr = w.host_addr.clone();
                    (
                        addr,
                        Node {
                            worker: w,
                            upstream: HashSet::new(),
                            downstream: HashSet::new(),
                        },
                    )
                })
                .collect(),
        };

        for (src, dst) in edges {
            if let Some(src_node) = graph.nodes.get_mut(&src) {
                src_node.downstream.insert(dst.clone());
            }
            if let Some(dst_node) = graph.nodes.get_mut(&dst) {
                dst_node.upstream.insert(src);
            }
        }

        graph.validate()?;
        Ok(graph)
    }

    /// Insert a node with downstream edges. Rejects if the edges would create a cycle.
    pub fn add_node(
        &mut self,
        worker: super::Model,
        downstream: &[HostAddr],
    ) -> Result<(), CycleDetected> {
        let addr = worker.host_addr.clone();

        // Check: would any downstream target create a cycle?
        // A cycle exists if any downstream node can already reach `addr` via existing edges.
        for dst in downstream {
            if *dst == addr {
                return Err(CycleDetected {
                    participants: vec![addr],
                });
            }
            if self.can_reach(dst, &addr) {
                return Err(CycleDetected {
                    participants: vec![addr, dst.clone()],
                });
            }
        }

        // Safe to insert
        let downstream_set: HashSet<HostAddr> = downstream.iter().cloned().collect();
        for dst in &downstream_set {
            if let Some(dst_node) = self.nodes.get_mut(dst) {
                dst_node.upstream.insert(addr.clone());
            }
        }

        self.nodes.insert(
            addr,
            Node {
                worker,
                upstream: HashSet::new(),
                downstream: downstream_set,
            },
        );

        Ok(())
    }

    /// Remove a node and all its edges (both directions).
    pub fn remove_node(&mut self, host_addr: &HostAddr) {
        if let Some(node) = self.nodes.remove(host_addr) {
            for upstream in &node.upstream {
                if let Some(u) = self.nodes.get_mut(upstream) {
                    u.downstream.remove(host_addr);
                }
            }
            for downstream in &node.downstream {
                if let Some(d) = self.nodes.get_mut(downstream) {
                    d.upstream.remove(host_addr);
                }
            }
        }
    }

    pub fn contains(&self, host_addr: &HostAddr) -> bool {
        self.nodes.contains_key(host_addr)
    }

    pub fn get_node(&self, host_addr: &HostAddr) -> Option<&Node> {
        self.nodes.get(host_addr)
    }

    pub fn workers(&self) -> impl Iterator<Item = &super::Model> {
        self.nodes.values().map(|n| &n.worker)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Find all paths from `src` to `dst` via downstream edges (DFS).
    pub fn find_paths(&self, src: &HostAddr, dst: &HostAddr) -> Vec<Vec<HostAddr>> {
        let mut results = Vec::new();
        let mut path = vec![src.clone()];
        self.dfs_paths(src, dst, &mut path, &mut results);
        results
    }

    fn dfs_paths(
        &self,
        current: &HostAddr,
        dst: &HostAddr,
        path: &mut Vec<HostAddr>,
        results: &mut Vec<Vec<HostAddr>>,
    ) {
        if current == dst && path.len() > 1 {
            results.push(path.clone());
            return;
        }
        if let Some(node) = self.nodes.get(current) {
            for next in &node.downstream {
                if !path.contains(next) {
                    path.push(next.clone());
                    self.dfs_paths(next, dst, path, results);
                    path.pop();
                }
            }
        }
    }

    /// Check if `from` can reach `to` via downstream edges.
    fn can_reach(&self, from: &HostAddr, to: &HostAddr) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![from];
        while let Some(current) = stack.pop() {
            if current == to {
                return true;
            }
            if !visited.insert(current) {
                continue;
            }
            if let Some(node) = self.nodes.get(current) {
                stack.extend(node.downstream.iter());
            }
        }
        false
    }

    /// Validate that the graph has no cycles (Kahn's algorithm).
    fn validate(&self) -> Result<(), CycleDetected> {
        let mut in_degree: HashMap<&HostAddr, usize> = self
            .nodes
            .keys()
            .map(|addr| (addr, 0))
            .collect();

        for node in self.nodes.values() {
            for dst in &node.downstream {
                if let Some(deg) = in_degree.get_mut(dst) {
                    *deg += 1;
                }
            }
        }

        let mut queue: std::collections::VecDeque<&HostAddr> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&addr, _)| addr)
            .collect();

        let mut sorted_count = 0;

        while let Some(node_addr) = queue.pop_front() {
            sorted_count += 1;
            if let Some(node) = self.nodes.get(node_addr) {
                for dst in &node.downstream {
                    if let Some(deg) = in_degree.get_mut(dst) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dst);
                        }
                    }
                }
            }
        }

        if sorted_count == self.nodes.len() {
            Ok(())
        } else {
            let participants = in_degree
                .into_iter()
                .filter(|(_, deg)| *deg > 0)
                .map(|(addr, _)| addr.clone())
                .collect();
            Err(CycleDetected { participants })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{DesiredWorkerState, WorkerState, endpoint::NetworkAddr};

    #[cfg(feature = "testing")]
    use proptest::prelude::*;

    fn make_worker(name: &str) -> super::super::Model {
        super::super::Model {
            host_addr: NetworkAddr::new(name, 1000),
            grpc_addr: NetworkAddr::new(name, 2000),
            capacity: 10,
            current_state: WorkerState::Active,
            desired_state: DesiredWorkerState::Active,
        }
    }

    fn make_workers(n: usize) -> Vec<super::super::Model> {
        (0..n).map(|i| make_worker(&format!("w{i}"))).collect()
    }

    fn dag_edges(
        workers: &[super::super::Model],
        edge_flags: &[bool],
    ) -> Vec<(HostAddr, HostAddr)> {
        let mut edges = Vec::new();
        let mut idx = 0;
        for i in 0..workers.len() {
            for j in (i + 1)..workers.len() {
                if idx < edge_flags.len() && edge_flags[idx] {
                    edges.push((workers[i].host_addr.clone(), workers[j].host_addr.clone()));
                }
                idx += 1;
            }
        }
        edges
    }

    fn max_edges(n: usize) -> usize {
        n * n.saturating_sub(1) / 2
    }

    #[test]
    fn empty_graph() {
        let graph = TopologyGraph::new();
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
    }

    #[test]
    fn add_and_get_node() {
        let mut graph = TopologyGraph::new();
        let w = make_worker("a");
        let addr = w.host_addr.clone();
        graph.add_node(w, &[]).unwrap();

        assert!(graph.contains(&addr));
        assert_eq!(graph.len(), 1);
        let node = graph.get_node(&addr).unwrap();
        assert!(node.upstream.is_empty());
        assert!(node.downstream.is_empty());
    }

    #[test]
    fn add_with_downstream() {
        let mut graph = TopologyGraph::new();
        let a = make_worker("a");
        let b = make_worker("b");
        let addr_a = a.host_addr.clone();
        let addr_b = b.host_addr.clone();

        graph.add_node(b, &[]).unwrap();
        graph.add_node(a, &[addr_b.clone()]).unwrap();

        let node_a = graph.get_node(&addr_a).unwrap();
        assert!(node_a.downstream.contains(&addr_b));
        assert!(node_a.upstream.is_empty());

        let node_b = graph.get_node(&addr_b).unwrap();
        assert!(node_b.upstream.contains(&addr_a));
        assert!(node_b.downstream.is_empty());
    }

    #[test]
    fn self_edge_rejected() {
        let mut graph = TopologyGraph::new();
        let w = make_worker("a");
        let addr = w.host_addr.clone();
        let err = graph.add_node(w, &[addr]).unwrap_err();
        assert!(!err.participants.is_empty());
    }

    #[test]
    fn add_node_no_false_cycle() {
        // After removing a node and re-adding with different edges, no spurious cycle
        let mut graph = TopologyGraph::new();
        let a = make_worker("a");
        let b = make_worker("b");
        let c = make_worker("c");
        let addr_a = a.host_addr.clone();
        let addr_b = b.host_addr.clone();
        let addr_c = c.host_addr.clone();

        graph.add_node(c, &[]).unwrap();
        graph.add_node(b, &[addr_c.clone()]).unwrap();
        graph.add_node(a, &[addr_b.clone()]).unwrap();
        // a→b→c. Remove c, re-add with c→a: gives c→a→b (valid DAG, b→c edge was cleaned up)
        graph.remove_node(&addr_c);
        let c2 = make_worker("c");
        graph.add_node(c2, &[addr_a.clone()]).unwrap();
        assert_eq!(graph.len(), 3);
    }

    #[test]
    fn remove_cleans_edges() {
        let mut graph = TopologyGraph::new();
        let a = make_worker("a");
        let b = make_worker("b");
        let c = make_worker("c");
        let addr_a = a.host_addr.clone();
        let addr_b = b.host_addr.clone();
        let addr_c = c.host_addr.clone();

        graph.add_node(c, &[]).unwrap();
        graph.add_node(b, &[addr_c.clone()]).unwrap();
        graph.add_node(a, &[addr_b.clone()]).unwrap();

        graph.remove_node(&addr_b);

        assert!(!graph.contains(&addr_b));
        let node_a = graph.get_node(&addr_a).unwrap();
        assert!(node_a.downstream.is_empty());
        let node_c = graph.get_node(&addr_c).unwrap();
        assert!(node_c.upstream.is_empty());
    }

    #[test]
    fn find_paths_diamond() {
        // a → b → d
        // a → c → d
        let mut graph = TopologyGraph::new();
        let a = make_worker("a");
        let b = make_worker("b");
        let c = make_worker("c");
        let d = make_worker("d");
        let addr_a = a.host_addr.clone();
        let addr_b = b.host_addr.clone();
        let addr_c = c.host_addr.clone();
        let addr_d = d.host_addr.clone();

        graph.add_node(d, &[]).unwrap();
        graph.add_node(b, &[addr_d.clone()]).unwrap();
        graph.add_node(c, &[addr_d.clone()]).unwrap();
        graph.add_node(a, &[addr_b.clone(), addr_c.clone()]).unwrap();

        let paths = graph.find_paths(&addr_a, &addr_d);
        assert_eq!(paths.len(), 2);
        for path in &paths {
            assert_eq!(path.first().unwrap(), &addr_a);
            assert_eq!(path.last().unwrap(), &addr_d);
        }
    }

    #[test]
    fn find_paths_no_path() {
        let mut graph = TopologyGraph::new();
        let a = make_worker("a");
        let b = make_worker("b");
        let addr_a = a.host_addr.clone();
        let addr_b = b.host_addr.clone();
        graph.add_node(a, &[]).unwrap();
        graph.add_node(b, &[]).unwrap();

        let paths = graph.find_paths(&addr_a, &addr_b);
        assert!(paths.is_empty());
    }

    #[test]
    fn from_parts_valid() {
        let workers = make_workers(3);
        let edges = vec![
            (workers[0].host_addr.clone(), workers[1].host_addr.clone()),
            (workers[1].host_addr.clone(), workers[2].host_addr.clone()),
        ];
        let graph = TopologyGraph::from_parts(workers, edges).unwrap();
        assert_eq!(graph.len(), 3);
    }

    #[test]
    fn from_parts_cycle_rejected() {
        let workers = make_workers(2);
        let edges = vec![
            (workers[0].host_addr.clone(), workers[1].host_addr.clone()),
            (workers[1].host_addr.clone(), workers[0].host_addr.clone()),
        ];
        assert!(TopologyGraph::from_parts(workers, edges).is_err());
    }

    #[cfg(feature = "testing")]
    proptest! {
        #[test]
        fn empty_or_single_node_is_valid(n in 0..=1usize) {
            let workers = make_workers(n);
            let graph = TopologyGraph::from_parts(workers, vec![]).unwrap();
            prop_assert_eq!(graph.len(), n);
        }

        #[test]
        fn any_forward_edges_form_valid_dag(
            n in 2..=8usize,
            seed in prop::collection::vec(any::<bool>(), 0..=28),
        ) {
            let workers = make_workers(n);
            let flags: Vec<bool> = seed.into_iter().chain(std::iter::repeat(false)).take(max_edges(n)).collect();
            let edges = dag_edges(&workers, &flags);
            let graph = TopologyGraph::from_parts(workers, edges).unwrap();
            prop_assert_eq!(graph.len(), n);

            // Verify all edges are reflected in upstream/downstream
            for node in graph.nodes.values() {
                for dst in &node.downstream {
                    let dst_node = graph.get_node(dst).unwrap();
                    prop_assert!(dst_node.upstream.contains(&node.worker.host_addr));
                }
                for src in &node.upstream {
                    let src_node = graph.get_node(src).unwrap();
                    prop_assert!(src_node.downstream.contains(&node.worker.host_addr));
                }
            }
        }

        #[test]
        fn back_edge_creates_cycle(
            n in 2..=8usize,
            seed in prop::collection::vec(any::<bool>(), 0..=28),
        ) {
            let workers = make_workers(n);
            let flags: Vec<bool> = seed.into_iter().chain(std::iter::repeat(false)).take(max_edges(n)).collect();
            let mut edges = dag_edges(&workers, &flags);
            // Add a back edge from last to first
            edges.push((workers[n - 1].host_addr.clone(), workers[0].host_addr.clone()));

            // Check if there's a forward path from first to last (which would make the back edge a cycle)
            let forward_edges = &edges[..edges.len() - 1];
            let has_path = {
                let mut adj: HashMap<&HostAddr, Vec<&HostAddr>> = HashMap::new();
                for (s, t) in forward_edges {
                    adj.entry(s).or_default().push(t);
                }
                let mut visited = HashSet::new();
                let mut stack = vec![&workers[0].host_addr];
                while let Some(node) = stack.pop() {
                    if !visited.insert(node) { continue; }
                    if let Some(neighbors) = adj.get(node) {
                        stack.extend(neighbors.iter().copied());
                    }
                }
                visited.contains(&workers[n - 1].host_addr)
            };

            if has_path {
                prop_assert!(TopologyGraph::from_parts(workers, edges).is_err());
            }
        }

        #[test]
        fn add_node_then_query_neighbors(
            n in 2..=6usize,
            seed in prop::collection::vec(any::<bool>(), 0..=15),
        ) {
            let workers = make_workers(n);
            let mut graph = TopologyGraph::new();

            // Add workers in order, each with forward edges only (no cycles possible)
            for (i, w) in workers.iter().enumerate() {
                let downstream: Vec<HostAddr> = (0..i)
                    .filter(|&j| {
                        let idx = i * (i - 1) / 2 + j;
                        idx < seed.len() && seed[idx]
                    })
                    .map(|j| workers[j].host_addr.clone())
                    .collect();
                graph.add_node(w.clone(), &downstream).unwrap();
            }

            prop_assert_eq!(graph.len(), n);

            // Verify bidirectional consistency
            for node in graph.nodes.values() {
                for dst in &node.downstream {
                    let dst_node = graph.get_node(dst).unwrap();
                    prop_assert!(dst_node.upstream.contains(&node.worker.host_addr));
                }
            }
        }

        #[test]
        fn disconnected_components_valid(
            n1 in 1..=4usize,
            n2 in 1..=4usize,
        ) {
            let mut workers = make_workers(n1 + n2);
            for w in workers.iter_mut().skip(n1) {
                w.host_addr = NetworkAddr::new(format!("x{}", w.host_addr.host), w.host_addr.port);
                w.grpc_addr = NetworkAddr::new(format!("x{}", w.grpc_addr.host), w.grpc_addr.port);
            }
            let graph = TopologyGraph::from_parts(workers, vec![]).unwrap();
            prop_assert_eq!(graph.len(), n1 + n2);
        }
    }
}
