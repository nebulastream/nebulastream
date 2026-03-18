use super::endpoint::HostAddr;
use std::collections::{HashMap, HashSet, VecDeque};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("cycle detected among workers: {participants:?}")]
pub struct CycleDetected {
    pub participants: Vec<HostAddr>,
}

#[derive(Debug)]
pub struct WorkerTopology {
    pub workers: Vec<super::Model>,
    pub edges: Vec<(HostAddr, HostAddr)>,
}

impl WorkerTopology {
    pub fn validate(&self) -> Result<Vec<HostAddr>, CycleDetected> {
        let mut in_degree: HashMap<&HostAddr, usize> = HashMap::new();
        let mut adjacency: HashMap<&HostAddr, Vec<&HostAddr>> = HashMap::new();

        for w in &self.workers {
            in_degree.entry(&w.host_addr).or_insert(0);
            adjacency.entry(&w.host_addr).or_default();
        }

        for (src, dst) in &self.edges {
            adjacency.entry(src).or_default().push(dst);
            *in_degree.entry(dst).or_insert(0) += 1;
        }

        let mut queue: VecDeque<&HostAddr> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&addr, _)| addr)
            .collect();

        let mut order = Vec::with_capacity(self.workers.len());

        while let Some(node) = queue.pop_front() {
            order.push(node.clone());
            if let Some(neighbors) = adjacency.get(node) {
                for &neighbor in neighbors {
                    let deg = in_degree.get_mut(neighbor).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        if order.len() == self.workers.len() {
            Ok(order)
        } else {
            let ordered: HashSet<_> = order.iter().collect();
            let participants = in_degree
                .keys()
                .filter(|addr| !ordered.contains(*addr))
                .map(|&addr| addr.clone())
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

    #[cfg(feature = "testing")]
    proptest! {
        #[test]
        fn empty_or_single_node_is_valid(n in 0..=1usize) {
            let workers = make_workers(n);
            let topo = WorkerTopology { workers, edges: vec![] };
            let order = topo.validate().unwrap();
            prop_assert_eq!(order.len(), n);
        }

        #[test]
        fn any_forward_edges_form_valid_dag(
            n in 2..=8usize,
            seed in prop::collection::vec(any::<bool>(), 0..=28),
        ) {
            let workers = make_workers(n);
            let flags: Vec<bool> = seed.into_iter().chain(std::iter::repeat(false)).take(max_edges(n)).collect();
            let edges = dag_edges(&workers, &flags);
            let topo = WorkerTopology { workers, edges };
            let order = topo.validate().unwrap();
            prop_assert_eq!(order.len(), n);

            let pos: HashMap<_, _> = order.iter().enumerate().map(|(i, a)| (a, i)).collect();
            for (src, tgt) in &topo.edges {
                prop_assert!(pos[src] < pos[tgt]);
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
            edges.push((workers[n - 1].host_addr.clone(), workers[0].host_addr.clone()));
            let has_path_first_to_last = {
                let adj: HashMap<_, Vec<_>> = {
                    let mut m: HashMap<&HostAddr, Vec<&HostAddr>> = HashMap::new();
                    for (s, t) in &edges[..edges.len() - 1] {
                        m.entry(s).or_default().push(t);
                    }
                    m
                };
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
            let topo = WorkerTopology { workers, edges };
            if has_path_first_to_last {
                let err = topo.validate().unwrap_err();
                prop_assert!(!err.participants.is_empty());
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
            let topo = WorkerTopology { workers, edges: vec![] };
            let order = topo.validate().unwrap();
            prop_assert_eq!(order.len(), n1 + n2);
        }
    }
}
