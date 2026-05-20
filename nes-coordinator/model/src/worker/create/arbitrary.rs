/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

use super::CreateWorker;
use crate::worker::endpoint::{self, NetworkAddr};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use std::collections::HashSet;

impl Arbitrary for CreateWorker {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<NetworkAddr>(),
            1024..65535u16,
            prop_oneof![Just(None), (0..=1024i32).prop_map(Some)],
        )
            .prop_map(|(host_addr, data_port, max_operators)| {
                // Generated ports are at most 65534 (range 1024..65535), so
                // +1 is always a valid, distinct u16.
                let data_port = if data_port == host_addr.port {
                    data_port + 1
                } else {
                    data_port
                };
                let data_addr = NetworkAddr::new(host_addr.host.clone(), data_port)
                    .expect("proptest inputs are non-empty / non-zero");
                CreateWorker {
                    host_addr,
                    data_addr,
                    max_operators,
                    peers: vec![],
                    config: Default::default(),
                    if_not_exists: false,
                }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

/// Convert a flat edge index into an (i, j) pair where i < j,
/// enumerating edges in the same order as nested loops: (0,1), (0,2), ..., (1,2), (1,3), ...
fn flat_index_to_edge(n: usize, idx: usize) -> (usize, usize) {
    let mut remaining = idx;
    for i in 0..n {
        let row_len = n - i - 1;
        if remaining < row_len {
            return (i, i + 1 + remaining);
        }
        remaining -= row_len;
    }
    unreachable!()
}

pub(crate) fn gen_unique_workers(n: usize) -> BoxedStrategy<Vec<CreateWorker>> {
    vec(prop_oneof![Just(None), (0..=1024i32).prop_map(Some)], n)
        .prop_map(|capacities| {
            capacities
                .into_iter()
                .enumerate()
                .map(|(i, max_operators)| {
                    let octet = (i + 2) as u8; // start at .2; .1 is reserved for the coordinator
                    let ip = format!("192.168.1.{octet}");
                    CreateWorker {
                        host_addr: NetworkAddr::new(&ip, endpoint::DEFAULT_HOST_PORT)
                            .expect("synthetic test address"),
                        data_addr: NetworkAddr::new(&ip, endpoint::DEFAULT_DATA_PORT)
                            .expect("synthetic test address"),
                        max_operators,
                        peers: vec![],
                        config: Default::default(),
                        if_not_exists: false,
                    }
                })
                .collect()
        })
        .boxed()
}

impl CreateWorker {
    pub(crate) fn topology_dag(min_nodes: u8, max_nodes: u8) -> BoxedStrategy<Vec<CreateWorker>> {
        (min_nodes as usize..=max_nodes as usize)
            .prop_flat_map(|n| {
                let max_edges = n * (n - 1) / 2;
                let num_edges = 0..=max_edges.min(2 * n);
                (gen_unique_workers(n), num_edges)
            })
            .prop_flat_map(|(workers, num_edges)| {
                let n = workers.len();
                let max_edges = n * (n - 1) / 2;
                (Just(workers), vec(0..max_edges, num_edges))
            })
            .prop_map(|(mut workers, edge_indices)| {
                let n = workers.len();
                let mut seen = HashSet::new();
                for idx in edge_indices {
                    if seen.insert(idx) {
                        let (i, j) = flat_index_to_edge(n, idx);
                        let addr = workers[j].host_addr.clone();
                        workers[i].peers.push(addr);
                    }
                }
                workers.into_iter().rev().collect()
            })
            .boxed()
    }
}
