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
use proptest::collection::{SizeRange, vec};
use proptest::option;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

impl Arbitrary for CreateWorker {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<NetworkAddr>(),
            1024..65535u16,
            option::of(0..=1024i32),
        )
            .prop_map(|(host_addr, data_port, max_operators)| {
                // Generated ports are at most 65534 (range 1024..65535),
                // so +1 is always a valid, distinct u16.
                let data_port = if data_port == host_addr.port() {
                    data_port + 1
                } else {
                    data_port
                };
                let data_addr = NetworkAddr::new(host_addr.host(), data_port)
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

impl CreateWorker {
    pub(crate) fn topology_dag(nodes: impl Into<SizeRange>) -> BoxedStrategy<Vec<CreateWorker>> {
        vec((option::of(0..=1024i32), any::<u32>()), nodes)
            .prop_map(|rows| {
                // start at .2; .1 is reserved for the coordinator
                let at = |i: usize, port| {
                    NetworkAddr::new(format!("192.168.1.{}", i + 2), port)
                        .expect("synthetic test address")
                };
                let workers = rows.iter().enumerate();
                workers
                    .map(|(i, &(max_operators, links))| CreateWorker {
                        host_addr: at(i, endpoint::DEFAULT_HOST_PORT),
                        data_addr: at(i, endpoint::DEFAULT_DATA_PORT),
                        max_operators,
                        peers: (i + 1..rows.len())
                            .filter(|j| links >> j & 1 == 1)
                            .map(|j| at(j, endpoint::DEFAULT_HOST_PORT))
                            .collect(),
                        config: Default::default(),
                        if_not_exists: false,
                    })
                    .collect()
            })
            .boxed()
    }
}
