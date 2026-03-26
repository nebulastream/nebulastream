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

pub mod endpoint;
pub mod network_link;

use std::collections::HashSet;
use crate::source::physical_source;
use crate::{IntoCondition, query, sink};
use endpoint::NetworkAddr;
use proptest::arbitrary::Arbitrary;
use proptest::collection::vec;
use proptest::strategy::BoxedStrategy;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, NotSet};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Display, EnumIter, Serialize, DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum WorkerState {
    #[default]
    Pending,
    Active,
    Unreachable,
    Removed,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Display,
    EnumIter,
    Serialize,
    Deserialize,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum DesiredWorkerState {
    #[default]
    Active,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "worker")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub host_addr: NetworkAddr,
    #[sea_orm(unique)]
    pub data_addr: NetworkAddr,
    pub capacity: Option<i32>,
    pub current_state: WorkerState,
    pub desired_state: DesiredWorkerState,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "crate::source::physical_source::Entity")]
    PhysicalSource,
    #[sea_orm(has_many = "crate::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "crate::query::fragment::Entity")]
    Fragment,
}

impl Related<physical_source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

impl Related<sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<query::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateWorker {
    pub host_addr: NetworkAddr,
    pub data_addr: NetworkAddr,
    pub capacity: Option<i32>,
    pub peers: Vec<NetworkAddr>,
    pub config: serde_json::Value,
}

impl From<CreateWorker> for ActiveModel {
    fn from(req: CreateWorker) -> Self {
        Self {
            host_addr: Set(req.host_addr),
            data_addr: Set(req.data_addr),
            capacity: Set(req.capacity),
            current_state: NotSet,
            desired_state: NotSet,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct GetWorker {
    pub host_addr: Option<NetworkAddr>,
    pub desired_state: Option<DesiredWorkerState>,
}

impl GetWorker {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_host_addr(mut self, host_addr: NetworkAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }

    pub fn with_desired_state(mut self, state: DesiredWorkerState) -> Self {
        self.desired_state = Some(state);
        self
    }
}

impl IntoCondition for GetWorker {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.desired_state.map(|v| Column::DesiredState.eq(v)))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DropWorker {
    pub host_addr: NetworkAddr,
}

impl DropWorker {
    pub fn new(host_addr: NetworkAddr) -> Self {
        Self { host_addr }
    }
}

impl Arbitrary for CreateWorker {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        (
            any::<NetworkAddr>(),
            1024..65535u16,
            prop_oneof![Just(None), (0..=1024i32).prop_map(Some)],
        )
            .prop_map(|(host_addr, data_port, capacity)| {
                let data_port = if data_port == host_addr.port {
                    if data_port < 65534 {
                        data_port + 1
                    } else {
                        data_port - 1
                    }
                } else {
                    data_port
                };
                let data_addr = NetworkAddr::new(host_addr.host.clone(), data_port);
                CreateWorker {
                    host_addr,
                    data_addr,
                    capacity,
                    peers: vec![],
                    config: Default::default(),
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

pub fn n_unique_workers(n: usize) -> BoxedStrategy<Vec<CreateWorker>> {
    use proptest::prelude::*;
    vec(
        prop_oneof![Just(None), (0..=1024i32).prop_map(Some)],
        n,
    )
    .prop_map(|capacities| {
        capacities
            .into_iter()
            .enumerate()
            .map(|(i, capacity)| {
                let octet = (i + 1) as u8;
                let ip = format!("192.168.1.{octet}");
                CreateWorker {
                    host_addr: NetworkAddr::new(&ip, endpoint::DEFAULT_HOST_PORT),
                    data_addr: NetworkAddr::new(&ip, endpoint::DEFAULT_DATA_PORT),
                    capacity,
                    peers: vec![],
                    config: Default::default(),
                }
            })
            .collect()
    })
    .boxed()
}

impl CreateWorker {
    pub fn topology_no_edges(min_nodes: u8, max_nodes: u8) -> BoxedStrategy<Vec<CreateWorker>> {
        use proptest::prelude::*;
        (min_nodes as usize..=max_nodes as usize)
            .prop_flat_map(n_unique_workers)
            .boxed()
    }

    pub fn topology_dag(min_nodes: u8, max_nodes: u8) -> BoxedStrategy<Vec<CreateWorker>> {
        use proptest::prelude::*;
        (min_nodes as usize..=max_nodes as usize)
            .prop_flat_map(|n| {
                let max_edges = n * (n - 1) / 2;
                let num_edges = 0..=max_edges.min(2 * n);
                (n_unique_workers(n), num_edges)
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
                workers
            })
            .boxed()
    }
}
